/*
File: routingservice/routingservice.go
Description: REFACTORED to use the new 'routing.ServiceDependencies'
struct and remove all references to the old delivery pipeline.
*/
package routingservice

import (
	"context"
	"errors" // <-- ADDED
	"fmt"
	"log/slog" // IMPORTED
	"net/http"
	"os"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog" // needed for noisy logger in pipeline
	"github.com/tinywideclouds/go-routing-service/internal/api"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"

	// REFACTORED: Use new base server and platform types
	"github.com/tinywideclouds/go-microservice-base/pkg/microservice"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// Wrapper now embeds BaseServer to get standard server functionality.
type Wrapper struct {
	*microservice.BaseServer
	processingService *messagepipeline.StreamingService[secure.SecureEnvelope]
	apiHandler        *api.API
	logger            *slog.Logger  // CHANGED
	httpReadyChan     chan struct{} // <-- ADDED: Channel to wait for HTTP server
}

// New creates and wires up the entire routing service using the base server.
func New(
	cfg *config.AppConfig,
	dependencies *routing.ServiceDependencies, // REFACTORED: Use new struct
	authMiddleware func(http.Handler) http.Handler,
	logger *slog.Logger, // CHANGED
) (*Wrapper, error) {

	// 1. Create the standard base server.
	baseServer := microservice.NewBaseServer(logger, ":"+cfg.APIPort) // CHANGED

	// --- ADDED: Set up the ready channel ---
	httpReadyChan := make(chan struct{})
	baseServer.SetReadyChannel(httpReadyChan)
	// --- END ADD ---

	// 2. Create the API handlers.
	// REFACTORED: Pass the new 'MessageQueue'
	apiHandler := api.NewAPI(
		dependencies.IngestionProducer,
		dependencies.MessageQueue,
		logger.With("component", "API"), // CHANGED
	)

	// 3. Create the main background processing pipeline.
	processingService, err := newProcessingService(cfg, dependencies, logger)
	if err != nil {
		logger.Error("Failed to create processing service", "err", err) // ADDED
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	// 4. Create the router and attach handlers.
	mux := baseServer.Mux()

	sendHandler := http.HandlerFunc(apiHandler.SendHandler)
	batchHandler := http.HandlerFunc(apiHandler.GetMessageBatchHandler)
	ackHandler := http.HandlerFunc(apiHandler.AcknowledgeMessagesHandler)

	authedSendHandler := authMiddleware(sendHandler)
	authedBatchHandler := authMiddleware(batchHandler)
	authedAckHandler := authMiddleware(ackHandler)

	mux.Handle("POST /api/send", authedSendHandler)
	mux.Handle("GET /api/messages", authedBatchHandler)
	mux.Handle("POST /api/messages/ack", authedAckHandler)

	logger.Debug("API routes registered") // ADDED

	return &Wrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		apiHandler:        apiHandler,
		logger:            logger,
		httpReadyChan:     httpReadyChan, // <-- ADDED: Store the channel
	}, nil
}

// newProcessingService builds the main message processing pipeline.
func newProcessingService(
	cfg *config.AppConfig,
	dependencies *routing.ServiceDependencies, // REFACTORED: Use new struct
	logger *slog.Logger, // CHANGED
) (*messagepipeline.StreamingService[secure.SecureEnvelope], error) {

	processorLogger := logger.With("component", "RoutingProcessor") // ADDED
	processor := pipeline.NewRoutingProcessor(dependencies, cfg, processorLogger)

	// streamLogger := logger.With("component", "StreamingService") // ADDED
	noisyZerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	return messagepipeline.NewStreamingService[secure.SecureEnvelope](
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		dependencies.IngestionConsumer,
		pipeline.EnvelopeTransformer,
		processor,
		noisyZerologLogger, // CHANGED (This assumes NewStreamingService accepts a slog.Logger)
	)
}

// Start runs the service's background components before starting the base HTTP server.
func (w *Wrapper) Start(ctx context.Context) error {
	w.logger.Info("Core processing pipeline starting...")
	if err := w.processingService.Start(ctx); err != nil {
		w.logger.Error("Failed to start processing service", "err", err) // ADDED
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	w.logger.Info("Core processing pipeline started.") // ADDED

	// --- MODIFIED: Start server and wait for it to be ready ---
	serverErrChan := make(chan error, 1)
	go func() {
		// BaseServer.Start() is already refactored to use slog
		if err := w.BaseServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// This error is logged inside BaseServer, no need to log here.
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	// Wait for EITHER the server to be ready OR for it to fail on startup
	select {
	case <-w.httpReadyChan:
		// This channel is closed by BaseServer.Start() *after* net.Listen() succeeds
		w.logger.Info("HTTP listener is active.") // CHANGED
		// NOW it is safe to mark the service as ready
		w.SetReady(true)                       // SetReady() is already refactored to use slog
		w.logger.Info("Service is now ready.") // CHANGED

	case err := <-serverErrChan:
		// Server failed before it could listen
		w.logger.Error("HTTP server failed to start", "err", err) // ADDED
		return fmt.Errorf("HTTP server failed to start: %w", err)

	case <-ctx.Done():
		w.logger.Info("Service start cancelled by context") // ADDED
		return ctx.Err()
	}

	// Wait for the server goroutine to exit (which happens on Shutdown)
	if err := <-serverErrChan; err != nil {
		w.logger.Error("HTTP server shut down with an error", "err", err) // ADDED
		return err
	}

	w.logger.Info("HTTP server shut down cleanly.") // ADDED
	return nil
	// --- END MODIFICATION ---
}

// Shutdown gracefully stops all service components in the correct order.
func (w *Wrapper) Shutdown(ctx context.Context) error {
	w.logger.Info("Shutting down service components...") // CHANGED
	var finalErr error

	if err := w.processingService.Stop(ctx); err != nil {
		w.logger.Error("Processing service shutdown failed.", "err", err) // CHANGED
		finalErr = err
	}

	w.logger.Debug("Waiting for API handler background tasks...") // ADDED
	w.apiHandler.Wait()                                           // Wait for any background API tasks (e.g., message deletion) to finish.
	w.logger.Debug("API handler tasks finished.")                 // ADDED

	// BaseServer.Shutdown() is already refactored to use slog
	if err := w.BaseServer.Shutdown(ctx); err != nil {
		// This error is logged inside BaseServer, no need to log here.
		finalErr = err
	}

	w.logger.Info("All components shut down.") // CHANGED
	return finalErr
}
