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
	"net/http"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
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
	logger            zerolog.Logger
	httpReadyChan     chan struct{} // <-- ADDED: Channel to wait for HTTP server
}

// New creates and wires up the entire routing service using the base server.
func New(
	cfg *config.AppConfig,
	dependencies *routing.ServiceDependencies, // REFACTORED: Use new struct
	authMiddleware func(http.Handler) http.Handler,
	logger zerolog.Logger,
) (*Wrapper, error) {

	// 1. Create the standard base server.
	baseServer := microservice.NewBaseServer(logger, ":"+cfg.APIPort)

	// --- ADDED: Set up the ready channel ---
	httpReadyChan := make(chan struct{})
	baseServer.SetReadyChannel(httpReadyChan)
	// --- END ADD ---

	// 2. Create the API handlers.
	// REFACTORED: Pass the new 'MessageQueue'
	apiHandler := api.NewAPI(
		dependencies.IngestionProducer,
		dependencies.MessageQueue,
		logger.With().Str("component", "API").Logger(),
	)

	// 3. Create the main background processing pipeline.
	processingService, err := newProcessingService(cfg, dependencies, logger)
	if err != nil {
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
	logger zerolog.Logger,
) (*messagepipeline.StreamingService[secure.SecureEnvelope], error) {

	processor := pipeline.NewRoutingProcessor(dependencies, cfg, logger)

	return messagepipeline.NewStreamingService[secure.SecureEnvelope](
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		dependencies.IngestionConsumer,
		pipeline.EnvelopeTransformer,
		processor,
		logger,
	)
}

// Start runs the service's background components before starting the base HTTP server.
func (w *Wrapper) Start(ctx context.Context) error {
	w.logger.Info().Msg("Core processing pipeline starting...")
	if err := w.processingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}

	// --- MODIFIED: Start server and wait for it to be ready ---
	serverErrChan := make(chan error, 1)
	go func() {
		if err := w.BaseServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			w.logger.Error().Err(err).Msg("HTTP server failed")
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	// Wait for EITHER the server to be ready OR for it to fail on startup
	select {
	case <-w.httpReadyChan:
		// This channel is closed by BaseServer.Start() *after* net.Listen() succeeds
		w.logger.Info().Msg("HTTP listener is active.")
		// NOW it is safe to mark the service as ready
		w.SetReady(true)
		w.logger.Info().Msg("Service is now ready.")

	case err := <-serverErrChan:
		// Server failed before it could listen
		return fmt.Errorf("HTTP server failed to start: %w", err)

	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for the server goroutine to exit (which happens on Shutdown)
	if err := <-serverErrChan; err != nil {
		return err
	}

	return nil
	// --- END MODIFICATION ---
}

// Shutdown gracefully stops all service components in the correct order.
func (w *Wrapper) Shutdown(ctx context.Context) error {
	w.logger.Info().Msg("Shutting down service components...")
	var finalErr error

	if err := w.processingService.Stop(ctx); err != nil {
		w.logger.Error().Err(err).Msg("Processing service shutdown failed.")
		finalErr = err
	}

	w.apiHandler.Wait() // Wait for any background API tasks (e.g., message deletion) to finish.

	if err := w.BaseServer.Shutdown(ctx); err != nil {
		w.logger.Error().Err(err).Msg("HTTP server shutdown failed.")
		finalErr = err
	}

	w.logger.Info().Msg("All components shut down.")
	return finalErr
}
