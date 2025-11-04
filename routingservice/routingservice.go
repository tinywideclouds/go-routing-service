/*
File: routingservice/routingservice.go
Description: REFACTORED to add a discoverable /service-info
endpoint and apply CORS middleware to all handlers.
*/
package routingservice

import (
	"context"
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
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware" // *** NEW ***
	"github.com/tinywideclouds/go-microservice-base/pkg/response"   // *** NEW ***
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// Wrapper now embeds BaseServer to get standard server functionality.
type Wrapper struct {
	*microservice.BaseServer
	processingService *messagepipeline.StreamingService[secure.SecureEnvelope]
	apiHandler        *api.API
	logger            zerolog.Logger
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

	// 2. Create the API handlers.
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

	// 4. *** NEW: Create CORS Middleware ***
	// Use the config to create the middleware
	corsCfg := middleware.CorsConfig{
		AllowedOrigins: cfg.Cors.AllowedOrigins,
		Role:           middleware.CorsRole(cfg.Cors.Role), // Cast string to CorsRole
	}
	corsMiddleware := middleware.NewCorsMiddleware(corsCfg)

	// 5. Get the router and attach handlers.
	mux := baseServer.Mux()

	sendHandler := http.HandlerFunc(apiHandler.SendHandler)
	batchHandler := http.HandlerFunc(apiHandler.GetMessageBatchHandler)
	ackHandler := http.HandlerFunc(apiHandler.AcknowledgeMessagesHandler)

	authedSendHandler := authMiddleware(sendHandler)
	authedBatchHandler := authMiddleware(batchHandler)
	authedAckHandler := authMiddleware(ackHandler)

	// *** NEW: Apply CORS to all handlers ***
	mux.Handle("POST /api/send", corsMiddleware(authedSendHandler))
	mux.Handle("GET /api/messages", corsMiddleware(authedBatchHandler))
	mux.Handle("POST /api/messages/ack", corsMiddleware(authedAckHandler))

	// *** NEW: Add OPTIONS handlers for API routes ***
	apiOptionsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("OPTIONS /api/send", corsMiddleware(apiOptionsHandler))
	mux.Handle("OPTIONS /api/messages", corsMiddleware(apiOptionsHandler))
	mux.Handle("OPTIONS /api/messages/ack", corsMiddleware(apiOptionsHandler))

	// 6. *** NEW: Define and register the discovery handler ***
	type serviceInfoPayload struct {
		WebSocketPort string `json:"webSocketPort"`
		APIPort       string `json:"apiPort"`
	}
	info := serviceInfoPayload{
		WebSocketPort: cfg.WebSocketPort,
		APIPort:       cfg.APIPort,
	}

	serviceInfoHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response.WriteJSON(w, http.StatusOK, info)
	})

	// Register the new handler on a "well-known" path
	mux.Handle("/.well-known/service-info", corsMiddleware(serviceInfoHandler))
	mux.Handle("OPTIONS /.well-known/service-info", corsMiddleware(apiOptionsHandler))

	return &Wrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		apiHandler:        apiHandler,
		logger:            logger,
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

	w.SetReady(true)
	w.logger.Info().Msg("Service is now ready.")

	return w.BaseServer.Start()
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
