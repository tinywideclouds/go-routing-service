// Package app contains the shared, reusable logic for starting and stopping the service.
package app

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/illmade-knight/go-routing-service/internal/realtime"
	"github.com/illmade-knight/go-routing-service/routingservice"
	"github.com/rs/zerolog"
)

// Run executes the main application lifecycle for the routing service. It starts
// both the API and WebSocket services, listens for OS signals, and performs a
// graceful shutdown of both.
func Run(
	ctx context.Context,
	logger zerolog.Logger,
	apiService *routingservice.Wrapper,
	connManager *realtime.ConnectionManager,
) {
	var wg sync.WaitGroup
	wg.Add(2)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start both services in separate goroutines.
	go func() {
		defer wg.Done()
		logger.Info().Msg("Starting API Service...")
		err := apiService.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error().Err(err).Msg("API Service failed")
			cancel() // Trigger shutdown of other services.
		}
	}()

	go func() {
		defer wg.Done()
		logger.Info().Msg("Starting Connection Manager Service...")
		err := connManager.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error().Err(err).Msg("Connection Manager Service failed")
			cancel() // Trigger shutdown of other services.
		}
	}()

	// Wait for a shutdown signal.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-shutdown:
		logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal.")
	case <-ctx.Done():
		logger.Info().Msg("Context cancelled, initiating shutdown.")
	}

	// Execute graceful shutdown.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	logger.Info().Msg("Shutting down API Service...")
	err := apiService.Shutdown(shutdownCtx)
	if err != nil {
		logger.Error().Err(err).Msg("API Service shutdown failed.")
	}

	logger.Info().Msg("Shutting down Connection Manager...")
	err = connManager.Shutdown(shutdownCtx)
	if err != nil {
		logger.Error().Err(err).Msg("Connection Manager shutdown failed.")
	}

	wg.Wait()
	logger.Info().Msg("All services shut down gracefully.")
}
