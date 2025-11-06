// Package app contains the shared, reusable logic for starting and stopping the service.
package app

import (
	"context"
	"errors"
	"log/slog" // IMPORTED
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// "github.com/rs/zerolog" // REMOVED
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/routingservice"
)

// Run executes the main application lifecycle for the routing service. It starts
// both the API and WebSocket services, listens for OS signals, and performs a
// graceful shutdown of both.
func Run(
	ctx context.Context,
	logger *slog.Logger, // CHANGED
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
		logger.Info("Starting API Service...") // CHANGED
		err := apiService.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("API Service failed", "err", err) // CHANGED
			cancel()                                       // Trigger shutdown of other services.
		}
	}()

	go func() {
		defer wg.Done()
		logger.Info("Starting Connection Manager Service...") // CHANGED
		err := connManager.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Connection Manager Service failed", "err", err) // CHANGED
			cancel()                                                      // Trigger shutdown of other services.
		}
	}()

	// Wait for a shutdown signal.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-shutdown:
		logger.Info("Received shutdown signal.", "signal", sig.String()) // CHANGED
	case <-ctx.Done():
		logger.Info("Context cancelled, initiating shutdown.") // CHANGED
	}

	// Execute graceful shutdown.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	logger.Info("Shutting down API Service...") // CHANGED
	err := apiService.Shutdown(shutdownCtx)
	if err != nil {
		logger.Error("API Service shutdown failed.", "err", err) // CHANGED
	}

	logger.Info("Shutting down Connection Manager...") // CHANGED
	err = connManager.Shutdown(shutdownCtx)
	if err != nil {
		logger.Error("Connection Manager shutdown failed.", "err", err) // CHANGED
	}

	wg.Wait()
	logger.Info("All services shut down gracefully.") // CHANGED
}
