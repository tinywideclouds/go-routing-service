/*
File: internal/app/app.go
Description: Provides the main application lifecycle logic,
handling startup, graceful shutdown, and OS signal trapping.
*/
// Package app contains the shared, reusable logic for starting and stopping the service.
package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/routingservice"
)

// Run executes the main application lifecycle for the routing service. It starts
// both the API and WebSocket services, listens for OS signals, and performs a
// graceful shutdown of both.
func Run(
	ctx context.Context,
	logger *slog.Logger,
	apiService *routingservice.Wrapper,
	connManager *realtime.ConnectionManager,
) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start API Service if enabled
	if apiService != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("Starting API Service...")
			err := apiService.Start(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("API Service failed", "err", err)
				cancel() // Trigger shutdown of other services.
			}
		}()
	} else {
		logger.Info("API Service is disabled.")
	}

	// Start Connection Manager if enabled
	if connManager != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("Starting Connection Manager Service...")
			err := connManager.Start(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("Connection Manager Service failed", "err", err)
				cancel() // Trigger shutdown of other services.
			}
		}()
	} else {
		logger.Info("Connection Manager Service is disabled.")
	}

	// Wait for a shutdown signal.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-shutdown:
		logger.Info("Received shutdown signal.", "signal", sig.String())
	case <-ctx.Done():
		logger.Info("Context cancelled, initiating shutdown.")
	}

	// Execute graceful shutdown.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if apiService != nil {
		logger.Info("Shutting down API Service...")
		err := apiService.Shutdown(shutdownCtx)
		if err != nil {
			logger.Error("API Service shutdown failed.", "err", err)
		}
	}

	if connManager != nil {
		logger.Info("Shutting down Connection Manager...")
		err := connManager.Shutdown(shutdownCtx)
		if err != nil {
			logger.Error("Connection Manager shutdown failed.", "err", err)
		}
	}

	wg.Wait()
	logger.Info("All services shut down gracefully.")
}
