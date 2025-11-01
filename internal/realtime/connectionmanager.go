/*
File: internal/realtime/connectionmanager.go
Description: REFACTORED to use the new 'secure.SecureEnvelope'
and 'urn' types, and CORRECTED to use the 'presenceCache.Set'
and 'presenceCache.Delete' methods from the interface.
*/
// Package realtime provides components for managing real-time client connections.
package realtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// ConnectionManager manages all active WebSocket connections and user presence.
// It runs its own dedicated HTTP server and a message pipeline for consuming real-time
// delivery messages from the message bus.
type ConnectionManager struct {
	server           *http.Server
	upgrader         websocket.Upgrader
	presenceCache    cache.PresenceCache[urn.URN, routing.ConnectionInfo]
	deliveryPipeline *messagepipeline.StreamingService[secure.SecureEnvelope] // REFACTORED: Type
	connections      sync.Map                                                 // map[string]*websocket.Conn
	logger           zerolog.Logger
	instanceID       string
}

// NewConnectionManager creates and wires up a new WebSocket connection manager.
func NewConnectionManager(
	port string,
	authMiddleware func(http.Handler) http.Handler,
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo],
	deliveryConsumer messagepipeline.MessageConsumer,
	logger zerolog.Logger,
) (*ConnectionManager, error) { // CORRECTED: Added error return

	instanceID := uuid.NewString()
	cmLogger := logger.With().Str("component", "ConnectionManager").Str("instance", instanceID).Logger()

	cm := &ConnectionManager{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				// TODO: Implement a real origin check
				return true
			},
		},
		presenceCache: presenceCache,
		connections:   sync.Map{},
		logger:        cmLogger,
		instanceID:    instanceID,
	}

	// REFACTORED: Update generic type
	// CORRECTED: Check for error from NewStreamingService
	deliveryPipeline, err := messagepipeline.NewStreamingService[secure.SecureEnvelope](
		messagepipeline.StreamingServiceConfig{NumWorkers: 10}, // TODO: Make configurable
		deliveryConsumer,
		pipeline.EnvelopeTransformer, // We can reuse the same transformer
		cm.deliveryProcessor,         // Use the method as the processor
		cmLogger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delivery pipeline: %w", err)
	}
	cm.deliveryPipeline = deliveryPipeline

	mux := http.NewServeMux()
	mux.Handle("/connect", authMiddleware(http.HandlerFunc(cm.connectHandler)))
	cm.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return cm, nil
}

// Start runs the two main components of the ConnectionManager.
func (cm *ConnectionManager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	errChan := make(chan error, 2)

	// 1. Start the HTTP server for new WebSocket connections.
	go func() {
		cm.logger.Info().Str("addr", cm.server.Addr).Msg("WebSocket server starting...")
		if err := cm.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- fmt.Errorf("websocket server failed: %w", err)
		}
	}()

	// 2. Start the background pipeline for consuming delivery messages.
	go func() {
		defer wg.Done()
		cm.logger.Info().Msg("WebSocket delivery pipeline starting...")
		if err := cm.deliveryPipeline.Start(ctx); err != nil {
			errChan <- fmt.Errorf("delivery pipeline failed: %w", err)
		}
	}()

	// Wait for the pipeline to exit (it will on context cancellation)
	wg.Wait()
	close(errChan)

	// Return the first error that occurred, if any.
	return <-errChan
}

// Shutdown gracefully stops the HTTP server and the delivery pipeline.
func (cm *ConnectionManager) Shutdown(ctx context.Context) error {
	cm.logger.Info().Msg("Shutting down WebSocket service...")
	var finalErr error

	if err := cm.server.Shutdown(ctx); err != nil {
		cm.logger.Error().Err(err).Msg("WebSocket server shutdown failed.")
		finalErr = err
	}

	if err := cm.deliveryPipeline.Stop(ctx); err != nil {
		cm.logger.Error().Err(err).Msg("WebSocket delivery pipeline shutdown failed.")
		finalErr = err
	}

	// TODO: Iterate all connections and send a clean close message.

	cm.logger.Info().Msg("WebSocket service shut down.")
	return finalErr
}

// deliveryProcessor is the final stage of the real-time pipeline.
// It finds the local connection for a user and writes the message.
// REFACTORED: Signature uses new *secure.SecureEnvelope
func (cm *ConnectionManager) deliveryProcessor(_ context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
	recipientKey := envelope.RecipientID.String()

	// REFACTORED: Log no longer has MessageID
	procLogger := cm.logger.With().Str("recipient_id", recipientKey).Logger()

	// Find the connection (if it's on this server instance).
	connVal, ok := cm.connections.Load(recipientKey)
	if !ok {
		// This is not an error. The user is just connected to a different server instance.
		return nil
	}
	conn, ok := connVal.(*websocket.Conn)
	if !ok {
		return fmt.Errorf("internal error: connection map stored non-conn value")
	}

	procLogger.Info().Msg("Delivering message to locally connected user.")

	// REFACTORED: Use conn.WriteJSON.
	// Our native 'secure.SecureEnvelope' facade has a 'MarshalJSON' method,
	// which 'WriteJSON' will automatically use. This is much cleaner
	// than manually marshaling the proto.
	if err := conn.WriteJSON(envelope); err != nil {
		procLogger.Error().Err(err).Msg("Failed to write message to WebSocket. Removing connection.")
		// The connection is broken. Remove it.
		cm.Remove(envelope.RecipientID)
		// Return the error so the pipeline NACKs the message,
		// allowing it to be stored for offline retrieval.
		return err
	}

	return nil
}

// connectHandler upgrades a new HTTP request to a WebSocket and manages its lifecycle.
func (cm *ConnectionManager) connectHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	// REFACTORED: Use new 'urn' package
	userURN, err := urn.New(urn.SecureMessaging, urn.EntityTypeUser, authedUserID)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	conn, err := cm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		cm.logger.Error().Err(err).Msg("Failed to upgrade connection.")
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			cm.logger.Warn().Err(err).Msg("error closing connection")
		}
	}()

	cm.add(userURN, conn)
	defer cm.Remove(userURN)

	cm.logger.Info().Str("user", userURN.String()).Msg("User connected via WebSocket.")

	// Read loop (to detect client disconnect)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break // Client disconnected or error
		}
	}
}

// add registers a new connection and sets the user's presence.
// REFACTORED: Uses new 'urn.URN' type
func (cm *ConnectionManager) add(userURN urn.URN, conn *websocket.Conn) {
	cm.connections.Store(userURN.String(), conn)
	info := routing.ConnectionInfo{
		ServerInstanceID: cm.instanceID,
		ConnectedAt:      time.Now().Unix(),
	}

	// --- THIS IS THE FIX ---
	// Changed 'Put' to 'Set' and removed the TTL.
	// This now matches the 'presencecache.go' interface.
	if err := cm.presenceCache.Set(context.Background(), userURN, info); err != nil {
		// --- END FIX ---
		cm.logger.Error().Err(err).Str("user", userURN.String()).Msg("Failed to set user presence in cache.")
	}
}

// Remove unregisters a connection and deletes the user's presence.
// REFACTORED: Uses new 'urn.URN' type
func (cm *ConnectionManager) Remove(userURN urn.URN) {
	cm.connections.Delete(userURN.String())
	// This call to 'Delete' matches the 'presencecache.go' interface.
	if err := cm.presenceCache.Delete(context.Background(), userURN); err != nil {
		cm.logger.Error().Err(err).Str("user", userURN.String()).Msg("Failed to delete user presence from cache.")
	}
	cm.logger.Info().Str("user", userURN.String()).Msg("User disconnected.")
}
