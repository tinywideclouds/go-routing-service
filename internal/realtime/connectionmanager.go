/*
File: internal/realtime/connectionmanager.go
Description: REFACTORED to remove the 'DeliveryBus' pipeline.
It now depends on 'queue.MessageQueue' to trigger the hot-to-cold
migration on user disconnect.
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
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/internal/queue" // NEW: Import
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

// ConnectionManager manages all active WebSocket connections and user presence.
// It runs its own dedicated HTTP server.
type ConnectionManager struct {
	server        *http.Server
	upgrader      websocket.Upgrader
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo]
	messageQueue  queue.MessageQueue // NEW: For triggering migration
	// DELETED: deliveryPipeline *messagepipeline.StreamingService
	connections sync.Map // map[string]*websocket.Conn
	logger      zerolog.Logger
	instanceID  string
}

// NewConnectionManager creates and wires up a new WebSocket connection manager.
func NewConnectionManager(
	port string,
	authMiddleware func(http.Handler) http.Handler,
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo],
	messageQueue queue.MessageQueue, // NEW: Use MessageQueue
	logger zerolog.Logger,
) (*ConnectionManager, error) {

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
		messageQueue:  messageQueue, // NEW
		connections:   sync.Map{},
		logger:        cmLogger,
		instanceID:    instanceID,
	}

	// DELETED: All logic for creating the deliveryPipeline

	mux := http.NewServeMux()
	mux.Handle("/connect", authMiddleware(http.HandlerFunc(cm.connectHandler)))
	cm.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return cm, nil
}

// Start runs the HTTP server for WebSocket connections.
func (cm *ConnectionManager) Start(ctx context.Context) error {
	// 1. Start the HTTP server.
	cm.logger.Info().Str("addr", cm.server.Addr).Msg("WebSocket server starting...")
	if err := cm.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("websocket server failed: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the HTTP server.
func (cm *ConnectionManager) Shutdown(ctx context.Context) error {
	cm.logger.Info().Msg("Shutting down WebSocket service...")
	var finalErr error

	if err := cm.server.Shutdown(ctx); err != nil {
		cm.logger.Error().Err(err).Msg("WebSocket server shutdown failed.")
		finalErr = err
	}

	// DELETED: deliveryPipeline.Stop()

	// TODO: Iterate all connections and send a clean close message.

	cm.logger.Info().Msg("WebSocket service shut down.")
	return finalErr
}

// DELETED: deliveryProcessor method

// connectHandler upgrades a new HTTP request to a WebSocket and manages its lifecycle.
func (cm *ConnectionManager) connectHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
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
	defer cm.Remove(userURN) // This now triggers migration

	cm.logger.Info().Str("user", userURN.String()).Msg("User connected via WebSocket.")

	// Read loop (to detect client disconnect)
	for {
		// We only read to detect the disconnect. We don't process messages.
		if _, _, err := conn.ReadMessage(); err != nil {
			break // Client disconnected or error
		}
	}
}

// add registers a new connection and sets the user's presence.
func (cm *ConnectionManager) add(userURN urn.URN, conn *websocket.Conn) {
	cm.connections.Store(userURN.String(), conn)
	info := routing.ConnectionInfo{
		ServerInstanceID: cm.instanceID,
		ConnectedAt:      time.Now().Unix(),
	}

	if err := cm.presenceCache.Set(context.Background(), userURN, info); err != nil {
		cm.logger.Error().Err(err).Str("user", userURN.String()).Msg("Failed to set user presence in cache.")
	}
}

// Remove unregisters a connection, deletes presence, and triggers queue migration.
func (cm *ConnectionManager) Remove(userURN urn.URN) {
	cm.connections.Delete(userURN.String())

	// 1. Delete presence
	if err := cm.presenceCache.Delete(context.Background(), userURN); err != nil {
		cm.logger.Error().Err(err).Str("user", userURN.String()).Msg("Failed to delete user presence from cache.")
	}

	// --- THIS IS THE NEW LOGIC ---
	// 2. Trigger hot-to-cold migration for this user
	cm.logger.Info().Str("user", userURN.String()).Msg("User disconnected. Triggering hot-to-cold queue migration.")
	if err := cm.messageQueue.MigrateHotToCold(context.Background(), userURN); err != nil {
		// This is a critical error, as messages might be stuck in the hot queue.
		cm.logger.Error().Err(err).Str("user", userURN.String()).Msg("CRITICAL: Hot-to-cold migration failed.")
	}
	// --- END NEW LOGIC ---

	cm.logger.Info().Str("user", userURN.String()).Msg("User disconnected.")
}
