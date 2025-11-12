/*
File: internal/realtime/connectionmanager.go
Description: Manages WebSocket connections, user presence,
and the lifecycle of real-time clients.
*/
// Package realtime provides components for managing real-time client connections.
package realtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

// ConnectionManager manages all active WebSocket connections and user presence.
type ConnectionManager struct {
	server        *http.Server
	upgrader      websocket.Upgrader
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo]
	messageQueue  queue.MessageQueue
	connections   sync.Map // map[string]*websocket.Conn
	logger        *slog.Logger
	instanceID    string
	listener      net.Listener // Manages the port for dynamic port allocation
	port          string       // The configured port
}

// NewConnectionManager creates and wires up a new WebSocket connection manager.
func NewConnectionManager(
	port string,
	authMiddleware func(http.Handler) http.Handler, // Accepts auth middleware
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo],
	messageQueue queue.MessageQueue,
	logger *slog.Logger,
) (*ConnectionManager, error) {

	instanceID := uuid.NewString()
	cmLogger := logger.With("component", "ConnectionManager", "instance", instanceID)

	cm := &ConnectionManager{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				cmLogger.Debug("WebSocket CheckOrigin", "origin", r.Header.Get("Origin"))
				return true
			},
		},
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		connections:   sync.Map{},
		logger:        cmLogger,
		instanceID:    instanceID,
		port:          port, // Store the configured port
	}

	mux := http.NewServeMux()
	// The /connect endpoint is protected by the injected auth middleware.
	mux.Handle("/connect", authMiddleware(http.HandlerFunc(cm.connectHandler)))
	cm.server = &http.Server{
		Handler: mux,
	}

	return cm, nil
}

// Start runs the HTTP server for WebSocket connections.
func (cm *ConnectionManager) Start(ctx context.Context) error {
	addr := cm.port
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	// 1. Create the listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		cm.logger.Error("WebSocket server failed to listen", "addr", addr, "err", err)
		return fmt.Errorf("websocket server failed to listen: %w", err)
	}
	// 2. Store the listener so GetHTTPPort can read the dynamically assigned port
	cm.listener = listener

	// 3. Log the *actual* port
	cm.logger.Info("WebSocket server starting...", "addr", cm.listener.Addr().String())

	// 4. Call Serve on the listener
	if err := cm.server.Serve(cm.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		cm.logger.Error("WebSocket server failed", "err", err)
		return fmt.Errorf("websocket server failed: %w", err)
	}
	cm.logger.Info("WebSocket server stopped.")
	return nil
}

// Shutdown gracefully stops the HTTP server.
func (cm *ConnectionManager) Shutdown(ctx context.Context) error {
	cm.logger.Info("Shutting down WebSocket service...")
	var finalErr error

	// 1. Shut down the HTTP server
	if err := cm.server.Shutdown(ctx); err != nil {
		cm.logger.Error("WebSocket server shutdown failed", "err", err)
		finalErr = err
	}

	// 2. Close the underlying listener
	if cm.listener != nil {
		if err := cm.listener.Close(); err != nil {
			cm.logger.Error("WebSocket listener close failed", "err", err)
			finalErr = err
		}
	}

	cm.logger.Info("WebSocket service shut down.")
	return finalErr
}

// GetHTTPPort returns the port the server is listening on (e.g., ":8080").
// Returns ":0" if the listener is not yet active.
func (cm *ConnectionManager) GetHTTPPort() string {
	if cm.listener == nil {
		return ":0" // Not started yet
	}
	addr := cm.listener.Addr().String()
	port := strings.LastIndex(addr, ":")
	if port == -1 {
		return ":0" // Should not happen
	}
	// Return in the same ":port" format
	return addr[port:]
}

// connectHandler upgrades a new HTTP request to a WebSocket and manages its lifecycle.
// This handler assumes authentication has already been verified by middleware.
func (cm *ConnectionManager) connectHandler(w http.ResponseWriter, r *http.Request) {
	// The auth middleware has already run and populated the context.
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		// This is a failsafe, but should be caught by middleware.
		cm.logger.Error("connectHandler reached without user ID in context. Auth middleware may be misconfigured.")
		http.Error(w, "Unauthorized: Auth middleware failed to set user context.", http.StatusUnauthorized)
		return
	}
	userURN, err := urn.Parse(authedUserID)
	if err != nil {
		cm.logger.Error("Failed to parse user URN from authenticated context", "err", err, "user_id", authedUserID)
		http.Error(w, "Internal Server Error: Could not parse user URN", http.StatusInternalServerError)
		return
	}

	log := cm.logger.With("user", userURN.String())

	// --- WebSocket Auth Handshake ---
	// The JWT is passed via the 'Sec-WebSocket-Protocol' header.
	// We must read this header and pass it *back* to the upgrader
	// to complete the handshake. The auth middleware has already validated it.
	protocol := r.Header.Get("Sec-WebSocket-Protocol")
	headers := http.Header{}
	if protocol != "" {
		log.Debug("Passing Sec-WebSocket-Protocol to upgrader")
		headers.Set("Sec-WebSocket-Protocol", protocol)
	} else {
		// This should have been caught by auth, but we log it just in case.
		log.Warn("connectHandler reached without Sec-WebSocket-Protocol header. Auth middleware may be misconfigured.")
	}

	conn, err := cm.upgrader.Upgrade(w, r, headers) // Pass headers to complete handshake
	if err != nil {
		log.Error("Failed to upgrade connection", "err", err)
		return
	}
	// --- End Handshake ---

	defer func() {
		err = conn.Close()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Warn("Error closing connection", "err", err)
			}
		}
	}()

	cm.add(userURN, conn)
	defer cm.Remove(userURN) // This now triggers migration

	log.Info("User connected via WebSocket.")

	// Read loop (to detect client disconnect)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Debug("WebSocket closed normally")
			} else {
				log.Warn("WebSocket read error", "err", err)
			}
			break
		}
	}
}

// add registers a new connection and sets the user's presence.
func (cm *ConnectionManager) add(userURN urn.URN, conn *websocket.Conn) {
	log := cm.logger.With("user", userURN.String())
	cm.connections.Store(userURN.String(), conn)

	info := routing.ConnectionInfo{
		ServerInstanceID: cm.instanceID,
		ConnectedAt:      time.Now().Unix(),
	}

	log.Debug("Setting user presence in cache")
	if err := cm.presenceCache.Set(context.Background(), userURN, info); err != nil {
		log.Error("Failed to set user presence in cache", "err", err)
	}
}

// Remove unregisters a connection, deletes presence, and triggers queue migration.
func (cm *ConnectionManager) Remove(userURN urn.URN) {
	log := cm.logger.With("user", userURN.String())
	cm.connections.Delete(userURN.String())

	// 1. Delete presence
	log.Debug("Deleting user presence from cache")
	if err := cm.presenceCache.Delete(context.Background(), userURN); err != nil {
		log.Error("Failed to delete user presence from cache", "err", err)
	}

	// 2. Trigger hot-to-cold migration for this user
	log.Info("User disconnected. Triggering hot-to-cold queue migration.")
	migrationCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cm.messageQueue.MigrateHotToCold(migrationCtx, userURN); err != nil {
		log.Error("CRITICAL: Hot-to-cold migration failed", "err", err)
	}

	log.Info("User disconnected.")
}
