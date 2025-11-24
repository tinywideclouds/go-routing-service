/*
File: internal/realtime/connectionmanager.go
Description: Manages WebSocket connections with "Handle is King" resolution.
*/
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
	listener      net.Listener
	port          string
}

// NewConnectionManager creates and wires up a new WebSocket connection manager.
func NewConnectionManager(
	port string,
	authMiddleware func(http.Handler) http.Handler,
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
		port:          port,
	}

	mux := http.NewServeMux()
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

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		cm.logger.Error("WebSocket server failed to listen", "addr", addr, "err", err)
		return fmt.Errorf("websocket server failed to listen: %w", err)
	}
	cm.listener = listener
	cm.logger.Info("WebSocket server starting...", "addr", cm.listener.Addr().String())

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

	if err := cm.server.Shutdown(ctx); err != nil {
		cm.logger.Error("WebSocket server shutdown failed", "err", err)
		finalErr = err
	}

	if cm.listener != nil {
		if err := cm.listener.Close(); err != nil {
			cm.logger.Error("WebSocket listener close failed", "err", err)
			finalErr = err
		}
	}

	cm.logger.Info("WebSocket service shut down.")
	return finalErr
}

// GetHTTPPort returns the port the server is listening on.
func (cm *ConnectionManager) GetHTTPPort() string {
	if cm.listener == nil {
		return ":0"
	}
	addr := cm.listener.Addr().String()
	port := strings.LastIndex(addr, ":")
	if port == -1 {
		return ":0"
	}
	return addr[port:]
}

// connectHandler upgrades a new HTTP request to a WebSocket.
func (cm *ConnectionManager) connectHandler(w http.ResponseWriter, r *http.Request) {
	// REFACTOR: Use "Handle is King" resolution for WebSockets too.
	var userURN urn.URN
	var parseErr error

	if handle, ok := middleware.GetUserHandleFromContext(r.Context()); ok && handle != "" {
		userURN, parseErr = urn.Parse(handle)
	} else if userID, ok := middleware.GetUserIDFromContext(r.Context()); ok && userID != "" {
		userURN, parseErr = urn.Parse(userID)
	} else {
		cm.logger.Error("connectHandler: No identity found in context")
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if parseErr != nil {
		cm.logger.Error("Failed to parse user URN", "err", parseErr)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	log := cm.logger.With("user", userURN.String())

	// WebSocket Auth Handshake
	protocol := r.Header.Get("Sec-WebSocket-Protocol")
	headers := http.Header{}
	if protocol != "" {
		headers.Set("Sec-WebSocket-Protocol", protocol)
	}

	conn, err := cm.upgrader.Upgrade(w, r, headers)
	if err != nil {
		log.Error("Failed to upgrade connection", "err", err)
		return
	}

	defer func() {
		conn.Close()
	}()

	// Register using the Resolved URN (Handle)
	cm.add(userURN, conn)
	defer cm.Remove(userURN)

	log.Info("User connected via WebSocket.")

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

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

func (cm *ConnectionManager) Remove(userURN urn.URN) {
	log := cm.logger.With("user", userURN.String())
	cm.connections.Delete(userURN.String())

	log.Debug("Deleting user presence from cache")
	if err := cm.presenceCache.Delete(context.Background(), userURN); err != nil {
		log.Error("Failed to delete user presence from cache", "err", err)
	}

	log.Info("User disconnected. Triggering hot-to-cold queue migration.")
	migrationCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cm.messageQueue.MigrateHotToCold(migrationCtx, userURN); err != nil {
		log.Error("CRITICAL: Hot-to-cold migration failed", "err", err)
	}
	log.Info("User disconnected.")
}
