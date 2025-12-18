/*
File: internal/realtime/connectionmanager.go
Description: Manages WebSocket connections with multi-device support and a Pub/Sub listener for real-time "Pokes".
*/
package realtime

import (
	"context"
	"encoding/json"
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
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
)

// ConnectionManager manages all active WebSocket connections and user presence.
type ConnectionManager struct {
	server        *http.Server
	upgrader      websocket.Upgrader
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo]
	messageQueue  queue.MessageQueue

	// Consumer for receiving "Poke" events from the Routing Processor
	pokeConsumer messagepipeline.MessageConsumer

	// Efficient Session Index: Map[UserURN] -> Map[SessionID] -> Connection
	sessionMu    sync.RWMutex
	userSessions map[string]map[string]*websocket.Conn

	logger     *slog.Logger
	instanceID string
	listener   net.Listener
	port       string
	wg         sync.WaitGroup
}

// pokePayload matches the JSON structure sent by the RoutingProcessor to Pub/Sub.
type pokePayload struct {
	Type      string `json:"type"`
	Recipient string `json:"recipient"`
}

// NewConnectionManager creates and wires up a new WebSocket connection manager.
func NewConnectionManager(
	port string,
	authMiddleware func(http.Handler) http.Handler,
	presenceCache cache.PresenceCache[urn.URN, routing.ConnectionInfo],
	messageQueue queue.MessageQueue,
	pokeConsumer messagepipeline.MessageConsumer,
	logger *slog.Logger,
) (*ConnectionManager, error) {

	instanceID := uuid.NewString()
	cmLogger := logger.With("component", "ConnectionManager", "instance", instanceID)

	cm := &ConnectionManager{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// In production, CheckOrigin should be stricter or configurable.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		pokeConsumer:  pokeConsumer,
		userSessions:  make(map[string]map[string]*websocket.Conn),
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

// Start runs the HTTP server for WebSockets and starts the Poke Consumer loop.
func (cm *ConnectionManager) Start(ctx context.Context) error {
	addr := cm.port
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	// 1. Start the Poke Consumer (The Bridge)
	if cm.pokeConsumer != nil {
		cm.logger.Info("Starting Poke Consumer...")
		if err := cm.pokeConsumer.Start(ctx); err != nil {
			return fmt.Errorf("failed to start poke consumer: %w", err)
		}

		cm.wg.Add(1)
		go cm.processPokes(ctx)
	} else {
		cm.logger.Warn("⚠️ No PokeConsumer configured. Real-time updates will NOT work.")
	}

	// 2. Start WebSocket Server
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

// processPokes consumes messages from the pipeline channel.
func (cm *ConnectionManager) processPokes(ctx context.Context) {
	defer cm.wg.Done()
	cm.logger.Info("Poke Processing Loop Started")

	for {
		select {
		case <-ctx.Done():
			cm.logger.Info("Context cancelled, stopping poke processor")
			return
		case msg, ok := <-cm.pokeConsumer.Messages():
			if !ok {
				cm.logger.Info("Poke consumer channel closed")
				return
			}
			cm.handlePoke(&msg)
			msg.Ack() // Always Ack pokes; they are fire-and-forget signals.
		}
	}
}

// handlePoke processes a single message payload from Pub/Sub.
func (cm *ConnectionManager) handlePoke(msg *messagepipeline.Message) {
	var message messagepipeline.MessageData
	if err := json.Unmarshal(msg.Payload, &message); err != nil {
		cm.logger.Warn("Failed to unmarshalmessage", "err", err)
		return
	}
	var poke pokePayload
	if err := json.Unmarshal(message.Payload, &poke); err != nil {
		cm.logger.Warn("Failed to unmarshal poke payload", "err", err)
		return
	}

	if poke.Type != "poke" {
		return
	}

	targetURN := poke.Recipient
	cm.logger.Debug("🔔 Received Poke Signal", "recipient", targetURN)

	// 1. Look up active sessions for this user in our local index
	cm.sessionMu.RLock()
	sessions, found := cm.userSessions[targetURN]
	if !found || len(sessions) == 0 {
		cm.sessionMu.RUnlock()
		// This is normal; the user might be connected to a different server instance.
		return
	}

	// Snapshot the connections to minimize lock time
	activeConns := make([]*websocket.Conn, 0, len(sessions))
	for _, conn := range sessions {
		activeConns = append(activeConns, conn)
	}
	cm.sessionMu.RUnlock()

	// 2. Broadcast the Empty Signal to all user devices connected to THIS instance
	signal := []byte("{}")
	for _, conn := range activeConns {
		// Note: We ignore write errors here; strict connection management happens in the read loop.
		if err := conn.WriteMessage(websocket.TextMessage, signal); err != nil {
			cm.logger.Warn("Failed to send poke to socket", "err", err)
		}
	}
}

// Shutdown gracefully stops the HTTP server and the Consumer.
func (cm *ConnectionManager) Shutdown(ctx context.Context) error {
	cm.logger.Info("Shutting down WebSocket service...")
	var finalErr error

	// Stop Consumer first to drain messages
	if cm.pokeConsumer != nil {
		if err := cm.pokeConsumer.Stop(ctx); err != nil {
			cm.logger.Error("Poke Consumer stop failed", "err", err)
		}
		cm.wg.Wait()
	}

	if err := cm.server.Shutdown(ctx); err != nil {
		cm.logger.Error("WebSocket server shutdown failed", "err", err)
		finalErr = err
	}
	if cm.listener != nil {
		if err := cm.listener.Close(); err != nil {
			cm.logger.Debug("WebSocket listener close", "err", err)
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

// connectHandler handles the WebSocket upgrade and lifecycle.
func (cm *ConnectionManager) connectHandler(w http.ResponseWriter, r *http.Request) {
	var userURN urn.URN
	var parseErr error

	if handle, ok := middleware.GetUserHandleFromContext(r.Context()); ok && handle != "" {
		userURN, parseErr = urn.Parse(handle)
	} else if userID, ok := middleware.GetUserIDFromContext(r.Context()); ok && userID != "" {
		userURN, parseErr = urn.Parse(userID)
	} else {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if parseErr != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Generate a unique Session ID for this specific connection
	sessionID := uuid.NewString()

	protocol := r.Header.Get("Sec-WebSocket-Protocol")
	headers := http.Header{}
	if protocol != "" {
		headers.Set("Sec-WebSocket-Protocol", protocol)
	}

	conn, err := cm.upgrader.Upgrade(w, r, headers)
	if err != nil {
		cm.logger.Error("Failed to upgrade connection", "err", err)
		return
	}

	defer conn.Close()

	// Register Session
	cm.add(userURN, sessionID, conn)

	// Defer Removal (Trigger migration only if last session)
	defer cm.Remove(userURN, sessionID)

	cm.logger.Info("User connected via WebSocket.", "user", userURN.String(), "session", sessionID)

	// Read Pump (Keeps connection alive)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

// add registers a connection in the 2-level map and the distributed cache.
func (cm *ConnectionManager) add(userURN urn.URN, sessionID string, conn *websocket.Conn) {
	userKey := userURN.String()

	// Add to Local Index
	cm.sessionMu.Lock()
	if cm.userSessions[userKey] == nil {
		cm.userSessions[userKey] = make(map[string]*websocket.Conn)
	}
	cm.userSessions[userKey][sessionID] = conn
	cm.sessionMu.Unlock()

	info := routing.ConnectionInfo{
		ServerInstanceID: cm.instanceID,
		ConnectedAt:      time.Now().Unix(),
	}

	// Add to Distributed Cache
	if err := cm.presenceCache.AddSession(context.Background(), userURN, sessionID, info); err != nil {
		cm.logger.Error("Failed to add session to presence cache", "err", err)
	}
}

// Remove handles cleanup and conditional migration.
func (cm *ConnectionManager) Remove(userURN urn.URN, sessionID string) {
	userKey := userURN.String()

	// 1. Remove from Local Index
	cm.sessionMu.Lock()
	if sessions, exists := cm.userSessions[userKey]; exists {
		delete(sessions, sessionID)
		if len(sessions) == 0 {
			delete(cm.userSessions, userKey)
		}
	}
	cm.sessionMu.Unlock()

	// 2. Remove from Distributed Cache (Atomic Check)
	remaining, err := cm.presenceCache.RemoveSession(context.Background(), userURN, sessionID)
	if err != nil {
		cm.logger.Error("Failed to remove session from presence cache", "err", err)
		return
	}

	// If other sessions exist, DO NOT migrate.
	if remaining > 0 {
		return
	}

	// 3. Last Session Out -> Migrate Hot to Cold
	cm.logger.Info("Last device disconnected. Triggering hot-to-cold queue migration.", "user", userKey)
	migrationCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cm.messageQueue.MigrateHotToCold(migrationCtx, userURN); err != nil {
		cm.logger.Error("CRITICAL: Hot-to-cold migration failed", "err", err)
	}
}
