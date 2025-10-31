//go:build unit

/*
File: internal/realtime/connectionmanager_test.go
Description: CORRECTED REFACTOR to fix the test failure.
- Uses 'SetWriteDeadline' for a deterministic failure.
- All other test setup logic is confirmed correct.
*/
package realtime

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// mockMessageConsumer satisfies the MessageConsumer interface,
// including the missing 'Done()' method.
type mockMessageConsumer struct {
	mock.Mock
}

func (m *mockMessageConsumer) Messages() <-chan messagepipeline.Message {
	return make(chan messagepipeline.Message)
}
func (m *mockMessageConsumer) Done() <-chan struct{} {
	return make(chan struct{})
}
func (m *mockMessageConsumer) Start(ctx context.Context) error { return nil }
func (m *mockMessageConsumer) Stop(ctx context.Context) error  { return nil }

// testFixture holds all the components for a test.
type testFixture struct {
	cm             *ConnectionManager
	presenceCache  cache.PresenceCache[urn.URN, routing.ConnectionInfo]
	wsServer       *httptest.Server
	wsClientConn   *websocket.Conn
	userURN        urn.URN
	messageHandler chan []byte
}

// REFACTORED: Helper to create the new "dumb" envelope
func newTestEnvelope(t *testing.T, recipient urn.URN) *secure.SecureEnvelope {
	t.Helper()
	return &secure.SecureEnvelope{
		RecipientID:   recipient,
		EncryptedData: []byte("test-data"),
	}
}

// setup creates a full test fixture for the ConnectionManager.
func setup(t *testing.T) *testFixture {
	t.Helper()
	logger := zerolog.Nop()

	// 1. Create Mocks & Real Cache
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]()
	consumer := new(mockMessageConsumer)

	// 2. Create ConnectionManager
	// (Assuming middleware.NoopAuth has been added)
	cm, err := NewConnectionManager(
		"0",
		middleware.NoopAuth(true, "test-user-id"),
		presenceCache,
		consumer,
		logger,
	)
	require.NoError(t, err, "NewConnectionManager failed")

	// 3. Create a test WebSocket server
	wsServer := httptest.NewServer(cm.server.Handler)
	t.Cleanup(wsServer.Close)

	// 4. Create a test WebSocket client
	wsURL := "ws" + strings.TrimPrefix(wsServer.URL, "http") + "/connect"
	wsClientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "Failed to dial test WebSocket server")
	t.Cleanup(func() { _ = wsClientConn.Close() })

	userURN, err := urn.Parse("urn:sm:user:test-user-id")
	require.NoError(t, err)

	// 5. Create a message handler to read from the client
	messageHandler := make(chan []byte, 1)
	go func() {
		for {
			_, msg, err := wsClientConn.ReadMessage()
			if err != nil {
				return // Connection closed
			}
			messageHandler <- msg
		}
	}()

	// Wait for the connection to be registered
	require.Eventually(t, func() bool {
		_, ok := cm.connections.Load(userURN.String())
		return ok
	}, 2*time.Second, 10*time.Millisecond, "User connection was not registered")

	return &testFixture{
		cm:             cm,
		presenceCache:  presenceCache,
		wsServer:       wsServer,
		wsClientConn:   wsClientConn,
		userURN:        userURN,
		messageHandler: messageHandler,
	}
}

func TestConnectionManager_deliveryProcessor(t *testing.T) {
	t.Run("Success - delivers message to connected user", func(t *testing.T) {
		fx := setup(t)

		// Arrange
		envelope := newTestEnvelope(t, fx.userURN)

		// Act
		err := fx.cm.deliveryProcessor(context.Background(), messagepipeline.Message{}, envelope)
		require.NoError(t, err)

		// Assert: Check that the client received the message
		var receivedMsgBytes []byte
		select {
		case receivedMsgBytes = <-fx.messageHandler:
			// success
		case <-time.After(1 * time.Second):
			t.Fatal("Timed out waiting for message from WebSocket client")
		}

		var receivedEnvelope secure.SecureEnvelope
		err = json.Unmarshal(receivedMsgBytes, &receivedEnvelope)
		require.NoError(t, err, "Failed to unmarshal JSON from WebSocket")

		assert.Equal(t, envelope, &receivedEnvelope)
	})

	t.Run("Success - skips message for unconnected user", func(t *testing.T) {
		fx := setup(t)

		// Arrange
		otherUser, _ := urn.Parse("urn:sm:user:other-user")
		envelope := newTestEnvelope(t, otherUser)

		// Act
		err := fx.cm.deliveryProcessor(context.Background(), messagepipeline.Message{}, envelope)

		// Assert
		assert.NoError(t, err, "Processing a message for an unconnected user should not be an error")
		assert.Len(t, fx.messageHandler, 0, "No message should have been sent")
	})

	t.Run("Failure - removes connection on write error", func(t *testing.T) {
		fx := setup(t)

		// --- THIS IS THE FIX ---
		// Arrange:
		// Get the *server-side* connection from the manager
		connVal, ok := fx.cm.connections.Load(fx.userURN.String())
		require.True(t, ok, "Precondition failed: server-side connection not found")
		serverConn, ok := connVal.(*websocket.Conn)
		require.True(t, ok, "Precondition failed: connection is not *websocket.Conn")

		// Set the write deadline to the past.
		// This will cause conn.WriteJSON() to fail with a timeout
		// without triggering the read loop's cleanup.
		err := serverConn.SetWriteDeadline(time.Now().Add(-1 * time.Second))
		require.NoError(t, err)

		envelope := newTestEnvelope(t, fx.userURN)

		// Act:
		// We immediately call the processor. It will find the connection,
		// attempt to write, fail, and then call cm.Remove().
		err = fx.cm.deliveryProcessor(context.Background(), messagepipeline.Message{}, envelope)
		// --- END FIX ---

		// Assert
		assert.Error(t, err, "Processing should have failed due to write error")

		// Assert that the connection and presence were removed *by deliveryProcessor*
		_, ok = fx.cm.connections.Load(fx.userURN.String())
		assert.False(t, ok, "Connection was not removed from map")

		_, err = fx.presenceCache.Fetch(context.Background(), fx.userURN)
		assert.Error(t, err, "Presence was not removed from cache")
	})
}
