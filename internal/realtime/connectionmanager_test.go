//go:build unit

/*
File: internal/realtime/connectionmanager_test.go
Description: REFACTORED to remove all tests for the delivery pipeline.
It now tests that 'Remove' correctly calls the new
'MessageQueue.MigrateHotToCold' method.
*/
package realtime

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks ---

type mockPresenceCache struct {
	mock.Mock
}

func (m *mockPresenceCache) Set(ctx context.Context, key urn.URN, val routing.ConnectionInfo) error {
	args := m.Called(ctx, key, val)
	return args.Error(0)
}
func (m *mockPresenceCache) Fetch(ctx context.Context, key urn.URN) (routing.ConnectionInfo, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(routing.ConnectionInfo), args.Error(1)
}
func (m *mockPresenceCache) Delete(ctx context.Context, key urn.URN) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}
func (m *mockPresenceCache) Close() error {
	args := m.Called()
	return args.Error(0)
}

// REFACTORED: Mock for queue.MessageQueue
type mockMessageQueue struct {
	mock.Mock
}

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}
func (m *mockMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingv1.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	var result []*routingv1.QueuedMessage
	if val, ok := args.Get(0).([]*routingv1.QueuedMessage); ok {
		result = val
	}
	return result, args.Error(1)
}
func (m *mockMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}
func (m *mockMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	args := m.Called(ctx, userURN)
	return args.Error(0)
}

// testFixture holds all the components for a test.
type testFixture struct {
	cm            *ConnectionManager
	presenceCache *mockPresenceCache
	messageQueue  *mockMessageQueue
	wsServer      *httptest.Server
	userURN       urn.URN
}

// setup creates a test fixture for the ConnectionManager.
func setup(t *testing.T) *testFixture {
	t.Helper()
	logger := zerolog.Nop()

	// 1. Create Mocks
	presenceCache := new(mockPresenceCache)
	messageQueue := new(mockMessageQueue)

	// 2. Create ConnectionManager
	cm, err := NewConnectionManager(
		"0",
		middleware.NoopAuth(true, "test-user-id"),
		presenceCache,
		messageQueue,
		logger,
	)
	require.NoError(t, err, "NewConnectionManager failed")

	// 3. Create a test WebSocket server
	wsServer := httptest.NewServer(cm.server.Handler)
	t.Cleanup(wsServer.Close)

	userURN, err := urn.Parse("urn:sm:user:test-user-id")
	require.NoError(t, err)

	return &testFixture{
		cm:            cm,
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		wsServer:      wsServer,
		userURN:       userURN,
	}
}

// connectClient connects a new websocket client and waits for it to be registered.
func (fx *testFixture) connectClient(t *testing.T) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(fx.wsServer.URL, "http") + "/connect"

	// Mock the 'add' dependencies
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo")).Return(nil)

	wsClientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "Failed to dial test WebSocket server")
	t.Cleanup(func() { _ = wsClientConn.Close() })

	// Wait for the connection to be registered
	require.Eventually(t, func() bool {
		_, ok := fx.cm.connections.Load(fx.userURN.String())
		return ok
	}, 2*time.Second, 10*time.Millisecond, "User connection was not registered")

	return wsClientConn
}

func TestConnectionManager_ConnectAndDisconnect(t *testing.T) {
	fx := setup(t)

	// --- 1. Test Connect ---
	// Mock expectations for 'add'
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo")).Return(nil).Once()

	// Connect the client
	wsClientConn := fx.connectClient(t)

	// Assert 'add' was called
	fx.presenceCache.AssertCalled(t, "Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo"))

	// --- 2. Test Disconnect ---
	// Mock expectations for 'Remove'
	fx.presenceCache.On("Delete", mock.Anything, fx.userURN).Return(nil).Once()
	fx.messageQueue.On("MigrateHotToCold", mock.Anything, fx.userURN).Return(nil).Once()

	// Close the client connection to trigger the server's read loop exit
	err := wsClientConn.Close()
	require.NoError(t, err)

	// Wait for 'Remove' to be called
	require.Eventually(t, func() bool {
		// Check that the mocks were called
		return fx.presenceCache.AssertCalled(t, "Delete", mock.Anything, fx.userURN) &&
			fx.messageQueue.AssertCalled(t, "MigrateHotToCold", mock.Anything, fx.userURN)
	}, 2*time.Second, 10*time.Millisecond, "Remove() logic was not fully triggered")

	// Assert connection is gone from the map
	_, ok := fx.cm.connections.Load(fx.userURN.String())
	assert.False(t, ok, "Connection was not removed from map")
}

func TestConnectionManager_Remove_MigrationFails(t *testing.T) {
	fx := setup(t)
	testErr := errors.New("migration failed")

	// Mock expectations for 'Remove'
	fx.presenceCache.On("Delete", mock.Anything, fx.userURN).Return(nil).Once()
	// --- This is the failure ---
	fx.messageQueue.On("MigrateHotToCold", mock.Anything, fx.userURN).Return(testErr).Once()

	// Manually call Remove (as if a disconnect happened)
	fx.cm.Remove(fx.userURN)

	// Assert all mocks were called
	fx.presenceCache.AssertCalled(t, "Delete", mock.Anything, fx.userURN)
	fx.messageQueue.AssertCalled(t, "MigrateHotToCold", mock.Anything, fx.userURN)
	// The test will log an error, which is the expected behavior.
}

// DELETED: All tests related to 'deliveryProcessor'
