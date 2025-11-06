/*
File: internal/realtime/connectionmanager_test.go
Description: REFACTORED to be a true integration test.
It now uses a real mock JWKS server and the *real*
NewJWKSWebsocketAuthMiddleware, removing NoopAuth.
*/
package realtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/lestrrat-go/jwx/v2/jwk"
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

// --- NEW: Test Helpers (from jwt_test.go) ---
const testKeyID = "test-key-id-1"

func createTestRS256Token(t *testing.T, userID, keyID string, privateKey *rsa.PrivateKey) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": userID,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = keyID
	return token.SignedString(privateKey)
}

func newMockJWKSServer(t *testing.T, keyID string, publicKey *rsa.PublicKey) *httptest.Server {
	t.Helper()
	jwkKey, err := jwk.FromRaw(publicKey)
	require.NoError(t, err)
	require.NoError(t, jwkKey.Set(jwk.KeyIDKey, keyID))
	require.NoError(t, jwkKey.Set(jwk.AlgorithmKey, "RS256"))
	keySet := jwk.NewSet()
	require.NoError(t, keySet.AddKey(jwkKey))
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(keySet)
		require.NoError(t, err)
	}))
}

// --- REFACTORED: testFixture ---
type testFixture struct {
	cm            *ConnectionManager
	presenceCache *mockPresenceCache
	messageQueue  *mockMessageQueue
	wsServer      *httptest.Server
	userURN       urn.URN
	wg            *sync.WaitGroup
	token         string // NEW: Store the valid token
}

// setup creates a test fixture for the ConnectionManager.
func setup(t *testing.T) *testFixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	// 1. Create Mocks
	presenceCache := new(mockPresenceCache)
	messageQueue := new(mockMessageQueue)

	// --- NEW: Create Real Auth ---
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	mockJWKSServer := newMockJWKSServer(t, testKeyID, &privateKey.PublicKey)
	t.Cleanup(mockJWKSServer.Close)

	// Create the *real* WebSocket auth middleware
	authMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(mockJWKSServer.URL, logger)
	require.NoError(t, err)

	// Create a valid token
	userURNStr := "urn:sm:user:test-user-id"
	token, err := createTestRS256Token(t, userURNStr, testKeyID, privateKey)
	require.NoError(t, err)
	// --- END NEW AUTH ---

	// 2. Create ConnectionManager (now with real auth)
	cm, err := NewConnectionManager(
		"0",
		authMiddleware, // <-- Pass in the real (mocked-server) middleware
		presenceCache,
		messageQueue,
		logger,
	)
	require.NoError(t, err, "NewConnectionManager failed")

	// 3. Create a test WebSocket server
	wsServer := httptest.NewServer(cm.server.Handler)
	t.Cleanup(wsServer.Close)

	userURN, err := urn.Parse(userURNStr)
	require.NoError(t, err)

	return &testFixture{
		cm:            cm,
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		wsServer:      wsServer,
		userURN:       userURN,
		wg:            &sync.WaitGroup{},
		token:         token, // Store the valid token
	}
}

// connectClient connects a new websocket client and waits for it to be registered.
func (fx *testFixture) connectClient(t *testing.T) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(fx.wsServer.URL, "http") + "/connect"

	// Mock the 'add' dependencies
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo")).Return(nil)

	// --- NEW: Add the token to the dialer ---
	dialer := websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
		Subprotocols:     []string{fx.token}, // Send the token here
	}

	wsClientConn, _, err := dialer.Dial(wsURL, nil) // Use the custom dialer
	require.NoError(t, err, "Failed to dial test WebSocket server")
	t.Cleanup(func() { _ = wsClientConn.Close() })
	// --- END NEW ---

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
	// Mock expectations for 'add' (unchanged)
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo")).Return(nil).Once()

	// Connect the client (this now performs a real auth check)
	wsClientConn := fx.connectClient(t)

	// Assert 'add' was called
	fx.presenceCache.AssertCalled(t, "Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo"))

	// --- 2. Test Disconnect ---

	// (This part is unchanged from your original test)
	fx.wg.Add(1)
	fx.presenceCache.On("Delete", mock.Anything, fx.userURN).Return(nil).Once()
	fx.messageQueue.On("MigrateHotToCold", mock.Anything, fx.userURN).
		Return(nil).
		Run(func(args mock.Arguments) {
			fx.wg.Done()
		}).
		Once()

	err := wsClientConn.Close()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.wg.Wait()
	}()

	select {
	case <-done:
		// Success!
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for disconnect to be processed")
	}

	fx.presenceCache.AssertCalled(t, "Delete", mock.Anything, fx.userURN)
	fx.messageQueue.AssertCalled(t, "MigrateHotToCold", mock.Anything, fx.userURN)
	_, ok := fx.cm.connections.Load(fx.userURN.String())
	assert.False(t, ok, "Connection was not removed from map")
}

func TestConnectionManager_Remove_MigrationFails(t *testing.T) {
	fx := setup(t)
	testErr := errors.New("migration failed")

	fx.wg.Add(1)
	fx.presenceCache.On("Delete", mock.Anything, fx.userURN).Return(nil).Once()
	fx.messageQueue.On("MigrateHotToCold", mock.Anything, fx.userURN).
		Return(testErr).
		Run(func(args mock.Arguments) {
			fx.wg.Done()
		}).
		Once()

	fx.cm.Remove(fx.userURN)

	// (Wait logic unchanged)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.wg.Wait()
	}()
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for Remove to complete")
	}

	fx.presenceCache.AssertCalled(t, "Delete", mock.Anything, fx.userURN)
	fx.messageQueue.AssertCalled(t, "MigrateHotToCold", mock.Anything, fx.userURN)
}
