/*
File: internal/realtime/connectionmanager_test.go
Description: Verifies Handle-based connection registration.
*/
package realtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks (Same as before) ---
type mockPresenceCache struct{ mock.Mock }

func (m *mockPresenceCache) Set(ctx context.Context, key urn.URN, val routing.ConnectionInfo) error {
	return m.Called(ctx, key, val).Error(0)
}
func (m *mockPresenceCache) Fetch(ctx context.Context, key urn.URN) (routing.ConnectionInfo, error) {
	return m.Called(ctx, key).Get(0).(routing.ConnectionInfo), m.Called(ctx, key).Error(1)
}
func (m *mockPresenceCache) Delete(ctx context.Context, key urn.URN) error {
	return m.Called(ctx, key).Error(0)
}
func (m *mockPresenceCache) Close() error {
	return m.Called().Error(0)
}

type mockMessageQueue struct{ mock.Mock }

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingv1.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	if val, ok := args.Get(0).([]*routingv1.QueuedMessage); ok {
		return val, args.Error(1)
	}
	return nil, args.Error(1)
}
func (m *mockMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return m.Called(ctx, userURN, messageIDs).Error(0)
}
func (m *mockMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	return m.Called(ctx, userURN).Error(0)
}

// --- Test Helpers ---
const testKeyID = "test-key-id-1"

func createTestToken(_ *testing.T, userID, handle, keyID string, privateKey *rsa.PrivateKey) (string, error) {
	// Add 'handle' to claims to test resolution priority
	claims := jwt.MapClaims{
		"sub": userID,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	if handle != "" {
		claims["handle"] = handle
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
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
		_ = json.NewEncoder(w).Encode(keySet)
	}))
}

// --- Test Fixture ---
type testFixture struct {
	cm            *ConnectionManager
	presenceCache *mockPresenceCache
	messageQueue  *mockMessageQueue
	wsServer      *httptest.Server
	userURN       urn.URN // This will now be the HANDLE URN
	wg            *sync.WaitGroup
	token         string
}

func setup(t *testing.T) *testFixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	presenceCache := new(mockPresenceCache)
	messageQueue := new(mockMessageQueue)

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	mockJWKSServer := newMockJWKSServer(t, testKeyID, &privateKey.PublicKey)
	t.Cleanup(mockJWKSServer.Close)

	authMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(mockJWKSServer.URL, logger)
	require.NoError(t, err)

	// Create a token with BOTH identity and handle
	identityURNStr := "urn:auth:google:123"
	handleURNStr := "urn:lookup:email:test@test.com"

	token, err := createTestToken(t, identityURNStr, handleURNStr, testKeyID, privateKey)
	require.NoError(t, err)

	cm, err := NewConnectionManager("0", authMiddleware, presenceCache, messageQueue, logger)
	require.NoError(t, err)

	wsServer := httptest.NewServer(cm.server.Handler)
	t.Cleanup(wsServer.Close)

	// We expect the system to resolve to the HANDLE
	expectedURN, _ := urn.Parse(handleURNStr)

	return &testFixture{
		cm:            cm,
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		wsServer:      wsServer,
		userURN:       expectedURN, // Expectations use this
		wg:            &sync.WaitGroup{},
		token:         token,
	}
}

func (fx *testFixture) connectClient(t *testing.T) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(fx.wsServer.URL, "http") + "/connect"

	dialer := websocket.Dialer{
		HandshakeTimeout: 45 * time.Second,
		Subprotocols:     []string{fx.token},
	}

	wsClientConn, _, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wsClientConn.Close() })

	require.Eventually(t, func() bool {
		// Check if connection is stored under the HANDLE URN
		_, ok := fx.cm.connections.Load(fx.userURN.String())
		return ok
	}, 2*time.Second, 10*time.Millisecond)

	return wsClientConn
}

func TestConnectionManager_Connect_UsesHandle(t *testing.T) {
	fx := setup(t)

	// Expectation: Presence is set for the HANDLE URN
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.AnythingOfType("routing.ConnectionInfo")).Return(nil).Once()

	_ = fx.connectClient(t)

	fx.presenceCache.AssertExpectations(t)
}

func TestConnectionManager_Disconnect_UsesHandle(t *testing.T) {
	fx := setup(t)
	fx.presenceCache.On("Set", mock.Anything, fx.userURN, mock.Anything).Return(nil)
	wsClientConn := fx.connectClient(t)

	// Expectation: Delete/Migrate called for HANDLE URN
	fx.wg.Add(1)
	fx.presenceCache.On("Delete", mock.Anything, fx.userURN).Return(nil).Once()
	fx.messageQueue.On("MigrateHotToCold", mock.Anything, fx.userURN).
		Return(nil).
		Run(func(args mock.Arguments) { fx.wg.Done() }).
		Once()

	_ = wsClientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout")
	}

	fx.presenceCache.AssertExpectations(t)
	fx.messageQueue.AssertExpectations(t)
}
