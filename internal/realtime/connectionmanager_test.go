// --- File: internal/realtime/connectionmanager_test.go ---
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
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks ---

type mockPokeConsumer struct {
	mock.Mock
	msgChan  chan messagepipeline.Message
	doneChan chan struct{}
}

func newMockPokeConsumer() *mockPokeConsumer {
	return &mockPokeConsumer{
		msgChan:  make(chan messagepipeline.Message, 10),
		doneChan: make(chan struct{}),
	}
}

func (m *mockPokeConsumer) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *mockPokeConsumer) Stop(ctx context.Context) error {
	close(m.msgChan)
	close(m.doneChan)
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *mockPokeConsumer) Messages() <-chan messagepipeline.Message {
	return m.msgChan
}
func (m *mockPokeConsumer) Done() <-chan struct{} {
	return m.doneChan
}

type mockPresenceCache struct{ mock.Mock }

func (m *mockPresenceCache) AddSession(ctx context.Context, key urn.URN, sessionID string, val routing.ConnectionInfo) error {
	return m.Called(ctx, key, sessionID, val).Error(0)
}
func (m *mockPresenceCache) RemoveSession(ctx context.Context, key urn.URN, sessionID string) (int, error) {
	args := m.Called(ctx, key, sessionID)
	return args.Int(0), args.Error(1)
}
func (m *mockPresenceCache) FetchSessions(ctx context.Context, key urn.URN) (map[string]routing.ConnectionInfo, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(map[string]routing.ConnectionInfo), args.Error(1)
}
func (m *mockPresenceCache) Clear(ctx context.Context, key urn.URN) error {
	return m.Called(ctx, key).Error(0)
}
func (m *mockPresenceCache) Close() error { return m.Called().Error(0) }

type mockMessageQueue struct{ mock.Mock }

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, messageID, envelope).Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, messageID, envelope).Error(0)
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

// ... Helpers ...
const testKeyID = "test-key-id-1"

func createTestToken(_ *testing.T, userID, handle, keyID string, privateKey *rsa.PrivateKey) (string, error) {
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

type testFixture struct {
	cm            *ConnectionManager
	presenceCache *mockPresenceCache
	messageQueue  *mockMessageQueue
	pokeConsumer  *mockPokeConsumer
	wsServer      *httptest.Server
	userURN       urn.URN
	wg            *sync.WaitGroup
	token         string
}

func setup(t *testing.T) *testFixture {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	presenceCache := new(mockPresenceCache)
	messageQueue := new(mockMessageQueue)
	pokeConsumer := newMockPokeConsumer()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	mockJWKSServer := newMockJWKSServer(t, testKeyID, &privateKey.PublicKey)
	t.Cleanup(mockJWKSServer.Close)

	authMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(mockJWKSServer.URL, logger)
	require.NoError(t, err)

	identityURNStr := "urn:auth:google:123"
	handleURNStr := "urn:lookup:email:test@test.com"

	token, err := createTestToken(t, identityURNStr, handleURNStr, testKeyID, privateKey)
	require.NoError(t, err)

	cm, err := NewConnectionManager("0", authMiddleware, presenceCache, messageQueue, pokeConsumer, logger)
	require.NoError(t, err)

	wsServer := httptest.NewServer(cm.server.Handler)
	t.Cleanup(wsServer.Close)

	expectedURN, _ := urn.Parse(handleURNStr)

	return &testFixture{
		cm:            cm,
		presenceCache: presenceCache,
		messageQueue:  messageQueue,
		pokeConsumer:  pokeConsumer,
		wsServer:      wsServer,
		userURN:       expectedURN,
		wg:            &sync.WaitGroup{},
		token:         token,
	}
}

func (fx *testFixture) connectClient(t *testing.T) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(fx.wsServer.URL, "http") + "/connect"
	dialer := websocket.Dialer{HandshakeTimeout: 45 * time.Second, Subprotocols: []string{fx.token}}
	wsClientConn, _, err := dialer.Dial(wsURL, nil)
	require.NoError(t, err)
	return wsClientConn
}

func TestConnectionManager_Connect_UsesHandle(t *testing.T) {
	fx := setup(t)

	// Since Start() is NOT called, we don't mock pokeConsumer lifecycle here.
	fx.presenceCache.On("AddSession", mock.Anything, fx.userURN, mock.AnythingOfType("string"), mock.AnythingOfType("routing.ConnectionInfo")).Return(nil).Once()

	// FIX: Return 1 to simulate "other sessions exist".
	// This prevents the connection manager from calling MigrateHotToCold (which we don't want to test/mock here).
	fx.presenceCache.On("RemoveSession", mock.Anything, mock.Anything, mock.Anything).Return(1, nil)

	conn := fx.connectClient(t)
	defer conn.Close()

	// Wait briefly to allow async operations
	time.Sleep(10 * time.Millisecond)
}

func TestConnectionManager_HandlePoke_RoutesToCorrectSession(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// 1. Expect Consumer Lifecycle
	fx.pokeConsumer.On("Start", mock.Anything).Return(nil)
	fx.pokeConsumer.On("Stop", mock.Anything).Return(nil)

	// 2. Start Manager
	go fx.cm.Start(ctx)
	defer fx.cm.Shutdown(ctx)

	// Wait a moment for the Start goroutine to launch the consumer loop
	time.Sleep(50 * time.Millisecond)

	// 3. Connect Client
	fx.presenceCache.On("AddSession", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Return 0 here to explicitly ALLOW migration (and verify it works with the expectation below)
	fx.presenceCache.On("RemoveSession", mock.Anything, mock.Anything, mock.Anything).Return(0, nil)

	fx.messageQueue.On("MigrateHotToCold", mock.Anything, mock.Anything).Return(nil)

	wsClientConn := fx.connectClient(t)
	defer wsClientConn.Close()

	// 4. Send Poke
	ackWg := sync.WaitGroup{}
	ackWg.Add(1)

	pokePayload := `{"type": "poke", "recipient": "` + fx.userURN.String() + `"}`

	// FIX: Double-wrap the payload to match handlePoke's logic
	// The Consumer unwraps Message -> MessageData.
	// Then handlePoke unwraps MessageData.Payload -> Poke.
	// So we must simulate a MessageData struct serialized as JSON in the main Payload.
	wrapperData := messagepipeline.MessageData{
		ID:      "poke-wrapper",
		Payload: []byte(pokePayload),
	}
	wrapperBytes, _ := json.Marshal(wrapperData)

	msg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{
			ID:      "msg-1",
			Payload: wrapperBytes, // The "Outer" Payload is the JSON of MessageData
		},
		Ack: func() { ackWg.Done() },
	}

	// Send to consumer channel
	fx.pokeConsumer.msgChan <- msg

	// 5. Verify Receipt
	wsClientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, p, err := wsClientConn.ReadMessage()
	require.NoError(t, err, "Client should receive poke signal")
	assert.JSONEq(t, "{}", string(p))

	// 6. Verify Ack
	ackWg.Wait()
}
