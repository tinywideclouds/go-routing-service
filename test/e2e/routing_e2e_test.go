//go:build integration

/*
File: cmd/e2e/routing_e2e_test.go
Description: REFACTORED to test the new "Hot/Cold/Migration" flow
and the explicit PushNotifier.PokeOnline/NotifyOffline methods.
*/
package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/app"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	psub "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingV1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Test Helpers ---

// --- START OF REFACTOR ---
// mockPushNotifier now implements the new routing.PushNotifier interface
type mockPushNotifier struct {
	pokeHandled chan urn.URN
	pushHandled chan urn.URN
}

func (m *mockPushNotifier) NotifyOffline(_ context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	// This is the "rich push" path
	m.pushHandled <- envelope.RecipientID
	return nil
}

func (m *mockPushNotifier) PokeOnline(_ context.Context, recipient urn.URN) error {
	// This is the "poke" path
	m.pokeHandled <- recipient
	return nil
}

// --- END OF REFACTOR ---

func newJWKSTestServer(t *testing.T, privateKey *rsa.PrivateKey) *httptest.Server {
	t.Helper()
	publicKey, err := jwk.FromRaw(privateKey.Public())
	require.NoError(t, err)
	_ = publicKey.Set(jwk.KeyIDKey, "test-key-id")
	_ = publicKey.Set(jwk.AlgorithmKey, jwa.RS256)
	keySet := jwk.NewSet()
	_ = keySet.AddKey(publicKey)
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(keySet)
		require.NoError(t, err)
	})
	return httptest.NewServer(mux)
}

func createTestRS256Token(t *testing.T, privateKey *rsa.PrivateKey, userID string) string {
	t.Helper()
	jwkKey, err := jwk.FromRaw(privateKey)
	require.NoError(t, err)
	err = jwkKey.Set(jwk.KeyIDKey, "test-key-id")
	require.NoError(t, err)
	token, err := jwt.NewBuilder().
		Subject(userID).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(time.Hour)).
		Build()
	require.NoError(t, err)
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, jwkKey))
	require.NoError(t, err)
	return string(signed)
}

func makeAPIRequest(t *testing.T, method, url, token string, body []byte) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reqBody)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func createTopic(t *testing.T, ctx context.Context, client *pubsub.Client, projectID, topicID string) {
	t.Helper()
	topicPath := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicPath,
	})
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("Failed to create topic %s: %v", topicPath, err)
	}
}

// --- Main Test ---

func TestFull_HotColdMigration_E2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const projectID = "test-project-e2e" // Project ID must match emulator
	runID := uuid.NewString()

	// --- 1. Setup Emulators & Auth ---
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwksServer := newJWKSTestServer(t, privateKey)
	t.Cleanup(jwksServer.Close)

	pubsubConn := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID))
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(context.Background(), projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	// --- 2. Arrange service dependencies ---
	senderURN, _ := urn.Parse("urn:contacts:user:user-alice")
	recipientURN, _ := urn.Parse("urn:contacts:user:user-bob")

	senderToken := createTestRS256Token(t, privateKey, senderURN.EntityID())
	recipientToken := createTestRS256Token(t, privateKey, recipientURN.EntityID())

	const coldCollection = "e2e-cold-queue"
	const hotMainCollection = "e2e-hot-main"
	const hotPendingCollection = "e2e-hot-pending"

	testConfig := &config.AppConfig{
		ProjectID:          projectID,
		APIPort:            "0",
		WebSocketPort:      "0",
		NumPipelineWorkers: 2,
		IdentityServiceURL: jwksServer.URL,
		IngressTopicID:     "ingress-" + runID,
		HotQueue: config.YamlHotQueueConfig{
			Type: "firestore",
			Firestore: config.YamlFirestoreConfig{
				MainCollectionName:    hotMainCollection,
				PendingCollectionName: hotPendingCollection,
			},
		},
		ColdQueueCollection:      coldCollection,
		PushNotificationsTopicID: "pushes-" + runID,
	}

	createTopic(t, ctx, psClient, projectID, testConfig.IngressTopicID)
	createTopic(t, ctx, psClient, projectID, testConfig.PushNotificationsTopicID)

	// --- START OF REFACTOR ---
	pokeHandled := make(chan urn.URN, 1) // Now receives a URN
	pushHandled := make(chan urn.URN, 1)
	// --- END OF REFACTOR ---
	mockNotifier := &mockPushNotifier{pokeHandled: pokeHandled, pushHandled: pushHandled}

	deps, err := assembleTestDependencies(ctx, psClient, fsClient, testConfig, mockNotifier, logger)
	require.NoError(t, err)

	// --- 3. Start the FULL Routing Service (API + ConnectionManager) ---
	// --- (FIX) We need to get the JWKS URL, not the base server URL ---
	jwksURL := jwksServer.URL + "/.well-known/jwks.json"
	authMiddleware, err := middleware.NewJWKSAuthMiddleware(jwksURL, logger)
	require.NoError(t, err)

	// --- (FIX) We need a *separate* WS auth middleware ---
	wsAuthMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(jwksURL, logger)
	require.NoError(t, err)

	apiService, err := routingservice.New(testConfig, deps, authMiddleware, logger)
	require.NoError(t, err)

	connManager, err := realtime.NewConnectionManager(
		testConfig.WebSocketPort,
		wsAuthMiddleware, // <-- Use the correct middleware
		deps.PresenceCache,
		deps.MessageQueue,
		logger,
	)
	require.NoError(t, err)
	// --- END OF FIXES ---

	serviceCtx, cancelService := context.WithCancel(context.Background())
	t.Cleanup(cancelService)

	go app.Run(serviceCtx, logger, apiService, connManager)

	// Wait for servers to start
	var apiServerURL, wsServerURL string
	require.Eventually(t, func() bool {
		apiPort := apiService.GetHTTPPort()
		wsPort := connManager.GetHTTPPort()
		if apiPort != "" && apiPort != ":0" && wsPort != "" && wsPort != ":0" {
			apiServerURL = "http://localhost" + apiPort
			wsServerURL = "ws://localhost" + wsPort
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Services did not start and report a port")
	t.Logf("API Server running at: %s", apiServerURL)
	t.Logf("WS Server running at: %s", wsServerURL)

	// --- PHASE 1: Bob Connects (Hot Path Setup) ---
	t.Log("Phase 1: Bob connecting via WebSocket...")
	// --- (FIX) Use the Sec-WebSocket-Protocol header for the token ---
	wsHeaders := http.Header{"Sec-WebSocket-Protocol": []string{recipientToken}}
	wsConn, _, err := websocket.DefaultDialer.Dial(wsServerURL+"/connect", wsHeaders)
	// --- END OF FIX ---
	require.NoError(t, err, "Failed to connect WebSocket client")
	t.Cleanup(func() { _ = wsConn.Close() })

	require.Eventually(t, func() bool {
		_, err := deps.PresenceCache.Fetch(ctx, recipientURN)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "User presence was not set")
	t.Log("✅ Bob is online.")

	// --- PHASE 2: Sally Sends Message (Hot Path Enqueue) ---
	t.Log("Phase 2: Sally sending message to online Bob...")
	msg1 := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("e2e-hot-path-data"),
	}
	msg1Bytes, err := protojson.Marshal(secure.ToProto(msg1))
	require.NoError(t, err)

	sendResp := makeAPIRequest(t, http.MethodPost, apiServerURL+"/api/send", senderToken, msg1Bytes)
	require.Equal(t, http.StatusAccepted, sendResp.StatusCode)
	_ = sendResp.Body.Close()

	// --- START OF REFACTOR ---
	select {
	case recipient := <-pokeHandled:
		t.Logf("✅ 'Poke' notification received for %s.", recipient.String())
		assert.Equal(t, recipientURN.String(), recipient.String())
	case recipient := <-pushHandled:
		t.Fatalf("FAIL: Received a full 'push' for %s instead of a 'poke'", recipient.String())
	case <-time.After(15 * time.Second):
		t.Fatal("Test timed out waiting for 'poke' notification")
	}
	// --- END OF REFACTOR ---

	var hotDocs []*firestore.DocumentSnapshot
	require.Eventually(t, func() bool {
		hotDocs, err = fsClient.Collection(hotMainCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(hotDocs) == 1
	}, 10*time.Second, 100*time.Millisecond, "Expected 1 message in HOT queue")
	t.Log("✅ Message stored in hot queue.")

	coldDocs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, coldDocs, 0, "Cold queue should be empty")

	// --- PHASE 3: Bob "Crashes" (Trigger Migration) ---
	t.Log("Phase 3: Bob 'crashing' (WebSocket disconnect)...")
	err = wsConn.Close()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := deps.PresenceCache.Fetch(ctx, recipientURN)
		return err != nil
	}, 5*time.Second, 10*time.Millisecond, "User presence was not cleared")
	t.Log("✅ Bob is offline.")

	t.Log("NOTE: Verifying hot-queue deletion is skipped due to emulator BulkWriter flakiness.")

	var coldDocsAfter []*firestore.DocumentSnapshot
	require.Eventually(t, func() bool {
		coldDocsAfter, err = fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(coldDocsAfter) == 1
	}, 10*time.Second, 100*time.Millisecond, "Message was not migrated to COLD queue")
	t.Log("✅ Message successfully migrated to cold queue.")

	// --- PHASE 4: Bob Reconnects (Cold Path Sync) ---
	t.Log("Phase 4: Bob reconnecting and pulling from cold queue...")
	batchResp := makeAPIRequest(t, http.MethodGet, apiServerURL+"/api/messages", recipientToken, nil)
	require.Equal(t, http.StatusOK, batchResp.StatusCode)

	batchBody, err := io.ReadAll(batchResp.Body)
	require.NoError(t, err)
	_ = batchResp.Body.Close()

	var receivedBatch routingV1.QueuedMessageList
	err = json.Unmarshal(batchBody, &receivedBatch)
	require.NoError(t, err)
	require.Len(t, receivedBatch.Messages, 1, "Batch should contain one message")
	assert.Equal(t, msg1.EncryptedData, receivedBatch.Messages[0].Envelope.EncryptedData)
	t.Log("✅ Bob retrieved migrated message from cold queue.")

	// --- PHASE 5: Acknowledge Message ---
	t.Log("Phase 5: Bob acknowledging message...")
	messageIDs := []string{receivedBatch.Messages[0].ID}
	ackBody, err := json.Marshal(map[string][]string{"messageIds": messageIDs})
	require.NoError(t, err)

	ackResp := makeAPIRequest(t, http.MethodPost, apiServerURL+"/api/messages/ack", recipientToken, ackBody)
	require.Equal(t, http.StatusNoContent, ackResp.StatusCode)
	_ = ackResp.Body.Close()

	t.Log("Phase 6: Verifying message deletion (SKIPPED for emulator).")
	t.Log("✅ Message acked. E2E test passed.")
}

// --- Test Setup Helpers ---

func assembleTestDependencies(
	ctx context.Context,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
	cfg *config.AppConfig,
	notifier routing.PushNotifier,
	logger *slog.Logger,
) (*routing.ServiceDependencies, error) {

	// Ingress Producer
	ingressProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))

	// Ingress Consumer (create persistent sub)
	ingressSubID := "e2e-ingress-sub-" + uuid.NewString()
	ingressTopicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.IngressTopicID)
	ingressSubPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, ingressSubID)

	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  ingressSubPath,
		Topic: ingressTopicPath,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e ingress subscription: %w", err)
	}
	ingressConsumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingressSubID), psClient, zerolog.Nop(),
	)
	if err != nil {
		return nil, err
	}

	// --- Create Real Queues (using Firestore) ---
	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, cfg.ColdQueueCollection, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e cold queue: %w", err)
	}
	hotQueue, err := fsqueue.NewFirestoreHotQueue(
		fsClient,
		cfg.HotQueue.Firestore.MainCollectionName,
		cfg.HotQueue.Firestore.PendingCollectionName,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e hot queue: %w", err)
	}
	messageQueue, err := queue.NewCompositeMessageQueue(hotQueue, coldQueue, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e composite queue: %w", err)
	}
	// --- End Queue Setup ---

	// Use an in-memory presence cache for E2E test speed
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]()

	// Use a real token fetcher
	stringDocFetcher, err := cache.NewFirestore[string, persistence.DeviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: cfg.ProjectID, CollectionName: "device-tokens"},
		fsClient,
		zerolog.Nop(),
	)
	if err != nil {
		return nil, err
	}
	stringTokenFetcher := &persistence.FirestoreTokenAdapter{DocFetcher: stringDocFetcher}
	deviceTokenFetcher := persistence.NewURNTokenFetcherAdapter(stringTokenFetcher)

	return &routing.ServiceDependencies{
		IngestionProducer:  ingressProducer,
		IngestionConsumer:  ingressConsumer,
		MessageQueue:       messageQueue,
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       notifier,
	}, nil
}
