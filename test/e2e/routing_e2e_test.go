//go:build integration

/*
File: cmd/e2e/routing_e2e_test.go
Description: REFACTORED to test the new "Reliable Dumb Queue"
(Pull/Ack) API flow instead of the old "Digest/History" API.
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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-routing-service/internal/app"
	"github.com/illmade-knight/go-routing-service/internal/platform/persistence"
	psub "github.com/illmade-knight/go-routing-service/internal/platform/pubsub"
	"github.com/illmade-knight/go-routing-service/internal/platform/websocket"
	"github.com/illmade-knight/go-routing-service/internal/realtime"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/illmade-knight/go-routing-service/routingservice"
	"github.com/illmade-knight/go-routing-service/routingservice/config"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	// REFACTORED: Use new 'tinywideclouds' and 'go-platform' packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingV1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Test Helpers ---

// REFACTORED: Mock uses new 'secure.SecureEnvelope'
type mockPushNotifier struct {
	handled chan urn.URN
}

func (m *mockPushNotifier) Notify(_ context.Context, _ []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	m.handled <- envelope.RecipientID
	return nil
}

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

// --- Main Test ---

// REFACTORED: Renamed to TestFullSendPullAckFlow
func TestFullSendPullAckFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	logger := zerolog.New(zerolog.NewTestWriter(t))
	const projectID = "test-project-e2e"
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
	// REFACTORED: Use new URNs
	senderURN, _ := urn.Parse("urn:sm:user:user-alice")
	recipientURN, _ := urn.Parse("urn:sm:user:user-bob")

	// Auth tokens for both users
	senderToken := createTestRS256Token(t, privateKey, senderURN.EntityID())
	recipientToken := createTestRS256Token(t, privateKey, recipientURN.EntityID())

	ingressTopicName := "ingress-" + runID
	ingressTopicID := fmt.Sprintf("projects/%s/topics/%s", projectID, ingressTopicName)

	deliveryTopicName := "delivery-" + runID
	deliveryTopicID := fmt.Sprintf("projects/%s/topics/%s", projectID, deliveryTopicName)

	testConfig := &config.AppConfig{
		ProjectID:                         projectID,
		APIPort:                           "0",
		WebSocketPort:                     "0",
		NumPipelineWorkers:                2,
		Cors:                              config.YamlCorsConfig{AllowedOrigins: []string{"*"}, Role: "admin"},
		IdentityServiceURL:                jwksServer.URL,
		IngressTopicID:                    ingressTopicID,
		DeliveryTopicID:                   deliveryTopicID,
		DeliveryBusSubscriptionExpiration: "24h",
		// Other config fields (DLQ, etc.) are not critical for this test
	}

	createTopic(t, ctx, psClient, testConfig.IngressTopicID)
	createTopic(t, ctx, psClient, testConfig.DeliveryTopicID)

	_, err = fsClient.Collection("device-tokens").Doc(recipientURN.String()).Set(ctx, map[string]interface{}{
		"Tokens": []routing.DeviceToken{{Token: "persistent-device-token-123", Platform: "ios"}},
	})
	require.NoError(t, err)

	// We expect 2 push notifications
	offlineHandled := make(chan urn.URN, 2)
	deps, err := assembleTestDependencies(ctx, psClient, fsClient, testConfig, &mockPushNotifier{handled: offlineHandled}, logger)
	require.NoError(t, err)

	// --- 3. Start the FULL Routing Service (API + ConnectionManager) ---
	// REFACTORED: Use correct JWKS URL
	authMiddleware, err := middleware.NewJWKSAuthMiddleware(jwksServer.URL + "/.well-known/jwks.json")
	require.NoError(t, err)

	apiService, err := routingservice.New(testConfig, deps, authMiddleware, logger)
	require.NoError(t, err)

	connManager, err := realtime.NewConnectionManager(
		testConfig.WebSocketPort, // Corrected: Port needs ":" prefix, but NewConnectionManager handles it.
		authMiddleware,
		deps.PresenceCache,
		deps.DeliveryConsumer,
		logger,
	)
	require.NoError(t, err)

	serviceCtx, cancelService := context.WithCancel(context.Background())
	t.Cleanup(cancelService)

	go app.Run(serviceCtx, logger, apiService, connManager)

	var routingServerURL string
	require.Eventually(t, func() bool {
		port := apiService.GetHTTPPort()
		if port != "" && port != ":0" {
			routingServerURL = "http://localhost" + port
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "API service did not start and report a port")

	// --- PHASE 1: Send messages ---
	t.Log("Phase 1: Sending two 1-to-1 messages to offline user...")

	// REFACTORED: Create new "dumb" envelopes
	msg1 := &secure.SecureEnvelope{
		RecipientID:           recipientURN,
		EncryptedData:         []byte("e2e-data-1"),
		EncryptedSymmetricKey: []byte("e2e-key-1"),
	}
	protoMsg1 := secure.ToProto(msg1)
	msg1Bytes, err := protojson.Marshal(protoMsg1)
	require.NoError(t, err)

	sendResp1 := makeAPIRequest(t, http.MethodPost, routingServerURL+"/api/send", senderToken, msg1Bytes)
	require.Equal(t, http.StatusAccepted, sendResp1.StatusCode)
	_ = sendResp1.Body.Close()

	msg2 := &secure.SecureEnvelope{
		RecipientID:           recipientURN,
		EncryptedData:         []byte("e2e-data-2"),
		EncryptedSymmetricKey: []byte("e2e-key-2"),
	}
	protoMsg2 := secure.ToProto(msg2)
	msg2Bytes, err := protojson.Marshal(protoMsg2)
	require.NoError(t, err)

	sendResp2 := makeAPIRequest(t, http.MethodPost, routingServerURL+"/api/send", senderToken, msg2Bytes)
	require.Equal(t, http.StatusAccepted, sendResp2.StatusCode)
	_ = sendResp2.Body.Close()

	// Assert: Check for 2 push notifications
	for i := 0; i < 2; i++ {
		select {
		case receivedRecipientURN := <-offlineHandled:
			require.Equal(t, recipientURN, receivedRecipientURN)
		case <-time.After(15 * time.Second):
			t.Fatal("Test timed out waiting for push notifications")
		}
	}
	t.Log("✅ Push notifications correctly triggered for both messages.")

	// Assert: Check that 2 messages are stored in Firestore
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 2
	}, 10*time.Second, 100*time.Millisecond, "Expected 2 messages to be stored")
	t.Log("✅ Both messages correctly stored in Firestore.")

	// --- PHASE 2: Retrieve Batch (as Recipient) ---
	t.Log("Phase 2: Retrieving message batch...")
	batchResp := makeAPIRequest(t, http.MethodGet, routingServerURL+"/api/messages", recipientToken, nil)
	require.Equal(t, http.StatusOK, batchResp.StatusCode)

	batchBody, err := io.ReadAll(batchResp.Body)
	require.NoError(t, err)
	_ = batchResp.Body.Close()

	var receivedBatch routingV1.QueuedMessageList
	err = json.Unmarshal(batchBody, &receivedBatch) // Use standard JSON unmarshal
	require.NoError(t, err)

	// Assert: Batch should contain 2 messages
	require.Len(t, receivedBatch.Messages, 2, "Batch should contain two messages")
	// Note: Order is guaranteed by QueuedAt, so msg1 should be first.
	assert.Equal(t, msg1.EncryptedData, receivedBatch.Messages[0].Envelope.EncryptedData)
	assert.Equal(t, msg2.EncryptedData, receivedBatch.Messages[1].Envelope.EncryptedData)
	assert.NotEmpty(t, receivedBatch.Messages[0].ID, "Queue ID should not be empty")
	t.Log("✅ Correct message batch retrieved.")

	// --- PHASE 3: Acknowledge Messages (as Recipient) ---
	t.Log("Phase 3: Acknowledging messages...")
	messageIDs := []string{receivedBatch.Messages[0].ID, receivedBatch.Messages[1].ID}
	ackBody, err := json.Marshal(map[string][]string{"messageIds": messageIDs})
	require.NoError(t, err)

	ackResp := makeAPIRequest(t, http.MethodPost, routingServerURL+"/api/messages/ack", recipientToken, ackBody)
	require.Equal(t, http.StatusNoContent, ackResp.StatusCode)
	_ = ackResp.Body.Close()

	// --- PHASE 4: Verify Deletion ---
	t.Log("Phase 4: Verifying message deletion...")

	require.Eventually(t, func() bool {
		docsAfter, err := fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		require.NoError(t, err, "Firestore query failed during final check")
		return len(docsAfter) == 0 // The condition we are waiting for
	}, 5*time.Second, 100*time.Millisecond, "Expected messages to be deleted after retrieval")

	t.Log("✅ Messages correctly deleted after retrieval.")
}

// --- Test Setup Helpers ---

// REFACTORED: This helper is now updated with all new types
func assembleTestDependencies(
	ctx context.Context,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
	cfg *config.AppConfig,
	notifier routing.PushNotifier,
	logger zerolog.Logger,
) (*routing.Dependencies, error) {

	// Ingress Producer
	ingressProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))

	// Delivery Producer
	dataflowProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.DeliveryTopicID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}
	deliveryProducer := websocket.NewDeliveryProducer(dataflowProducer)

	// Ingress Consumer (create persistent sub)
	ingressSubID := "e2e-ingress-sub-" + uuid.NewString()
	ingressSubPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, ingressSubID)
	_, err = psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  ingressSubPath,
		Topic: cfg.IngressTopicID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e ingress subscription: %w", err)
	}
	ingressConsumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingressSubID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}

	// Delivery Consumer (create ephemeral sub)
	deliverySubID := "e2e-delivery-sub-" + uuid.NewString()
	deliverySubPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, deliverySubID)
	exp, _ := time.ParseDuration(cfg.DeliveryBusSubscriptionExpiration) // Assume valid
	_, err = psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:             deliverySubPath,
		Topic:            cfg.DeliveryTopicID,
		ExpirationPolicy: &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(exp)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create e2e delivery subscription: %w", err)
	}
	deliveryConsumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(deliverySubID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}

	// Stores and Caches
	messageStore, err := persistence.NewFirestoreStore(fsClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create firestore store for test: %w", err)
	}

	// REFACTORED: Use correct in-memory cache
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]()

	stringDocFetcher, err := cache.NewFirestore[string, persistence.DeviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: cfg.ProjectID, CollectionName: "device-tokens"},
		fsClient,
		logger,
	)
	if err != nil {
		return nil, err
	}
	stringTokenFetcher := &persistence.FirestoreTokenAdapter{DocFetcher: stringDocFetcher}
	deviceTokenFetcher := persistence.NewURNTokenFetcherAdapter(stringTokenFetcher)

	return &routing.Dependencies{
		IngestionProducer:  ingressProducer,
		IngestionConsumer:  ingressConsumer,
		DeliveryProducer:   deliveryProducer,
		DeliveryConsumer:   deliveryConsumer,
		MessageStore:       messageStore,
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       notifier,
	}, nil
}

func createTopic(t *testing.T, ctx context.Context, client *pubsub.Client, topicID string) {
	t.Helper()

	topic, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicID,
	})
	require.NoError(t, err)

	require.NotNil(t, topic)

	t.Cleanup(func() {
		_ = client.TopicAdminClient.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: topicID})
	})
}
