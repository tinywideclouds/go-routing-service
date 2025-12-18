// --- File: cmd/e2e/routing_e2e_test.go ---
//go:build integration

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/app"
	psub "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// ... (Test Helpers: newJWKSTestServer, createTestRS256Token, makeAPIRequest, createTopic remain same) ...

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

// Assemble Dependencies with REAL Push Notifier and Ephemeral Poke Consumer
func assembleTestDependencies(
	ctx context.Context,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
	cfg *config.AppConfig,
	logger *slog.Logger,
) (*routing.ServiceDependencies, messagepipeline.MessageConsumer, error) {

	ingressProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))

	ingressSubID := "e2e-ingress-sub-" + uuid.NewString()
	ingressTopicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.IngressTopicID)
	ingressSubPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, ingressSubID)

	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  ingressSubPath,
		Topic: ingressTopicPath,
	})
	if err != nil {
		return nil, nil, err
	}

	ingressConsumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingressSubID), psClient, logger,
	)
	if err != nil {
		return nil, nil, err
	}

	coldQueue, _ := fsqueue.NewFirestoreColdQueue(fsClient, cfg.ColdQueueCollection, logger)
	hotQueue, _ := fsqueue.NewFirestoreHotQueue(fsClient, cfg.HotQueue.Firestore.MainCollectionName, cfg.HotQueue.Firestore.PendingCollectionName, logger)
	messageQueue, _ := queue.NewCompositeMessageQueue(hotQueue, coldQueue, logger)

	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]()

	// REAL Push Notifier
	pushProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, logger,
	)
	if err != nil {
		return nil, nil, err
	}
	realPushNotifier, err := push.NewPubSubNotifier(pushProducer, logger)
	if err != nil {
		return nil, nil, err
	}

	// Ephemeral Poke Consumer (Fan-Out for Test)
	pokeSubID := "e2e-poke-sub-" + uuid.NewString()
	pokeSubPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, pokeSubID)
	pushTopicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.PushNotificationsTopicID)

	_, err = psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  pokeSubPath,
		Topic: pushTopicPath,
	})
	if err != nil {
		return nil, nil, err
	}

	pokeConsumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(pokeSubID), psClient, logger,
	)
	if err != nil {
		return nil, nil, err
	}

	deps := &routing.ServiceDependencies{
		IngestionProducer: ingressProducer,
		IngestionConsumer: ingressConsumer,
		MessageQueue:      messageQueue,
		PresenceCache:     presenceCache,
		PushNotifier:      realPushNotifier,
	}

	return deps, pokeConsumer, nil
}

func TestFull_HotColdMigration_E2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const projectID = "test-project-e2e"
	runID := uuid.NewString()

	// --- 1. Setup Emulators ---
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

	// --- 2. Config & Deps ---
	senderURN, _ := urn.Parse("urn:contacts:user:user-alice")
	recipientURN, _ := urn.Parse("urn:contacts:user:user-bob")
	senderToken := createTestRS256Token(t, privateKey, senderURN.EntityID())
	recipientToken := createTestRS256Token(t, privateKey, recipientURN.EntityID())

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
				MainCollectionName:    "e2e-hot-main",
				PendingCollectionName: "e2e-hot-pending",
			},
		},
		ColdQueueCollection:      "e2e-cold-queue",
		PushNotificationsTopicID: "pushes-" + runID,
	}

	createTopic(t, ctx, psClient, projectID, testConfig.IngressTopicID)
	createTopic(t, ctx, psClient, projectID, testConfig.PushNotificationsTopicID)

	deps, pokeConsumer, err := assembleTestDependencies(ctx, psClient, fsClient, testConfig, logger)
	require.NoError(t, err)

	jwksURL := jwksServer.URL + "/.well-known/jwks.json"
	authMiddleware, _ := middleware.NewJWKSAuthMiddleware(jwksURL, logger)
	wsAuthMiddleware, _ := middleware.NewJWKSWebsocketAuthMiddleware(jwksURL, logger)

	apiService, err := routingservice.New(testConfig, deps, authMiddleware, logger)
	require.NoError(t, err)

	connManager, err := realtime.NewConnectionManager(
		testConfig.WebSocketPort,
		wsAuthMiddleware,
		deps.PresenceCache,
		deps.MessageQueue,
		pokeConsumer, // Real consumer attached
		logger,
	)
	require.NoError(t, err)

	serviceCtx, cancelService := context.WithCancel(context.Background())
	t.Cleanup(cancelService)
	go app.Run(serviceCtx, logger, apiService, connManager)

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
	}, 10*time.Second, 100*time.Millisecond, "Services did not start")

	// --- PHASE 1: Bob Connects ---
	wsHeaders := http.Header{"Sec-WebSocket-Protocol": []string{recipientToken}}
	wsConn, _, err := websocket.DefaultDialer.Dial(wsServerURL+"/connect", wsHeaders)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wsConn.Close() })

	// Wait for Presence
	require.Eventually(t, func() bool {
		sessions, err := deps.PresenceCache.FetchSessions(ctx, recipientURN)
		return err == nil && len(sessions) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// --- PHASE 2: Send Message -> Expect Poke ---
	msg1 := &secure.SecureEnvelope{RecipientID: recipientURN, EncryptedData: []byte("e2e-hot-path-data")}
	msg1Bytes, _ := protojson.Marshal(secure.ToProto(msg1))
	sendResp := makeAPIRequest(t, http.MethodPost, apiServerURL+"/api/send", senderToken, msg1Bytes)
	require.Equal(t, http.StatusAccepted, sendResp.StatusCode)
	_ = sendResp.Body.Close()

	// Verify WebSocket Poke
	wsConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, p, err := wsConn.ReadMessage()
	require.NoError(t, err, "Failed to receive poke via WebSocket")
	assert.JSONEq(t, "{}", string(p))

	// --- PHASE 3: Disconnect & Migrate ---
	wsConn.Close()
	require.Eventually(t, func() bool {
		sessions, err := deps.PresenceCache.FetchSessions(ctx, recipientURN)
		return err != nil || len(sessions) == 0
	}, 5*time.Second, 10*time.Millisecond)

	var coldDocsAfter []*firestore.DocumentSnapshot
	require.Eventually(t, func() bool {
		coldDocsAfter, err = fsClient.Collection("e2e-cold-queue").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(coldDocsAfter) == 1
	}, 10*time.Second, 100*time.Millisecond)
}
