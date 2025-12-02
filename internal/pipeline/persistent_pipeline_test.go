//go:build integration

/*
File: internal/pipeline/persistent_pipeline_test.go
Description: Full integration test for the pipeline's "cold path",
validating persistence to Firestore using real emulators.
*/

package pipeline_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog" // Keep zerolog for the external library

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Test Mocks (for dependencies not under test) ---

type mockTokenFetcher struct {
	mock.Mock
}

func (m *mockTokenFetcher) Fetch(ctx context.Context, key urn.URN) ([]routing.DeviceToken, error) {
	args := m.Called(ctx, key)
	var result []routing.DeviceToken
	if val, ok := args.Get(0).([]routing.DeviceToken); ok {
		result = val
	}
	return result, args.Error(1)
}
func (m *mockTokenFetcher) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockHotQueue struct {
	mock.Mock
}

func (m *mockHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}
func (m *mockHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingv1.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	return nil, args.Error(1)
}
func (m *mockHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}
func (m *mockHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	args := m.Called(ctx, userURN, destination)
	return args.Error(0)
}

type mockPPushNotifier struct {
	mock.Mock
}

func (m *mockPPushNotifier) NotifyOffline(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, tokens, envelope)
	return args.Error(0)
}

func (m *mockPPushNotifier) PokeOnline(ctx context.Context, urn urn.URN) error {
	args := m.Called(ctx, urn)
	return args.Error(0)
}

// storedMessageForTest is the struct *actually* stored by FirestoreColdQueue
type storedMessageForTest struct {
	QueuedAt time.Time              `firestore:"queued_at"`
	Envelope *secure.SecureEnvelope `firestore:"envelope"`
}

// TestPersistentPipeline_Integration validates the full "offline" path.
// It starts a real pipeline service with emulators and mocks.
// It publishes a message to Pub/Sub and verifies it is correctly stored
// in the Firestore cold queue.
func TestPersistentPipeline_Integration(t *testing.T) {
	// 1. Arrange: Top-level context, IDs, and logger
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel) // Will run last

	const projectID = "test-project-pipeline"
	runID := uuid.NewString()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	const coldCollection = "test-cold-queue"

	// 2. Arrange: Emulators and Clients
	// Emulators register their own container.Terminate cleanup first.
	pubsubConn := emulators.SetupPubsubEmulator(t, context.Background(), emulators.GetDefaultPubsubConfig(projectID))
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)

	firestoreConn := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(ctx, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)

	// 3. Arrange: Dependencies (Mocks and Real Firestore Queue)
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]() // User is offline
	store, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollection, logger)
	require.NoError(t, err)

	mockHot := new(mockHotQueue)
	// Force the hot queue to "fail" so the composite queue falls back to cold.
	mockHot.On("Enqueue", mock.Anything, mock.Anything).Return(fmt.Errorf("hot queue is offline"))

	messageQueue, err := queue.NewCompositeMessageQueue(mockHot, store, logger)
	require.NoError(t, err)

	tokenFetcher := new(mockTokenFetcher)
	tokenFetcher.On("Fetch", mock.Anything, mock.Anything).Return([]routing.DeviceToken{}, nil) // No tokens
	pushNotifier := new(mockPPushNotifier)

	deps := &routing.ServiceDependencies{
		PresenceCache:      presenceCache,
		MessageQueue:       messageQueue,
		DeviceTokenFetcher: tokenFetcher,
		PushNotifier:       pushNotifier,
	}

	// 4. Arrange: Create the REAL pipeline service
	ingressTopicID := "ingress-topic-" + runID
	ingestSubID := "ingress-sub-" + runID
	createPubsubResources(t, ctx, psClient, projectID, ingressTopicID, ingestSubID)

	// Use a noisy logger for pipeline debugging if necessary
	noisyZerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	cfg := &config.AppConfig{NumPipelineWorkers: 1}
	processor := pipeline.NewRoutingProcessor(deps, cfg, logger)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingestSubID), psClient, noisyZerologLogger,
	)
	require.NoError(t, err)

	streamingService, err := messagepipeline.NewStreamingService(
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		consumer,
		pipeline.EnvelopeTransformer,
		processor,
		noisyZerologLogger,
	)
	require.NoError(t, err)

	// 5. Arrange: Create test data publisher
	recipientURN, _ := urn.Parse("urn:contacts:user:test-offline-user")
	originalEnvelope := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(uuid.NewString()), // Unique payload
	}
	originalPayload, err := protojson.Marshal(secure.ToProto(originalEnvelope))
	require.NoError(t, err)

	ingestPublisher := psClient.Publisher(ingressTopicID)

	// 6. Arrange: LIFO Cleanup Stack
	// We register cleanup functions in the reverse order they should run.
	// Emulators' internal t.Cleanup (terminating containers) will run *after* all these.

	// Will run 4th (after clients): Stop the test publisher
	t.Cleanup(func() {
		t.Log("Cleanup (4): Stopping ingest publisher...")
		ingestPublisher.Stop()
		t.Log("Cleanup (4): Ingest publisher stopped.")
	})

	// Will run 3rd: Close Firestore client
	t.Cleanup(func() {
		t.Log("Cleanup (3): Closing Firestore client...")
		if err := fsClient.Close(); err != nil {
			t.Logf("Cleanup (3): fsClient.Close() error: %v", err)
		}
		t.Log("Cleanup (3): Firestore client closed.")
	})

	// Will run 2nd: Close Pub/Sub client
	t.Cleanup(func() {
		t.Log("Cleanup (2): Closing Pub/Sub client...")
		if err := psClient.Close(); err != nil {
			t.Logf("Cleanup (2): psClient.Close() error: %v", err)
		}
		t.Log("Cleanup (2): Pub/Sub client closed.")
	})

	// Will run 1st: Stop the streaming service
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		t.Log("Cleanup (1): Shutting down streaming service...")
		pipelineCancel() // Tell the pipeline to stop
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := streamingService.Stop(stopCtx); err != nil {
			t.Logf("Cleanup (1): Error stopping streaming service: %v", err)
		}
		t.Log("Cleanup (1): Streaming service stopped.")
	})

	// 7. Act: Start the pipeline, publish the message
	go func() {
		_ = streamingService.Start(pipelineCtx)
	}()
	time.Sleep(100 * time.Millisecond) // Give service a moment to start

	_, err = ingestPublisher.Publish(ctx, &pubsub.Message{Data: originalPayload}).Get(ctx)
	require.NoError(t, err, "Failed to publish test message")

	// 8. Assert: Check Firestore for the stored message
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 1
	}, 10*time.Second, 100*time.Millisecond, "Message was not stored in Firestore")

	// 9. Final Verification: Retrieve the message and assert data integrity.
	docs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
	require.NoError(t, err)
	var storedData storedMessageForTest
	err = docs[0].DataTo(&storedData)
	require.NoError(t, err)

	assert.Equal(t, originalEnvelope.EncryptedData, storedData.Envelope.EncryptedData, "EncryptedData was corrupted in the pipeline")
	assert.WithinDuration(t, time.Now(), storedData.QueuedAt, 15*time.Second, "QueuedAt timestamp was not set")
	t.Log("✅ Pipeline integration test passed. Data integrity verified.")
}

// createPubsubResources is a test helper to create topics and subscriptions.
func createPubsubResources(t *testing.T, ctx context.Context, client *pubsub.Client, projectID, topicID, subID string) {
	t.Helper()
	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("Failed to create topic: %v", err)
	}
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("Failed to create subscription: %v", err)
	}
}
