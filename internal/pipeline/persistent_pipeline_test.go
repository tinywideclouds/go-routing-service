//go:build integration

/*
File: internal/pipeline/persistent_pipeline_test.go
Description: REFACTORED to test the full "cold path" pipeline
using the new 'CompositeMessageQueue' and 'ServiceDependencies'.
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
	"github.com/rs/zerolog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue" // NEW
	"github.com/tinywideclouds/go-routing-service/internal/queue"                  // NEW
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Test Helpers ---

// REFACTORED: Use the mock from the processor test
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

// NEW: We only need to mock the HotQueue, as ColdQueue is what we're testing.
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

func (m *mockPPushNotifier) Notify(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, tokens, envelope)
	return args.Error(0)
}

// This is the struct *actually* stored by FirestoreColdQueue
type storedMessageForTest struct {
	QueuedAt time.Time                  `firestore:"queued_at"`
	Envelope *securev1.SecureEnvelopePb `firestore:"envelope"`
}

// TestPersistentPipeline_Integration validates the full "offline" path.
func TestPersistentPipeline_Integration(t *testing.T) {
	// 1. Arrange: Set up emulators
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	const projectID = "test-project-pipeline"
	runID := uuid.NewString()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	// 1. Setup Emulators
	pubsubConn := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID))
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(context.Background(), projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	// 3. Arrange: Create Dependencies
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]() // User is offline

	// --- REFACTORED: Use new CompositeMessageQueue ---
	const coldCollection = "test-cold-queue"
	store, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollection, logger) // Real ColdQueue
	require.NoError(t, err)

	mockHot := new(mockHotQueue) // Mocked HotQueue

	messageQueue, err := queue.NewCompositeMessageQueue(mockHot, store, logger) // Composite
	require.NoError(t, err)
	// --- END REFACTOR ---

	tokenFetcher := new(mockTokenFetcher)
	tokenFetcher.On("Fetch", mock.Anything, mock.Anything).Return([]routing.DeviceToken{}, nil) // No tokens

	pushNotifier := new(mockPPushNotifier) // Will not be called

	deps := &routing.ServiceDependencies{
		PresenceCache:      presenceCache,
		MessageQueue:       messageQueue, // Use new composite queue
		DeviceTokenFetcher: tokenFetcher,
		PushNotifier:       pushNotifier,
		// DELETED: DeliveryProducer
	}

	// 4. Arrange: Create the REAL pipeline service
	ingressTopicID := "ingress-topic-" + runID
	ingestSubID := "ingress-sub-" + runID
	createPubsubResources(t, ctx, psClient, projectID, ingressTopicID, ingestSubID)

	noisyZerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	cfg := &config.AppConfig{NumPipelineWorkers: 1}
	processor := pipeline.NewRoutingProcessor(deps, cfg, logger)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingestSubID), psClient, noisyZerologLogger,
	)
	require.NoError(t, err)

	streamingService, err := messagepipeline.NewStreamingService[secure.SecureEnvelope](
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		consumer,
		pipeline.EnvelopeTransformer,
		processor,
		noisyZerologLogger,
	)
	require.NoError(t, err)

	// 5. Arrange: Create test data
	recipientURN, _ := urn.Parse("urn:sm:user:test-offline-user")
	originalEnvelope := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(uuid.NewString()), // Unique payload
	}
	originalPayload, err := protojson.Marshal(secure.ToProto(originalEnvelope))
	require.NoError(t, err)

	// 6. Act: Start the pipeline, publish the message, and stop
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	defer pipelineCancel()

	go func() {
		_ = streamingService.Start(pipelineCtx)
	}()

	time.Sleep(100 * time.Millisecond) // Give service a moment to start

	ingestPublisher := psClient.Publisher(ingressTopicID)
	_, err = ingestPublisher.Publish(ctx, &pubsub.Message{Data: originalPayload}).Get(ctx)
	require.NoError(t, err, "Failed to publish test message")

	// 7. Assert: Check Firestore for the stored message
	// The pipeline should process and store the message in the COLD queue
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 1
	}, 10*time.Second, 100*time.Millisecond, "Message was not stored in Firestore")

	// 8. Final Verification: Retrieve the message and assert data integrity.
	docs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
	require.NoError(t, err)
	var storedData storedMessageForTest
	err = docs[0].DataTo(&storedData)
	require.NoError(t, err)

	storedEnvelope, err := secure.FromProto(storedData.Envelope)
	require.NoError(t, err)

	assert.Equal(t, originalEnvelope.EncryptedData, storedEnvelope.EncryptedData, "EncryptedData was corrupted in the pipeline")
	assert.WithinDuration(t, time.Now(), storedData.QueuedAt, 15*time.Second, "QueuedAt timestamp was not set")
	t.Log("✅ Pipeline integration test passed. Data integrity verified.")

	// 9. Cleanup
	pipelineCancel()
	_ = streamingService.Stop(ctx)
}

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
