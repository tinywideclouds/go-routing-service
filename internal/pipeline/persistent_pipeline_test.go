//go:build integration

/*
File: internal/pipeline/persistent_pipeline_test.go
Description: REFACTORED to fix all compilation errors.
- Uses new 'secure.SecureEnvelope' and 'urn' types.
- Uses 'messagepipeline.NewStreamingService'.
- Adds local 'mockTokenFetcher' and 'mockPushNotifier' helpers.
- Correctly asserts the 'storedMessage' struct in Firestore.
*/
package pipeline_test

import (
	"context"
	"fmt"
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
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	// REFACTORED: Use new generated proto types
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
)

// --- Test Helpers ---

// NEW: Added mock for DeviceTokenFetcher
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

// NEW: Added mock for PushNotifier
type mockPushNotifier struct {
	mock.Mock
}

func (m *mockPushNotifier) Notify(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, tokens, envelope)
	return args.Error(0)
}

// NEW: This is the struct we now store in Firestore
type storedMessageForTest struct {
	QueuedAt time.Time                  `firestore:"queued_at"`
	Envelope *securev1.SecureEnvelopePb `firestore:"envelope"`
}

// TestPersistentPipeline_Integration validates the full "offline" path.
// It publishes a message to the ingress topic and verifies that it is
// processed and correctly stored in Firestore.
func TestPersistentPipeline_Integration(t *testing.T) {
	// 1. Arrange: Set up emulators
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	const projectID = "test-project-pipeline"
	runID := uuid.NewString()
	logger := zerolog.Nop()

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
	store, err := persistence.NewFirestoreStore(fsClient, logger)
	require.NoError(t, err)

	tokenFetcher := new(mockTokenFetcher)
	tokenFetcher.On("Fetch", mock.Anything, mock.Anything).Return([]routing.DeviceToken{}, nil) // No tokens

	pushNotifier := new(mockPushNotifier) // Will not be called

	deps := &routing.Dependencies{
		PresenceCache:      presenceCache,
		MessageStore:       store,
		DeviceTokenFetcher: tokenFetcher,
		PushNotifier:       pushNotifier,
		// DeliveryProducer not needed for offline path
	}

	// 4. Arrange: Create the REAL pipeline service
	ingressTopicID := "ingress-topic-" + runID
	ingestSubID := "ingress-sub-" + runID
	createPubsubResources(t, ctx, psClient, projectID, ingressTopicID, ingestSubID)
	// REFACTORED: Use NewStreamingService and its config
	cfg := &config.AppConfig{NumPipelineWorkers: 1}
	processor := pipeline.NewRoutingProcessor(deps, cfg, logger)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingestSubID), psClient, logger,
	)
	require.NoError(t, err)

	streamingService, err := messagepipeline.NewStreamingService[secure.SecureEnvelope](
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		consumer,
		pipeline.EnvelopeTransformer,
		processor,
		logger,
	)
	require.NoError(t, err)

	// 5. Arrange: Create test data
	// REFACTORED: Use new "dumb" envelope and new URN types
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

	// Give the service a moment to start its workers
	time.Sleep(100 * time.Millisecond)

	ingestPublisher := psClient.Publisher(ingressTopicID)

	// Publish the raw payload
	_, err = ingestPublisher.Publish(ctx, &pubsub.Message{Data: originalPayload}).Get(ctx)
	require.NoError(t, err, "Failed to publish test message")

	// 7. Assert: Check Firestore for the stored message
	// The pipeline should process and store the message.
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 1
	}, 10*time.Second, 100*time.Millisecond, "Message was not stored in Firestore")

	// 8. Final Verification: Retrieve the message and assert data integrity.
	docs, err := fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
	require.NoError(t, err)
	var storedData storedMessageForTest // REFACTORED: Check for the stored wrapper
	err = docs[0].DataTo(&storedData)
	require.NoError(t, err)

	// REFACTORED: Check the envelope *inside* the wrapper
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
