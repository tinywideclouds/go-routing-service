// --- File: internal/pipeline/persistent_pipeline_test.go ---
//go:build integration

package pipeline_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-test/emulators"

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

type mockHotQueue struct {
	mock.Mock
}

// REFACTORED: Accept messageID
func (m *mockHotQueue) Enqueue(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, messageID, envelope).Error(0)
}
func (m *mockHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingv1.QueuedMessage, error) {
	return nil, nil
}
func (m *mockHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return nil
}
func (m *mockHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	return nil
}

type mockPPushNotifier struct {
	mock.Mock
}

func (m *mockPPushNotifier) NotifyOffline(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}

func (m *mockPPushNotifier) PokeOnline(ctx context.Context, urn urn.URN) error {
	return m.Called(ctx, urn).Error(0)
}

// FIX: Match the storage format used by FirestoreColdQueue (Protobuf)
type storedMessageForTest struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

func TestPersistentPipeline_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	const projectID = "test-project-pipeline"
	runID := uuid.NewString()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	const coldCollection = "test-cold-queue"

	// 2. Arrange: Emulators
	pubsubConn := emulators.SetupPubsubEmulator(t, context.Background(), emulators.GetDefaultPubsubConfig(projectID))
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)

	firestoreConn := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(ctx, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)

	// 3. Arrange: Dependencies
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]() // Offline
	store, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollection, logger)
	require.NoError(t, err)

	mockHot := new(mockHotQueue)
	// REFACTORED: Mock expectations to accept ID (mock.Anything)
	mockHot.On("Enqueue", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("hot queue offline"))

	messageQueue, err := queue.NewCompositeMessageQueue(mockHot, store, logger)
	require.NoError(t, err)

	pushNotifier := new(mockPPushNotifier)
	pushNotifier.On("NotifyOffline", mock.Anything, mock.Anything).Return(nil)

	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
		PushNotifier:  pushNotifier,
	}

	// 4. Arrange: Create Pipeline
	ingressTopicID := "ingress-topic-" + runID
	ingestSubID := "ingress-sub-" + runID
	createPubsubResources(t, ctx, psClient, projectID, ingressTopicID, ingestSubID)

	cfg := &config.AppConfig{NumPipelineWorkers: 1}
	processor := pipeline.NewRoutingProcessor(deps, cfg, logger)
	consumer, err := messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(ingestSubID), psClient, logger,
	)
	require.NoError(t, err)

	streamingService, err := messagepipeline.NewStreamingService(
		messagepipeline.StreamingServiceConfig{NumWorkers: cfg.NumPipelineWorkers},
		consumer,
		pipeline.EnvelopeTransformer,
		processor,
		logger,
	)
	require.NoError(t, err)

	// 5. Arrange: Publisher
	recipientURN, _ := urn.Parse("urn:contacts:user:test-offline-user")
	originalEnvelope := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(uuid.NewString()),
	}
	originalPayload, err := protojson.Marshal(secure.ToProto(originalEnvelope))
	require.NoError(t, err)

	ingestPublisher := psClient.Publisher(ingressTopicID)

	t.Cleanup(func() { ingestPublisher.Stop() })
	t.Cleanup(func() { _ = fsClient.Close() })
	t.Cleanup(func() { _ = psClient.Close() })
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		pipelineCancel()
		_ = streamingService.Stop(context.Background())
	})

	// 7. Act
	go func() { _ = streamingService.Start(pipelineCtx) }()
	time.Sleep(100 * time.Millisecond)

	_, err = ingestPublisher.Publish(ctx, &pubsub.Message{Data: originalPayload}).Get(ctx)
	require.NoError(t, err)

	// 8. Assert
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 1
	}, 10*time.Second, 100*time.Millisecond)

	docs, _ := fsClient.Collection(coldCollection).Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
	var storedData storedMessageForTest
	err = docs[0].DataTo(&storedData)
	require.NoError(t, err)

	// FIX: Convert the retrieved Proto back to Native for comparison
	retrievedEnvelope, err := secure.FromProto(storedData.Envelope)
	require.NoError(t, err)

	assert.Equal(t, originalEnvelope.EncryptedData, retrievedEnvelope.EncryptedData)
}

func createPubsubResources(t *testing.T, ctx context.Context, client *pubsub.Client, projectID, topicID, subID string) {
	t.Helper()
	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, _ = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, _ = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: subName, Topic: topicName})
}
