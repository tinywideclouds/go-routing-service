//go:build integration

/*
File: internal/pipeline/onlinepath_integration_test.go
Description: REFACTORED to fix all compilation errors.
- Added the missing 'mockPushNotifier'.
- Swapped to the correct 'cache.NewMemoryCache'.
- Fixed assertion logic for the mock.
*/
package pipeline_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
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
	"github.com/tinywideclouds/go-routing-service/internal/platform/websocket"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	// REFACTORED: Use new generated proto types
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
)

// TestOnlinePath_Integration validates that the pipeline correctly routes
// a message for an ONLINE user to the delivery bus (and not to push notifications).
func TestOnlinePath_Integration(t *testing.T) {
	// 1. Arrange: Set up emulators
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	logger := zerolog.Nop()
	const projectID = "test-project-online"
	runID := uuid.NewString()

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
	// --- THIS IS THE FIX (2/2) ---
	// REFACTORED: Use correct in-memory cache
	presenceCache := cache.NewInMemoryPresenceCache[urn.URN, routing.ConnectionInfo]()
	// --- END FIX ---

	// REFACTORED: Use new MessageStore
	store, err := persistence.NewFirestoreStore(fsClient, logger)
	require.NoError(t, err)

	deliveryTopicID := "delivery-topic-" + runID
	// This is the producer for the *delivery bus*
	deliveryPubsubProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(deliveryTopicID), psClient, logger,
	)
	require.NoError(t, err)
	deliveryProducer := websocket.NewDeliveryProducer(deliveryPubsubProducer)

	mockNotifier := new(mockPushNotifier) // We will assert this is NOT called

	deps := &routing.Dependencies{
		PresenceCache:    presenceCache,
		MessageStore:     store,
		DeliveryProducer: deliveryProducer,
		PushNotifier:     mockNotifier,
		// TokenFetcher is not needed for the online path
	}

	// 4. Arrange: Create the pipeline processor
	deliverySubID := "delivery-sub-" + runID
	createPubsubResources(t, ctx, psClient, projectID, deliveryTopicID, deliverySubID) // Create delivery topic/sub
	cfg := &config.AppConfig{DeliveryTopicID: deliveryTopicID}
	processor := pipeline.NewRoutingProcessor(deps, cfg, logger)

	// 5. Arrange: Create test data
	// REFACTORED: Use new "dumb" envelope
	recipientURN, _ := urn.Parse("urn:sm:user:test-online-user")
	originalEnvelope := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(uuid.NewString()),
	}
	originalPayload, err := protojson.Marshal(secure.ToProto(originalEnvelope))
	require.NoError(t, err)

	testMsg := messagepipeline.Message{
		MessageData: messagepipeline.MessageData{
			ID:      "test-pubsub-id-123",
			Payload: originalPayload,
		},
	}
	transformedEnvelope, skipped, err := pipeline.EnvelopeTransformer(ctx, &testMsg)
	require.NoError(t, err)
	require.False(t, skipped)

	// 6. Arrange: SET THE USER ONLINE in the presence cache
	// REFACTORED: Use cache.Set
	err = presenceCache.Set(ctx, recipientURN, routing.ConnectionInfo{ServerInstanceID: "test-instance"})
	require.NoError(t, err)

	// 7. Act: Subscribe to the delivery bus (to catch the output)
	deliverySub := psClient.Subscriber(deliverySubID)
	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *pubsub.Message

	go func() {
		defer wg.Done()
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err = deliverySub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			receivedMsg = msg
			cancel() // Stop after one message
		})
	}()

	// Act: Process the message
	err = processor(ctx, testMsg, transformedEnvelope)
	require.NoError(t, err, "The routing processor failed")

	// 8. Assert: Wait for the message on the delivery bus
	wg.Wait()
	require.NotNil(t, receivedMsg, "Did not receive message on the delivery topic")
	t.Log("✅ Message correctly received on the delivery topic.")

	// 9. Assert: Check the payload on the delivery bus
	// The payload is a 'MessageData' struct, which itself contains the envelope payload
	var receivedData messagepipeline.MessageData
	err = json.Unmarshal(receivedMsg.Data, &receivedData)
	require.NoError(t, err)

	var receivedEnvelopePb securev1.SecureEnvelopePb
	err = protojson.Unmarshal(receivedData.Payload, &receivedEnvelopePb)
	require.NoError(t, err)
	receivedEnvelope, err := secure.FromProto(&receivedEnvelopePb)
	require.NoError(t, err)

	// REFACTORED: We can only assert the content, not the ID
	assert.Equal(t, originalEnvelope.EncryptedData, receivedEnvelope.EncryptedData)

	// 10. Assert: Check that push notifications were NOT sent
	mockNotifier.AssertNotCalled(t, "Notify", mock.Anything, mock.Anything, mock.Anything)
	t.Log("✅ Push notifier was not called.")

	// 11. Assert: Check that message *was* stored in Firestore (for multi-device)
	require.Eventually(t, func() bool {
		docs, err := fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages").Documents(ctx).GetAll()
		return err == nil && len(docs) == 1
	}, 5*time.Second, 100*time.Millisecond, "Message was not stored in Firestore for multi-device sync")
	t.Log("✅ Message correctly stored in Firestore.")
}
