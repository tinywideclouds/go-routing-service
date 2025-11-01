/*
File: internal/platform/pubsub/producer_pubsub_test.go
Description: REFACTORED to test the producer with the
new 'secure.SecureEnvelope' type.
*/
package pubsub_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ps "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub" // Aliased import
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	// REFACTORED: Use new generated proto types
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
)

func TestProducer_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	// Arrange: Set up the v2 pstest in-memory server
	srv := pstest.NewServer()

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	const projectID = "test-project"
	const topicID = "test-topic"
	const subID = "test-sub"

	// Create a real client connected to the in-memory server
	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// Create the topic and subscription
	topicName := fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	require.NoError(t, err)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicName,
	})
	require.NoError(t, err)

	// Create our producer, pointing to the in-memory topic
	topic := client.Publisher(topicID)
	producer := ps.NewProducer(topic)

	// REFACTORED: Create the new "dumb" envelope
	recipientURN, err := urn.Parse("urn:sm:user:user-bob")
	require.NoError(t, err)

	testEnvelope := &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("encrypted-payload"),
	}

	// Act: Publish the message using our producer
	err = producer.Publish(ctx, testEnvelope)
	require.NoError(t, err)

	// Assert: Verify the message was received by the in-memory server
	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *pubsub.Message

	sub := client.Subscriber(subID)
	go func() {
		defer wg.Done()
		receiveCtx, cancelReceive := context.WithCancel(ctx)
		defer cancelReceive()

		err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			receivedMsg = msg
			cancelReceive()
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Receive returned an unexpected error: %v", err)
		}
	}()

	wg.Wait()

	require.NotNil(t, receivedMsg, "Did not receive a message from the subscription")

	// REFACTORED: Unmarshal into the new proto type
	var receivedEnvelopePb securev1.SecureEnvelopePb
	err = protojson.Unmarshal(receivedMsg.Data, &receivedEnvelopePb)
	require.NoError(t, err)

	// REFACTORED: Convert from proto using the new facade
	receivedEnvelope, err := secure.FromProto(&receivedEnvelopePb)
	require.NoError(t, err)

	assert.Equal(t, testEnvelope, receivedEnvelope)
}
