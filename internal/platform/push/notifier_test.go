/*
File: internal/platform/push/notifier_test.go
Description: NEW unit test for the PubSubNotifier.
*/
package push_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// mockEventProducer mocks the new EventProducer interface
type mockEventProducer struct {
	mock.Mock
}

func (m *mockEventProducer) Publish(ctx context.Context, data messagepipeline.MessageData) (string, error) {
	args := m.Called(ctx, data)
	return args.String(0), args.Error(1)
}

// TestNotify tests the PubSubNotifier's core logic
func TestNotify(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()
	testURN, _ := urn.Parse("urn:sm:user:test-user")

	// The envelope is now only used to satisfy the interface,
	// its content should NOT be in the push.
	testEnvelope := &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("this-should-not-be-in-the-push"),
	}

	testTokens := []routing.DeviceToken{
		{Token: "ios-token-123", Platform: "ios"},
		{Token: "android-token-456", Platform: "android"},
	}

	t.Run("Success - Publishes correct 'dumb' payload", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		// We capture the MessageData sent to the producer
		var capturedData messagepipeline.MessageData
		producer.On("Publish", ctx, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedData = args.Get(1).(messagepipeline.MessageData)
			}).
			Return("mock-message-id", nil)

		// Act
		err = notifier.Notify(ctx, testTokens, testEnvelope)

		// Assert
		require.NoError(t, err)
		producer.AssertCalled(t, "Publish", ctx, mock.Anything)

		// This is the most important assertion:
		// We verify the payload is "dumb" and contains no metadata.

		// 1. Unmarshal the outer payload
		var actualRequest struct {
			Tokens  []routing.DeviceToken `json:"tokens"`
			Content struct {
				Title string `json:"title"`
				Body  string `json:"body"`
			} `json:"content"`
		}
		err = json.Unmarshal(capturedData.Payload, &actualRequest)
		require.NoError(t, err, "Failed to unmarshal the payload sent to the producer")

		// 2. Assert its contents
		assert.Equal(t, "New Message", actualRequest.Content.Title)
		assert.Equal(t, "You have received a new secure message.", actualRequest.Content.Body)
		assert.Equal(t, testTokens, actualRequest.Tokens)
	})

	t.Run("Success - No tokens skips publish", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		// Act
		err = notifier.Notify(ctx, []routing.DeviceToken{}, testEnvelope)

		// Assert
		require.NoError(t, err)
		producer.AssertNotCalled(t, "Publish")
	})

	t.Run("Failure - Producer returns error", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		testErr := errors.New("pubsub failed")
		producer.On("Publish", ctx, mock.Anything).Return("", testErr)

		// Act
		err = notifier.Notify(ctx, testTokens, testEnvelope)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), testErr.Error())
	})
}
