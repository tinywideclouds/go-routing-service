// --- File: internal/platform/push/notifier_test.go ---
package push_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
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

// TestNotifyOffline tests the "rich push" path
func TestNotifyOffline(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testURN, _ := urn.Parse("urn:sm:user:test-user")

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

		var capturedData messagepipeline.MessageData
		producer.On("Publish", ctx, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedData = args.Get(1).(messagepipeline.MessageData)
			}).
			Return("mock-message-id", nil)

		// Act
		err = notifier.NotifyOffline(ctx, testTokens, testEnvelope)

		// Assert
		require.NoError(t, err)
		producer.AssertCalled(t, "Publish", ctx, mock.Anything)

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
		err = notifier.NotifyOffline(ctx, []routing.DeviceToken{}, testEnvelope)

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
		err = notifier.NotifyOffline(ctx, testTokens, testEnvelope)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), testErr.Error())
	})

	t.Run("Failure - Nil envelope returns error", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		// Act
		err = notifier.NotifyOffline(ctx, testTokens, nil) // Pass nil envelope

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "envelope cannot be nil")
		producer.AssertNotCalled(t, "Publish")
	})
}

// TestPokeOnline tests the "poke" path
func TestPokeOnline(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testURN, _ := urn.Parse("urn:sm:user:test-user")

	t.Run("Success - Publishes a 'poke' payload", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		var capturedData messagepipeline.MessageData
		producer.On("Publish", ctx, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedData = args.Get(1).(messagepipeline.MessageData)
			}).
			Return("mock-poke-id", nil)

		// Act
		err = notifier.PokeOnline(ctx, testURN)

		// Assert
		require.NoError(t, err)
		producer.AssertCalled(t, "Publish", ctx, mock.Anything)

		// 1. Unmarshal the poke payload
		var actualPoke struct {
			Type      string `json:"type"`
			Recipient string `json:"recipient"`
		}
		err = json.Unmarshal(capturedData.Payload, &actualPoke)
		require.NoError(t, err, "Failed to unmarshal the poke payload")

		// 2. Assert its contents
		assert.Equal(t, "poke", actualPoke.Type)
		assert.Equal(t, testURN.String(), actualPoke.Recipient)
	})

	t.Run("Failure - Producer returns error", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		testErr := errors.New("pubsub failed")
		producer.On("Publish", ctx, mock.Anything).Return("", testErr)

		// Act
		err = notifier.PokeOnline(ctx, testURN)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), testErr.Error())
	})
}
