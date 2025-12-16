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
	"google.golang.org/protobuf/encoding/protojson"

	// Generated Proto types for verification
	nv1 "github.com/tinywideclouds/gen-platform/go/types/notification/v1"

	// Platform types
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	// Code Under Test
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
)

// mockEventProducer mocks the EventProducer interface.
type mockEventProducer struct {
	mock.Mock
}

func (m *mockEventProducer) Publish(ctx context.Context, data messagepipeline.MessageData) (string, error) {
	args := m.Called(ctx, data)
	return args.String(0), args.Error(1)
}

// TestNotifyOffline validates that the notifier uses the Facade to produce strictly compliant ProtoJSON.
func TestNotifyOffline(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testURN, _ := urn.Parse("urn:contacts:user:test-user")

	testEnvelope := &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("this-should-not-be-in-the-push"),
	}

	t.Run("Success - Publishes strictly compliant ProtoJSON payload", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		var capturedData messagepipeline.MessageData

		// Capture the payload sent to the producer
		producer.On("Publish", ctx, mock.Anything).
			Run(func(args mock.Arguments) {
				capturedData = args.Get(1).(messagepipeline.MessageData)
			}).
			Return("mock-message-id", nil)

		// Act
		err = notifier.NotifyOffline(ctx, testEnvelope)

		// Assert
		require.NoError(t, err)
		producer.AssertCalled(t, "Publish", ctx, mock.Anything)

		// Verification:
		// We verify the Facade Pattern worked by attempting to unmarshal the JSON
		// back into the GENERATED PROTO STRUCT.
		// If the JSON keys were wrong (e.g. "RecipientID" instead of "recipientId"), this would fail.
		var actualPb nv1.NotificationRequestPb
		err = protojson.Unmarshal(capturedData.Payload, &actualPb)
		require.NoError(t, err, "Payload produced by Notifier should be valid ProtoJSON")

		// Check Data Integrity
		assert.Equal(t, testURN.String(), actualPb.GetRecipientId())
		assert.Equal(t, "New Message", actualPb.GetContent().GetTitle())
		assert.Equal(t, "default", actualPb.GetContent().GetSound())
	})

	t.Run("Failure - Producer returns error", func(t *testing.T) {
		// Arrange
		producer := new(mockEventProducer)
		notifier, err := push.NewPubSubNotifier(producer, logger)
		require.NoError(t, err)

		testErr := errors.New("pubsub connection failed")
		producer.On("Publish", ctx, mock.Anything).Return("", testErr)

		// Act
		err = notifier.NotifyOffline(ctx, testEnvelope)

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
		err = notifier.NotifyOffline(ctx, nil)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "envelope cannot be nil")
		producer.AssertNotCalled(t, "Publish")
	})
}

// TestPokeOnline tests the "poke" signal which uses internal JSON (no Proto contract required yet).
func TestPokeOnline(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testURN, _ := urn.Parse("urn:contacts:user:test-user")

	t.Run("Success - Publishes poke payload", func(t *testing.T) {
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

		// Verify manual JSON structure
		var actualPoke struct {
			Type      string `json:"type"`
			Recipient string `json:"recipient"`
		}
		err = json.Unmarshal(capturedData.Payload, &actualPoke)
		require.NoError(t, err)

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
