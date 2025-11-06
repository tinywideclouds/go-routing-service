/*
File: internal/platform/push/notifier.go
Description: REFACTORED to be testable by depending on an
'EventProducer' interface instead of a concrete producer.
*/
// Package push contains the concrete implementation for the PushNotifier interface.
package push

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog" // IMPORTED

	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	// "github.com/rs/zerolog" // REMOVED
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform 'secure' package
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// NEW: This interface matches the 'Publish' method on
// 'messagepipeline.GooglePubsubProducer' and 'delivery.go'.
// This makes the Notifier unit-testable.
type EventProducer interface {
	Publish(ctx context.Context, data messagepipeline.MessageData) (string, error)
}

// PubSubNotifier is a production-ready implementation of routing.PushNotifier.
// It sends push notification requests to a dedicated Pub/Sub topic for the
// notification-service to consume.
type PubSubNotifier struct {
	// REFACTORED: Now uses the testable interface.
	producer EventProducer
	logger   *slog.Logger // CHANGED
}

// This is the new, simple, "dumb" notification contract.
type notificationRequest struct {
	Tokens  []routing.DeviceToken `json:"tokens"`
	Content notificationContent   `json:"content"`
}

// Generic content for the push notification.
type notificationContent struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Sound string `json:"sound"`
}

// NewPubSubNotifier creates a new push notification publisher.
// REFACTORED: Signature now accepts the interface.
func NewPubSubNotifier(producer EventProducer, logger *slog.Logger) (*PubSubNotifier, error) { // CHANGED
	if producer == nil {
		return nil, fmt.Errorf("producer cannot be nil")
	}

	notifier := &PubSubNotifier{
		producer: producer,
		logger:   logger.With("component", "PubSubNotifier"), // CHANGED
	}

	return notifier, nil
}

// Notify implements the routing.PushNotifier interface. It transforms the internal
// routing data into the public NotificationRequest contract and publishes it.
// REFACTORED: Signature now accepts *secure.SecureEnvelope
func (n *PubSubNotifier) Notify(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	log := n.logger.With("recipient", envelope.RecipientID.String()) // ADDED

	if len(tokens) == 0 {
		log.Debug("No device tokens provided, skipping push notification.") // ADDED
		return nil                                                          // Nothing to do
	}

	// 1. Create the new "dumb" notification request.
	request := createNotificationRequest(tokens)

	// 2. Marshal it using standard JSON.
	payloadBytes, err := json.Marshal(request)
	if err != nil {
		log.Error("Failed to marshal new notification request", "err", err) // ADDED
		return fmt.Errorf("failed to marshal new notification request: %w", err)
	}

	// 3. Create the dataflow message
	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(), // Generate a new UUID for this Pub/Sub message.
		Payload: payloadBytes,
	}

	// 4. Publish the message.
	log.Debug("Publishing push notification request", "token_count", len(tokens), "msg_id", messageData.ID) // ADDED
	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
		log.Error("Failed to publish push notification request", "err", err, "msg_id", messageData.ID) // ADDED
		return fmt.Errorf("failed to publish push notification request: %w", err)
	}

	n.logger.Info("Push notification request published successfully", "token_count", len(tokens)) // CHANGED
	return nil
}

// createNotificationRequest is a helper to build the public notification contract.
// REFACTORED: This is now "dumb" and only includes generic content.
func createNotificationRequest(tokens []routing.DeviceToken) *notificationRequest {
	return &notificationRequest{
		Tokens: tokens,
		Content: notificationContent{
			Title: "New Message",
			Body:  "You have received a new secure message.",
			Sound: "default",
		},
	}
}
