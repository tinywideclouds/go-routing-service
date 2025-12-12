// --- File: internal/platform/push/notifier.go ---
package push

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// EventProducer defines the interface for publishing a message.
type EventProducer interface {
	Publish(ctx context.Context, data messagepipeline.MessageData) (string, error)
}

// PubSubNotifier implements the routing.PushNotifier interface.
type PubSubNotifier struct {
	producer EventProducer
	logger   *slog.Logger
}

// notificationRequest matches the new contract expected by Notification Service.
// REFACTORED: Includes RecipientID, removed Tokens.
type notificationRequest struct {
	RecipientID string              `json:"recipientId"`
	Content     notificationContent `json:"content"`
}

type notificationContent struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Sound string `json:"sound"`
}

type pokeRequest struct {
	Type      string `json:"type"`
	Recipient string `json:"recipient"`
}

func NewPubSubNotifier(producer EventProducer, logger *slog.Logger) (*PubSubNotifier, error) {
	if producer == nil {
		return nil, fmt.Errorf("producer cannot be nil")
	}
	return &PubSubNotifier{
		producer: producer,
		logger:   logger.With("component", "PubSubNotifier"),
	}, nil
}

// NotifyOffline sends a command to the Notification Service.
// REFACTORED: No longer handles tokens.
func (n *PubSubNotifier) NotifyOffline(ctx context.Context, envelope *secure.SecureEnvelope) error {
	if envelope == nil {
		return fmt.Errorf("NotifyOffline failed: envelope cannot be nil")
	}

	log := n.logger.With("recipient", envelope.RecipientID.String())

	// 1. Create the request
	request := &notificationRequest{
		RecipientID: envelope.RecipientID.String(),
		Content: notificationContent{
			Title: "New Message",
			Body:  "You have received a new secure message.",
			Sound: "default",
		},
	}

	// 2. Marshal
	payloadBytes, err := json.Marshal(request)
	if err != nil {
		log.Error("Failed to marshal notification request", "err", err)
		return fmt.Errorf("failed to marshal notification request: %w", err)
	}

	// 3. Publish
	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(),
		Payload: payloadBytes,
	}

	log.Debug("Publishing notification request", "msg_id", messageData.ID)
	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
		log.Error("Failed to publish notification request", "err", err)
		return fmt.Errorf("failed to publish notification request: %w", err)
	}

	return nil
}

func (n *PubSubNotifier) PokeOnline(ctx context.Context, recipient urn.URN) error {
	log := n.logger.With("recipient", recipient.String())
	request := pokeRequest{
		Type:      "poke",
		Recipient: recipient.String(),
	}

	payloadBytes, err := json.Marshal(request)
	if err != nil {
		return err
	}

	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(),
		Payload: payloadBytes,
	}

	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
		log.Error("Failed to publish poke request", "err", err)
		return err
	}
	return nil
}
