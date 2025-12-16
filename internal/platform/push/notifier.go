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
	"github.com/tinywideclouds/go-platform/pkg/notification/v1"
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

	// 1. Create the DOMAIN Request (Clean Struct)
	request := &notification.NotificationRequest{
		RecipientID: envelope.RecipientID,
		Content: notification.NotificationContent{
			Title: "New Message",
			Body:  "You have received a new secure message.",
			Sound: "default",
		},
		DataPayload: map[string]string{
			"url":      "/messages/" + envelope.RecipientID.EntityID(),
			"msg_type": "secure_text",
		},
	}

	// 2. Marshal using STANDARD JSON
	// The Facade's MarshalJSON intercepts this and ensures Proto compliance.
	payloadBytes, err := json.Marshal(request)
	if err != nil {
		n.logger.Error("Failed to marshal notification request", "err", err)
		return fmt.Errorf("failed to marshal notification request: %w", err)
	}

	// 3. Publish
	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(),
		Payload: payloadBytes,
	}

	n.logger.Debug("Publishing notification request",
		"recipient", envelope.RecipientID.String(),
		"msg_id", messageData.ID,
	)

	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
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
