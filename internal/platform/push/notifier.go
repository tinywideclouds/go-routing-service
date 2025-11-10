/*
File: internal/platform/push/notifier.go
Description: REFACTORED to implement the explicit
NotifyOffline and PokeOnline methods.
*/
// Package push contains the concrete implementation for the PushNotifier interface.
package push

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

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

// notificationRequest is the "rich push" contract for offline devices.
type notificationRequest struct {
	Tokens  []routing.DeviceToken `json:"tokens"`
	Content notificationContent   `json:"content"`
}

// notificationContent is the generic content for a rich push.
type notificationContent struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Sound string `json:"sound"`
}

// --- (NEW) pokeRequest is the "poke" contract for online clients ---
type pokeRequest struct {
	Type      string `json:"type"`
	Recipient string `json:"recipient"`
}

// NewPubSubNotifier creates a new push notification publisher.
func NewPubSubNotifier(producer EventProducer, logger *slog.Logger) (*PubSubNotifier, error) {
	if producer == nil {
		return nil, fmt.Errorf("producer cannot be nil")
	}

	notifier := &PubSubNotifier{
		producer: producer,
		logger:   logger.With("component", "PubSubNotifier"),
	}

	return notifier, nil
}

// --- (REFACTORED) NotifyOffline implements the "rich push" path ---
func (n *PubSubNotifier) NotifyOffline(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	// --- (NEW) Guard clause for nil envelope ---
	if envelope == nil {
		return fmt.Errorf("NotifyOffline failed: envelope cannot be nil")
	}

	log := n.logger.With("recipient", envelope.RecipientID.String())

	if len(tokens) == 0 {
		log.Debug("No device tokens provided, skipping push notification.")
		return nil // Nothing to do
	}

	// 1. Create the "rich push" notification request.
	request := createNotificationRequest(tokens)

	// 2. Marshal it using standard JSON.
	payloadBytes, err := json.Marshal(request)
	if err != nil {
		log.Error("Failed to marshal new notification request", "err", err)
		return fmt.Errorf("failed to marshal new notification request: %w", err)
	}

	// 3. Create the dataflow message
	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(), // Generate a new UUID for this Pub/Sub message.
		Payload: payloadBytes,
	}

	// 4. Publish the message.
	log.Debug("Publishing push notification request", "token_count", len(tokens), "msg_id", messageData.ID)
	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
		log.Error("Failed to publish push notification request", "err", err, "msg_id", messageData.ID)
		return fmt.Errorf("failed to publish push notification request: %w", err)
	}

	n.logger.Info("Push notification request published successfully", "token_count", len(tokens))
	return nil
}

// --- (NEW) PokeOnline implements the "poke" path ---
func (n *PubSubNotifier) PokeOnline(ctx context.Context, recipient urn.URN) error {
	log := n.logger.With("recipient", recipient.String())

	// 1. Create the lightweight "poke" payload
	request := pokeRequest{
		Type:      "poke",
		Recipient: recipient.String(),
	}

	// 2. Marshal it
	payloadBytes, err := json.Marshal(request)
	if err != nil {
		log.Error("Failed to marshal poke request", "err", err)
		return fmt.Errorf("failed to marshal poke request: %w", err)
	}

	// 3. Create the dataflow message
	messageData := messagepipeline.MessageData{
		ID:      uuid.NewString(),
		Payload: payloadBytes,
	}

	// 4. Publish the message
	log.Debug("Publishing 'poke' request", "msg_id", messageData.ID)
	_, err = n.producer.Publish(ctx, messageData)
	if err != nil {
		log.Error("Failed to publish poke request", "err", err, "msg_id", messageData.ID)
		return fmt.Errorf("failed to publish poke request: %w", err)
	}

	n.logger.Info("'Poke' request published successfully")
	return nil
}

// createNotificationRequest is a helper to build the public notification contract.
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
