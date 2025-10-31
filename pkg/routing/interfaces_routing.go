/*
File: pkg/routing/interfaces_routing.go
Description: REFACTORED to define the new "Reliable Dumb Queue" contracts
and use the new platform-level 'secure.SecureEnvelope' type.
*/
package routing

import (
	"context"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// IngestionProducer defines the interface for publishing a message into the pipeline.
type IngestionProducer interface {
	Publish(ctx context.Context, envelope *secure.SecureEnvelope) error
}

// DeliveryProducer defines the interface for publishing a message to a specific delivery topic.
type DeliveryProducer interface {
	Publish(ctx context.Context, topicID string, envelope *secure.SecureEnvelope) error
}

// PushNotifier defines the interface for sending push notifications.
type PushNotifier interface {
	Notify(ctx context.Context, tokens []DeviceToken, envelope *secure.SecureEnvelope) error
}

// MessageStore defines the interface for persisting and retrieving messages.
// REFACTORED: This interface now models a "Reliable Dumb Queue"
// with a "Paginate-Save-Ack-Delete" flow.
type MessageStore interface {
	// StoreMessages saves a slice of message envelopes for a specific recipient.
	// REFACTORED: Swapped types and modified signature to be more generic.
	StoreMessages(ctx context.Context, recipient urn.URN, envelopes []*secure.SecureEnvelope) error

	// RetrieveMessageBatch fetches the next available batch of queued messages
	// for a user, ordered by when they were queued (oldest first).
	RetrieveMessageBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error)

	// AcknowledgeMessages permanently deletes a list of messages by their MessageIDs
	// after the client has confirmed persistent local storage.
	AcknowledgeMessages(ctx context.Context, userURN urn.URN, messageIDs []string) error
}
