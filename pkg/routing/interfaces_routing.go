/*
File: pkg/routing/interfaces_routing.go
Description: REFACTORED to remove the old 'MessageStore' and
'DeliveryProducer' interfaces, which are now replaced by
the 'queue.MessageQueue' contract.
*/
package routing

import (
	"context"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// IngestionProducer defines the interface for publishing a message into the pipeline.
type IngestionProducer interface {
	Publish(ctx context.Context, envelope *secure.SecureEnvelope) error
}

// DELETED: DeliveryProducer interface
// type DeliveryProducer interface { ... }

// PushNotifier defines the interface for sending push notifications.
type PushNotifier interface {
	Notify(ctx context.Context, tokens []DeviceToken, envelope *secure.SecureEnvelope) error
}

// DELETED: MessageStore interface
// This is now replaced by 'internal/queue.MessageQueue'
// type MessageStore interface { ... }
