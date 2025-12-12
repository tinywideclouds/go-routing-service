// --- File: pkg/routing/interfaces_routing.go ---
package routing

import (
	"context"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// IngestionProducer defines the interface for publishing a message into the pipeline.
type IngestionProducer interface {
	// Publish accepts an envelope and sends it to the ingestion topic for processing.
	Publish(ctx context.Context, envelope *secure.SecureEnvelope) error
}

// PushNotifier defines the interface for notifying users of new messages.
// REFACTORED: No longer takes a list of tokens. It just takes the envelope (which contains the Recipient ID).
type PushNotifier interface {
	// NotifyOffline sends a command to the Notification Service to notify the user.
	NotifyOffline(ctx context.Context, envelope *secure.SecureEnvelope) error

	// PokeOnline sends a lightweight "poke" notification to a specific
	// user's active connection(s). This is for the "hot" path.
	PokeOnline(ctx context.Context, recipient urn.URN) error
}
