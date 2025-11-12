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

// PushNotifier defines the interface for notifying users of new messages,
// whether they are online ("poke") or offline ("push").
type PushNotifier interface {
	// NotifyOffline sends a rich push notification (with content) to a user's
	// registered devices. This is for the "cold" path.
	NotifyOffline(ctx context.Context, tokens []DeviceToken, envelope *secure.SecureEnvelope) error

	// PokeOnline sends a lightweight "poke" notification to a specific
	// user's active connection(s). This is for the "hot" path.
	PokeOnline(ctx context.Context, recipient urn.URN) error
}
