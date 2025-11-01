// Package queue defines the interfaces for the message queuing system.
package queue

import (
	"context"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// Queue is the base interface for a user-specific message queue.
type Queue interface {
	// Enqueue adds a message to the queue for a specific recipient.
	Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error

	// RetrieveBatch fetches the next available batch of queued messages
	// for a user, ordered by when they were queued (oldest first).
	RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error)

	// Acknowledge permanently deletes a list of messages by their MessageIDs
	// after the client has confirmed persistent local storage.
	Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error
}

// ColdQueue is a durable, long-term implementation of a Queue.
type ColdQueue interface {
	Queue
}

// HotQueue is a transient, high-speed implementation of a Queue
// that adds the ability to migrate its contents to a ColdQueue.
type HotQueue interface {
	Queue

	// MigrateToCold moves all messages for a user from this HotQueue
	// to the provided ColdQueue destination.
	MigrateToCold(ctx context.Context, userURN urn.URN, destination ColdQueue) error
}

// MessageQueue is the high-level, unified interface for interacting
// with the hot/cold queuing system.
type MessageQueue interface {
	// EnqueueHot attempts to enqueue a message to the fast, transient "hot" queue.
	// If the hot queue fails, it MUST fall back to the cold queue.
	EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error

	// EnqueueCold enqueues a message directly to the durable "cold" queue.
	EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error

	// RetrieveBatch fetches the next batch of messages, checking the hot
	// queue first, then falling back to the cold queue if hot is empty.
	RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error)

	// Acknowledge acknowledges a message from *whichever* queue it came from.
	Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error

	// MigrateHotToCold triggers the migration of a user's entire hot queue
	// (both main and pending) to the cold queue.
	MigrateHotToCold(ctx context.Context, userURN urn.URN) error
}
