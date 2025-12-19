// --- File: internal/queue/message_queue.go ---
package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// CompositeMessageQueue manages both Hot and Cold queues.
type CompositeMessageQueue struct {
	hot  HotQueue
	cold ColdQueue
	log  *slog.Logger
}

func NewCompositeMessageQueue(hot HotQueue, cold ColdQueue, logger *slog.Logger) (MessageQueue, error) {
	if hot == nil || cold == nil {
		return nil, fmt.Errorf("hot and cold queues cannot be nil")
	}
	return &CompositeMessageQueue{
		hot:  hot,
		cold: cold,
		log:  logger.With("component", "MessageQueue"),
	}, nil
}

// EnqueueHot attempts to enqueue to the Hot Queue.
// If it fails, it falls back to the Cold Queue (unless the message is ephemeral).
// It strictly uses the provided messageID for both attempts.
func (c *CompositeMessageQueue) EnqueueHot(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	err := c.hot.Enqueue(ctx, messageID, envelope)
	if err == nil {
		return nil
	}

	c.log.Warn("Hot queue enqueue failed, attempting fallback to cold queue",
		"msg_id", messageID,
		"err", err,
	)

	// Fallback Logic
	if envelope.IsEphemeral {
		c.log.Warn("Dropping ephemeral message due to hot queue failure", "msg_id", messageID)
		return nil // Swallow error for ephemeral (fire-and-forget)
	}

	// Retry in Cold Queue using the SAME ID
	if err := c.cold.Enqueue(ctx, messageID, envelope); err != nil {
		c.log.Error("Cold queue fallback failed", "msg_id", messageID, "err", err)
		return fmt.Errorf("failed to enqueue to both hot and cold queues")
	}

	return nil
}

// EnqueueCold delegates to the ColdQueue, passing the ID.
func (c *CompositeMessageQueue) EnqueueCold(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	return c.cold.Enqueue(ctx, messageID, envelope)
}

func (c *CompositeMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	// 1. Try Hot Queue
	messages, err := c.hot.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		// Log error but attempt cold queue to ensure availability
		c.log.Error("Failed to retrieve from hot queue, falling back to cold only", "err", err)
		messages = []*routing.QueuedMessage{}
	}

	if len(messages) >= limit {
		return messages, nil
	}

	// 2. Fill remainder from Cold Queue
	remaining := limit - len(messages)
	coldMessages, err := c.cold.RetrieveBatch(ctx, userURN, remaining)
	if err != nil {
		c.log.Error("Failed to retrieve from cold queue", "err", err)
		// Return what we have from hot (partial success)
		return messages, nil
	}

	return append(messages, coldMessages...), nil
}

func (c *CompositeMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	// Acknowledge in BOTH queues.
	errHot := c.hot.Acknowledge(ctx, userURN, messageIDs)
	if errHot != nil {
		c.log.Warn("Failed to ack in hot queue", "err", errHot)
	}

	errCold := c.cold.Acknowledge(ctx, userURN, messageIDs)
	if errCold != nil {
		c.log.Warn("Failed to ack in cold queue", "err", errCold)
	}

	if errHot != nil && errCold != nil {
		return fmt.Errorf("failed to acknowledge in both queues")
	}

	return nil
}

func (c *CompositeMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	return c.hot.MigrateToCold(ctx, userURN, c.cold)
}
