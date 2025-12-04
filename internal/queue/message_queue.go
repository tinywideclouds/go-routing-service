// --- File: internal/queue/message_queue.go ---
package queue

import (
	"context"
	"fmt"
	"log/slog"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// CompositeMessageQueue is the concrete implementation of MessageQueue.
// It orchestrates a HotQueue and a ColdQueue to provide a single, unified
// queuing interface to the application.
type CompositeMessageQueue struct {
	hot    HotQueue
	cold   ColdQueue
	logger *slog.Logger
}

// NewCompositeMessageQueue creates a new composite queue.
// It requires non-nil hot and cold queue implementations.
func NewCompositeMessageQueue(hot HotQueue, cold ColdQueue, logger *slog.Logger) (MessageQueue, error) {
	if hot == nil {
		return nil, fmt.Errorf("hot queue cannot be nil")
	}
	if cold == nil {
		return nil, fmt.Errorf("cold queue cannot be nil")
	}
	return &CompositeMessageQueue{
		hot:    hot,
		cold:   cold,
		logger: logger.With("component", "composite_queue"),
	}, nil
}

// EnqueueHot attempts to use the hot queue, but falls back to cold on error.
// This guarantees at-least-once delivery even if the hot queue (e.g., Redis) is down.
func (c *CompositeMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	log := c.logger.With("user", envelope.RecipientID.String())

	if err := c.hot.Enqueue(ctx, envelope); err != nil {
		log.Error("Hot queue enqueue failed. Falling back to cold queue.", "err", err)

		if envelope.IsEphemeral {
			log.Info("Dropping ephemeral message during hot queue failure (fallback skipped).")
			return nil // Treat as success (handled)
		}
		// Fallback to cold queue
		if errCold := c.cold.Enqueue(ctx, envelope); errCold != nil {
			log.Error("FATAL: Hot and Cold queue enqueue failed.", "err_cold", errCold, "err_hot", err)
			return errCold
		}
		log.Warn("Message enqueued to cold queue as hot-fallback.")
	}
	return nil
}

// EnqueueCold writes directly to the cold queue.
func (c *CompositeMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	// The cold queue implementation is responsible for its own logging.
	return c.cold.Enqueue(ctx, envelope)
}

// RetrieveBatch checks the hot queue first. If it is empty or fails,
// it falls back to checking the cold queue.
func (c *CompositeMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	log := c.logger.With("user", userURN.String())

	// 1. Try the hot queue first.
	log.Debug("Retrieving batch from hot queue...")
	hotMessages, err := c.hot.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve from hot queue. Falling back to cold.", "err", err)
		// Don't return the error, just fall back to cold.
	}

	// If we got messages from the hot queue, return them immediately.
	if len(hotMessages) > 0 {
		log.Debug("Retrieved batch from hot queue", "count", len(hotMessages))
		return hotMessages, nil
	}

	// 2. Hot queue was empty (or failed), so try the cold queue.
	log.Debug("Hot queue empty, retrieving batch from cold queue...")
	coldMessages, err := c.cold.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve from cold queue", "err", err)
		return nil, err // Return the cold queue error
	}

	if len(coldMessages) > 0 {
		log.Debug("Retrieved batch from cold queue", "count", len(coldMessages))
	} else {
		log.Debug("No messages found in hot or cold queues")
	}

	return coldMessages, nil
}

// Acknowledge sends the acknowledgment to *both* queues in parallel.
// The underlying hot/cold implementations are responsible for handling
// non-existent IDs gracefully (e.g., Redis LRem returning 0).
func (c *CompositeMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	log := c.logger.With("user", userURN.String())

	if len(messageIDs) == 0 {
		log.Debug("Acknowledge called with no message IDs, skipping.")
		return nil
	}

	log.Debug("Acknowledging messages in parallel", "count", len(messageIDs))

	// We run them in parallel.
	errChan := make(chan error, 2)

	go func() {
		err := c.hot.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			log.Error("Hot queue acknowledge failed", "err", err)
		}
		errChan <- err
	}()

	go func() {
		err := c.cold.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			log.Error("Cold queue acknowledge failed", "err", err)
		}
		errChan <- err
	}()

	// Wait for both to finish. Return the first error we find.
	err1 := <-errChan
	err2 := <-errChan

	log.Debug("Acknowledge complete", "hot_err", err1, "cold_err", err2)

	if err1 != nil {
		return err1
	}
	return err2
}

// MigrateHotToCold triggers the hot queue's migration logic, passing it
// a reference to the cold queue as the destination.
func (c *CompositeMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	log := c.logger.With("user", userURN.String())
	log.Info("Triggering hot-to-cold migration")

	err := c.hot.MigrateToCold(ctx, userURN, c.cold)
	if err != nil {
		log.Error("Hot-to-cold migration failed", "err", err)
		return err
	}

	log.Info("Hot-to-cold migration finished successfully")
	return nil
}
