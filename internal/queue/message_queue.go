/*
File: internal/queue/message_queue.go
Description: NEW FILE. Defines the high-level composite MessageQueue
interface and its concrete implementation.
*/
package queue

import (
	"context"
	"fmt"
	"log/slog" // IMPORTED

	// "github.com/rs/zerolog" // REMOVED
	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// CompositeMessageQueue is the concrete implementation of MessageQueue.
// It orchestrates the hot and cold queues.
type CompositeMessageQueue struct {
	hot    HotQueue
	cold   ColdQueue
	logger *slog.Logger // CHANGED
}

// NewCompositeMessageQueue creates a new composite queue.
func NewCompositeMessageQueue(hot HotQueue, cold ColdQueue, logger *slog.Logger) (MessageQueue, error) { // CHANGED
	if hot == nil {
		return nil, fmt.Errorf("hot queue cannot be nil")
	}
	if cold == nil {
		return nil, fmt.Errorf("cold queue cannot be nil")
	}
	return &CompositeMessageQueue{
		hot:    hot,
		cold:   cold,
		logger: logger.With("component", "composite_queue"), // CHANGED
	}, nil
}

// EnqueueHot attempts to use the hot queue, but falls back to cold on error.
// This guarantees at-least-once delivery even if Redis is down.
func (c *CompositeMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	log := c.logger.With("user", envelope.RecipientID.String()) // ADDED

	if err := c.hot.Enqueue(ctx, envelope); err != nil {
		log.Error("Hot queue enqueue failed. Falling back to cold queue.", "err", err) // CHANGED

		// Fallback to cold queue
		if errCold := c.cold.Enqueue(ctx, envelope); errCold != nil {
			log.Error("FATAL: Hot and Cold queue enqueue failed.", "err_cold", errCold, "err_hot", err) // CHANGED
			return errCold
		}
		log.Warn("Message enqueued to cold queue as hot-fallback.") // ADDED
	}
	return nil
}

// EnqueueCold writes directly to the cold queue.
func (c *CompositeMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	// The cold queue implementation is responsible for its own logging.
	return c.cold.Enqueue(ctx, envelope)
}

// RetrieveBatch checks hot, then cold.
func (c *CompositeMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	log := c.logger.With("user", userURN.String()) // ADDED

	// 1. Try the hot queue first.
	log.Debug("Retrieving batch from hot queue...") // ADDED
	hotMessages, err := c.hot.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve from hot queue. Falling back to cold.", "err", err) // CHANGED
		// Don't return the error, just fall back to cold.
	}

	// If we got messages from the hot queue, return them immediately.
	if len(hotMessages) > 0 {
		log.Debug("Retrieved batch from hot queue", "count", len(hotMessages)) // ADDED
		return hotMessages, nil
	}

	// 2. Hot queue was empty (or failed), so try the cold queue.
	log.Debug("Hot queue empty, retrieving batch from cold queue...") // ADDED
	coldMessages, err := c.cold.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve from cold queue", "err", err) // ADDED
		return nil, err                                             // Return the cold queue error
	}

	if len(coldMessages) > 0 { // ADDED
		log.Debug("Retrieved batch from cold queue", "count", len(coldMessages))
	} else {
		log.Debug("No messages found in hot or cold queues")
	}

	return coldMessages, nil
}

// Acknowledge must be sent to *both* queues.
// The queues are responsible for handling non-existent IDs.
func (c *CompositeMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	log := c.logger.With("user", userURN.String()) // ADDED

	if len(messageIDs) == 0 { // ADDED
		log.Debug("Acknowledge called with no message IDs, skipping.")
		return nil
	}

	log.Debug("Acknowledging messages in parallel", "count", len(messageIDs)) // ADDED

	// We run them in parallel.
	errChan := make(chan error, 2)

	go func() {
		err := c.hot.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			log.Error("Hot queue acknowledge failed", "err", err) // CHANGED
		}
		errChan <- err
	}()

	go func() {
		err := c.cold.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			log.Error("Cold queue acknowledge failed", "err", err) // CHANGED
		}
		errChan <- err
	}()

	// Wait for both to finish. Return the first error we find.
	err1 := <-errChan
	err2 := <-errChan

	log.Debug("Acknowledge complete", "hot_err", err1, "cold_err", err2) // ADDED

	if err1 != nil {
		return err1
	}
	return err2
}

// MigrateHotToCold triggers the hot queue's migration logic.
func (c *CompositeMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	log := c.logger.With("user", userURN.String()) // ADDED
	log.Info("Triggering hot-to-cold migration")   // ADDED

	err := c.hot.MigrateToCold(ctx, userURN, c.cold)
	if err != nil { // ADDED
		log.Error("Hot-to-cold migration failed", "err", err)
		return err
	}

	log.Info("Hot-to-cold migration finished successfully") // ADDED
	return nil
}
