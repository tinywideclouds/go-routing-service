/*
File: internal/queue/message_queue.go
Description: NEW FILE. Defines the high-level composite MessageQueue
interface and its concrete implementation.
*/
package queue

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
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
	logger zerolog.Logger
}

// NewCompositeMessageQueue creates a new composite queue.
func NewCompositeMessageQueue(hot HotQueue, cold ColdQueue, logger zerolog.Logger) (MessageQueue, error) {
	if hot == nil {
		return nil, fmt.Errorf("hot queue cannot be nil")
	}
	if cold == nil {
		return nil, fmt.Errorf("cold queue cannot be nil")
	}
	return &CompositeMessageQueue{
		hot:    hot,
		cold:   cold,
		logger: logger,
	}, nil
}

// EnqueueHot attempts to use the hot queue, but falls back to cold on error.
// This guarantees at-least-once delivery even if Redis is down.
func (c *CompositeMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	if err := c.hot.Enqueue(ctx, envelope); err != nil {
		c.logger.Error().Err(err).Str("user", envelope.RecipientID.String()).
			Msg("Hot queue enqueue failed. Falling back to cold queue.")

		// Fallback to cold queue
		if errCold := c.cold.Enqueue(ctx, envelope); errCold != nil {
			c.logger.Error().Err(errCold).Str("user", envelope.RecipientID.String()).
				Msg("FATAL: Hot and Cold queue enqueue failed.")
			return errCold
		}
	}
	return nil
}

// EnqueueCold writes directly to the cold queue.
func (c *CompositeMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return c.cold.Enqueue(ctx, envelope)
}

// RetrieveBatch checks hot, then cold.
func (c *CompositeMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	// 1. Try the hot queue first.
	hotMessages, err := c.hot.RetrieveBatch(ctx, userURN, limit)
	if err != nil {
		c.logger.Error().Err(err).Str("user", userURN.String()).
			Msg("Failed to retrieve from hot queue. Falling back to cold.")
		// Don't return the error, just fall back to cold.
	}

	// If we got messages from the hot queue, return them immediately.
	if len(hotMessages) > 0 {
		return hotMessages, nil
	}

	// 2. Hot queue was empty (or failed), so try the cold queue.
	return c.cold.RetrieveBatch(ctx, userURN, limit)
}

// Acknowledge must be sent to *both* queues.
// The queues are responsible for handling non-existent IDs.
func (c *CompositeMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	// We run them in parallel.
	errChan := make(chan error, 2)

	go func() {
		err := c.hot.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			c.logger.Error().Err(err).Str("user", userURN.String()).Msg("Hot queue acknowledge failed")
		}
		errChan <- err
	}()

	go func() {
		err := c.cold.Acknowledge(ctx, userURN, messageIDs)
		if err != nil {
			c.logger.Error().Err(err).Str("user", userURN.String()).Msg("Cold queue acknowledge failed")
		}
		errChan <- err
	}()

	// Wait for both to finish. Return the first error we find.
	err1 := <-errChan
	err2 := <-errChan

	if err1 != nil {
		return err1
	}
	return err2
}

// MigrateHotToCold triggers the hot queue's migration logic.
func (c *CompositeMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	return c.hot.MigrateToCold(ctx, userURN, c.cold)
}
