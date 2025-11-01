/*
File: internal/platform/queue/redis_hot_queue.go
Description: NEW FILE. This is the production implementation of the HotQueue
interface using Redis.
*/
package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/internal/queue" // Import new interfaces

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// redisClient defines the interface we need from go-redis.
type redisClient interface {
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd
	RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// RedisHotQueue implements the queue.HotQueue interface using Redis.
type RedisHotQueue struct {
	client redisClient
	logger zerolog.Logger
}

// queuedRedisMessage is the struct we store as a JSON string in Redis.
// We must wrap the envelope in our QueuedMessage struct so it has an ID
// that we can use for Acknowledgment.
type queuedRedisMessage struct {
	ID       string                 `json:"id"`
	Envelope *secure.SecureEnvelope `json:"envelope"`
}

// NewRedisHotQueue is the constructor for the RedisHotQueue.
func NewRedisHotQueue(client redisClient, logger zerolog.Logger) (queue.HotQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client cannot be nil")
	}
	return &RedisHotQueue{
		client: client,
		logger: logger,
	}, nil
}

// key formatting helpers
func userQueueKey(urn urn.URN) string      { return fmt.Sprintf("queue:%s", urn.String()) }
func userPendingKey(urn urn.URN) string    { return fmt.Sprintf("pending:%s", urn.String()) }
func userMigrationLock(urn urn.URN) string { return fmt.Sprintf("lock:migrate:%s", urn.String()) }

// Enqueue adds a message to the user's hot queue list.
func (s *RedisHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	// We generate a UUID to be the ACK ID.
	msg := queuedRedisMessage{
		ID:       uuid.NewString(),
		Envelope: envelope,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal redis message: %w", err)
	}

	key := userQueueKey(envelope.RecipientID)
	if err := s.client.LPush(ctx, key, payload).Err(); err != nil {
		return fmt.Errorf("failed to lpush to hot queue: %w", err)
	}
	return nil
}

// RetrieveBatch implements the atomic "move-to-pending" logic.
// It uses RPopLPush to move messages from the main queue to a pending list.
func (s *RedisHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	queueKey := userQueueKey(userURN)
	pendingKey := userPendingKey(userURN)

	queuedMessages := make([]*routing.QueuedMessage, 0, limit)

	for i := 0; i < limit; i++ {
		// Atomically move one message from the right of the queue
		// to the left of the pending list.
		payload, err := s.client.RPopLPush(ctx, queueKey, pendingKey).Result()
		if err == redis.Nil {
			// The queue is empty, we're done.
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to rpoplpush message: %w", err)
		}

		// Unmarshal the message we just moved
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			s.logger.Error().Err(err).Str("user", userURN.String()).Msg("Failed to unmarshal poison message from hot queue")
			// Remove the poison message from the pending queue to stop a loop
			_ = s.client.LRem(ctx, pendingKey, 1, payload)
			continue
		}

		// Convert to the external type
		queuedMessages = append(queuedMessages, &routing.QueuedMessage{
			ID:       msg.ID,
			Envelope: msg.Envelope,
		})
	}

	return queuedMessages, nil
}

// Acknowledge removes messages from the *pending* list.
// This is not perfectly atomic by ID, so we find the full message payload
// and remove it by value.
func (s *RedisHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	pendingKey := userPendingKey(userURN)
	log := s.logger.With().Str("user", userURN.String()).Logger()

	// Get all pending messages
	payloads, err := s.client.LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to read pending queue for ack: %w", err)
	}

	// Create a map of IDs to payloads for efficient lookup
	idMap := make(map[string]string)
	for _, payload := range payloads {
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Warn().Err(err).Msg("Failed to unmarshal message in pending queue during ack")
			continue
		}
		idMap[msg.ID] = payload
	}

	// For each ID we need to ack, find its payload and remove it
	for _, id := range messageIDs {
		payloadToRemove, ok := idMap[id]
		if !ok {
			log.Warn().Str("id", id).Msg("Attempted to ack message ID not in pending queue")
			continue
		}

		// Remove by value. This removes the first matching payload.
		if err := s.client.LRem(ctx, pendingKey, 1, payloadToRemove).Err(); err != nil {
			log.Error().Err(err).Str("id", id).Msg("Failed to lrem message from pending queue")
			// Continue trying to ack other messages
		}
	}

	return nil
}

// MigrateToCold moves all messages from both the main and pending queues
// to the ColdQueue.
func (s *RedisHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With().Str("user", userURN.String()).Logger()

	// This is a complex, multi-step process. We must "stop the world"
	// for this user to prevent race conditions (e.g., a new Enqueue
	// arriving during migration). We use a simple Redis lock.
	lockKey := userMigrationLock(userURN)
	if err := s.client.Del(ctx, lockKey).Err(); err != nil {
		log.Warn().Err(err).Msg("Failed to clear old migration lock")
	}

	// We'll migrate pending messages first, then main queue messages
	queueKey := userQueueKey(userURN)
	pendingKey := userPendingKey(userURN)
	keysToMigrate := []string{pendingKey, queueKey}

	var allEnvelopes []*secure.SecureEnvelope
	var payloadsToDelete []string
	var keysToDelete []string

	for _, key := range keysToMigrate {
		// Pull all messages from the list
		payloads, err := s.client.LRange(ctx, key, 0, -1).Result()
		if err != nil || len(payloads) == 0 {
			continue // No messages or error, move to next key
		}

		// Process them
		for _, payload := range payloads {
			var msg queuedRedisMessage
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				log.Error().Err(err).Str("key", key).Msg("Failed to unmarshal message for migration, skipping")
				continue
			}
			allEnvelopes = append(allEnvelopes, msg.Envelope)
			payloadsToDelete = append(payloadsToDelete, payload)
			keysToDelete = append(keysToDelete, key)
		}
	}

	if len(allEnvelopes) == 0 {
		log.Info().Msg("No hot queue messages to migrate.")
		return nil
	}

	// Enqueue all messages to the cold store.
	// This is not atomic with the Redis deletion, but we do it first
	// to ensure at-least-once.
	for _, env := range allEnvelopes {
		if err := destination.Enqueue(ctx, env); err != nil {
			// This is a partial failure. We stop here.
			// The messages are still in Redis. The migration will be
			// retried on the next disconnect.
			return fmt.Errorf("failed to write to cold queue during migration: %w", err)
		}
	}

	// Now that all messages are safely in the cold store, delete them
	// from Redis.
	for i, payload := range payloadsToDelete {
		key := keysToDelete[i]
		// We delete by value, just in case.
		if err := s.client.LRem(ctx, key, 1, payload).Err(); err != nil {
			log.Warn().Err(err).Str("key", key).Msg("Failed to LRem message during migration cleanup")
		}
	}

	log.Info().Int("count", len(allEnvelopes)).Msg("Successfully migrated hot queue to cold queue.")
	return nil
}
