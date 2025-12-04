// --- File: internal/platform/queue/hot_queue_redis.go ---
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

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
// It uses two lists per user:
//  1. `queue:{urn}`: The main ingestion queue (LPush/RPop).
//  2. `pending:{urn}`: A temporary holding list for messages that have been
//     retrieved by a client but not yet acknowledged (LPush/LRem).
type RedisHotQueue struct {
	client redisClient
	logger *slog.Logger
}

// queuedRedisMessage is the struct we store as a JSON string in Redis.
// It wraps the envelope with a unique ID for acknowledgment.
type queuedRedisMessage struct {
	ID       string                 `json:"id"`
	Envelope *secure.SecureEnvelope `json:"envelope"`
}

// NewRedisHotQueue is the constructor for the RedisHotQueue.
func NewRedisHotQueue(client redisClient, logger *slog.Logger) (queue.HotQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client cannot be nil")
	}
	return &RedisHotQueue{
		client: client,
		logger: logger.With("component", "redis_hot_queue"),
	}, nil
}

// Enqueue adds a message to the left side (head) of the user's main queue list.
func (s *RedisHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	log := s.logger.With("user", envelope.RecipientID.String())

	// We generate a UUID to be the ACK ID.
	msg := queuedRedisMessage{
		ID:       uuid.NewString(),
		Envelope: envelope,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		log.Error("Failed to marshal redis message", "err", err)
		return fmt.Errorf("failed to marshal redis message: %w", err)
	}

	key := userQueueKey(envelope.RecipientID)
	log.Debug("Enqueuing message to hot queue", "key", key, "msg_id", msg.ID)

	if err := s.client.LPush(ctx, key, payload).Err(); err != nil {
		log.Error("Failed to lpush to hot queue", "key", key, "err", err)
		return fmt.Errorf("failed to lpush to hot queue: %w", err)
	}
	return nil
}

// RetrieveBatch atomically moves messages from the main queue to the pending queue.
// It uses RPopLPush to take items from the *right* (oldest) of the main queue
// and place them on the *left* (newest) of the pending queue.
func (s *RedisHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	log := s.logger.With("user", userURN.String())
	queueKey := userQueueKey(userURN)
	pendingKey := userPendingKey(userURN)

	queuedMessages := make([]*routing.QueuedMessage, 0, limit)

	log.Debug("Retrieving hot message batch", "limit", limit, "queue_key", queueKey, "pending_key", pendingKey)

	for i := 0; i < limit; i++ {
		// Atomically move one message from the right of the queue
		// to the left of the pending list.
		payload, err := s.client.RPopLPush(ctx, queueKey, pendingKey).Result()
		if err == redis.Nil {
			// The queue is empty, we're done.
			if i == 0 {
				log.Debug("Hot queue is empty")
			}
			break
		}
		if err != nil {
			log.Error("Failed to rpoplpush message", "err", err)
			return nil, fmt.Errorf("failed to rpoplpush message: %w", err)
		}

		// Unmarshal the message we just moved
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Error("Failed to unmarshal poison message from hot queue", "err", err)

			// Remove the poison message from the pending queue to stop a loop
			log.Warn("Removing poison message from pending queue", "key", pendingKey)
			_ = s.client.LRem(ctx, pendingKey, 1, payload)
			continue
		}

		// Convert to the external type
		queuedMessages = append(queuedMessages, &routing.QueuedMessage{
			ID:       msg.ID,
			Envelope: msg.Envelope,
		})
	}

	if len(queuedMessages) > 0 {
		log.Debug("Retrieved and moved hot message batch to pending", "count", len(queuedMessages))
	}
	return queuedMessages, nil
}

// Acknowledge removes messages from the *pending* list by their ID.
// It does this by fetching all pending messages, finding the matching payload
// for the ID, and then removing that payload *by value* from the list.
func (s *RedisHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	pendingKey := userPendingKey(userURN)
	log := s.logger.With("user", userURN.String(), "pending_key", pendingKey)

	// Get all pending messages
	log.Debug("Ack: Fetching all pending messages to find by ID", "count", len(messageIDs))
	payloads, err := s.client.LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		log.Error("Failed to read pending queue for ack", "err", err)
		return fmt.Errorf("failed to read pending queue for ack: %w", err)
	}

	// Create a map of IDs to payloads for efficient lookup
	idMap := make(map[string]string)
	for _, payload := range payloads {
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Warn("Failed to unmarshal message in pending queue during ack", "err", err)
			continue
		}
		idMap[msg.ID] = payload
	}

	// For each ID we need to ack, find its payload and remove it
	var ackCount int
	for _, id := range messageIDs {
		payloadToRemove, ok := idMap[id]
		if !ok {
			log.Warn("Attempted to ack message ID not in pending queue", "id", id)
			continue
		}

		// Remove by value. This removes the first matching payload.
		if err := s.client.LRem(ctx, pendingKey, 1, payloadToRemove).Err(); err != nil {
			log.Error("Failed to lrem message from pending queue", "err", err, "id", id)
			// Continue trying to ack other messages
		} else {
			ackCount++
		}
	}

	log.Info("Successfully acknowledged (deleted) hot pending messages", "count", ackCount)
	return nil
}

// MigrateToCold moves all messages from both the main and pending queues
// to the ColdQueue.
func (s *RedisHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With("user", userURN.String())
	log.Info("Starting hot-to-cold migration")

	queueKey := userQueueKey(userURN)
	pendingKey := userPendingKey(userURN)
	keysToMigrate := []string{pendingKey, queueKey}

	var messagesToPersist []*secure.SecureEnvelope
	var payloadsToDelete []string
	var keysToDelete []string

	for _, key := range keysToMigrate {
		log.Debug("Scanning key for migration", "key", key)

		payloads, err := s.client.LRange(ctx, key, 0, -1).Result()
		if err != nil || len(payloads) == 0 {
			log.Debug("No messages in key or error, skipping", "key", key, "err", err)
			continue
		}

		log.Debug("Found messages in key", "key", key, "count", len(payloads))

		for _, payload := range payloads {
			var msg queuedRedisMessage
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				log.Error("Failed to unmarshal message for migration, skipping", "err", err, "key", key)
				continue
			}

			// We ALWAYS mark for deletion from Redis (cleanup).
			payloadsToDelete = append(payloadsToDelete, payload)
			keysToDelete = append(keysToDelete, key)

			// We ONLY migrate if it's NOT ephemeral.
			if !msg.Envelope.IsEphemeral {
				messagesToPersist = append(messagesToPersist, msg.Envelope)
			} else {
				log.Debug("Dropping ephemeral message during migration", "msg_id", msg.ID)
			}
		}
	}

	// 1. Write persistent messages to Cold Queue
	if len(messagesToPersist) > 0 {
		log.Debug("Writing messages to cold queue", "count", len(messagesToPersist))
		for _, env := range messagesToPersist {
			if err := destination.Enqueue(ctx, env); err != nil {
				log.Error("Failed to write to cold queue during migration. Aborting.", "err", err)
				return fmt.Errorf("failed to write to cold queue during migration: %w", err)
			}
		}
	} else {
		log.Info("No persistent messages to migrate.")
	}

	// 2. Delete ALL messages (including ephemeral) from Hot Queue
	log.Debug("Cleaning up Redis...", "count", len(payloadsToDelete))
	for i, payload := range payloadsToDelete {
		key := keysToDelete[i]
		if err := s.client.LRem(ctx, key, 1, payload).Err(); err != nil {
			log.Warn("Failed to LRem message during migration cleanup", "err", err, "key", key)
		}
	}

	log.Info("Successfully migrated hot queue to cold queue", "persisted", len(messagesToPersist), "cleaned", len(payloadsToDelete))
	return nil
}

// --- Private Helpers ---

// key formatting helpers
func userQueueKey(urn urn.URN) string   { return fmt.Sprintf("queue:%s", urn.String()) }
func userPendingKey(urn urn.URN) string { return fmt.Sprintf("pending:%s", urn.String()) }
