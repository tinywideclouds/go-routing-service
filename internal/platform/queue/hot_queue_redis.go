// --- File: internal/platform/queue/hot_queue_redis.go ---
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

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
// It uses THREE lists per user to implement Priority Bands:
//  1. `queue:{urn}:high`: The Express Lane (Sync, Revoke). Checked FIRST.
//  2. `queue:{urn}`:      The Standard Lane (Chat, Typing). Checked SECOND.
//  3. `pending:{urn}`:    The In-Flight list. Shared destination for Ack tracking.
type RedisHotQueue struct {
	client redisClient
	logger *slog.Logger
}

// queuedRedisMessage is the struct we store as a JSON string in Redis.
type queuedRedisMessage struct {
	ID       string                 `json:"id"`
	Envelope *secure.SecureEnvelope `json:"envelope"`
	QueuedAt time.Time              `json:"queued_at"` // Added for observability/TTL potential
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

// Enqueue adds a message to the appropriate priority band.
func (s *RedisHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	log := s.logger.With("user", envelope.RecipientID.String(), "priority", envelope.Priority)

	msg := queuedRedisMessage{
		ID:       uuid.NewString(),
		Envelope: envelope,
		QueuedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		log.Error("Failed to marshal redis message", "err", err)
		return fmt.Errorf("failed to marshal redis message: %w", err)
	}

	// 1. Determine Target Lane based on Priority
	var key string
	if envelope.Priority >= 5 {
		key = userHighQueueKey(envelope.RecipientID) // Express Lane
		log.Info("Enqueuing to HIGH priority band", "key", key)
	} else {
		key = userQueueKey(envelope.RecipientID) // Standard Lane
		log.Debug("Enqueuing to STANDARD priority band", "key", key)
	}

	if err := s.client.LPush(ctx, key, payload).Err(); err != nil {
		log.Error("Failed to lpush to hot queue", "key", key, "err", err)
		return fmt.Errorf("failed to lpush to hot queue: %w", err)
	}
	return nil
}

// RetrieveBatch drains the High Priority band FIRST, then the Standard band.
func (s *RedisHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	log := s.logger.With("user", userURN.String())

	highKey := userHighQueueKey(userURN)
	stdKey := userQueueKey(userURN)
	pendingKey := userPendingKey(userURN)

	queuedMessages := make([]*routing.QueuedMessage, 0, limit)

	log.Debug("Retrieving hot message batch", "limit", limit)

	for i := 0; i < limit; i++ {
		// 1. Priority Fetch Strategy
		// Try to pop from High Priority first.
		payload, err := s.client.RPopLPush(ctx, highKey, pendingKey).Result()

		if err == redis.Nil {
			// High Priority is empty. Fallback to Standard.
			payload, err = s.client.RPopLPush(ctx, stdKey, pendingKey).Result()
		}

		// 2. Check Result (from whichever queue we tried)
		if err == redis.Nil {
			// Both queues are empty. We are done.
			break
		}
		if err != nil {
			log.Error("Failed to rpoplpush message", "err", err)
			return nil, fmt.Errorf("failed to rpoplpush message: %w", err)
		}

		// 3. Unmarshal
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err != nil {
			log.Error("Failed to unmarshal poison message from hot queue", "err", err)
			// Cleanup poison pill
			_ = s.client.LRem(ctx, pendingKey, 1, payload)
			continue
		}

		// 4. Append
		queuedMessages = append(queuedMessages, &routing.QueuedMessage{
			ID:       msg.ID,
			Envelope: msg.Envelope,
		})
	}

	if len(queuedMessages) > 0 {
		log.Debug("Retrieved hot message batch", "count", len(queuedMessages))
	}
	return queuedMessages, nil
}

// Acknowledge removes messages from the pending list.
func (s *RedisHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	pendingKey := userPendingKey(userURN)
	log := s.logger.With("user", userURN.String(), "pending_key", pendingKey)

	// Fetch all pending messages to match IDs to Payloads
	payloads, err := s.client.LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		log.Error("Failed to read pending queue for ack", "err", err)
		return fmt.Errorf("failed to read pending queue for ack: %w", err)
	}

	idMap := make(map[string]string)
	for _, payload := range payloads {
		var msg queuedRedisMessage
		if err := json.Unmarshal([]byte(payload), &msg); err == nil {
			idMap[msg.ID] = payload
		}
	}

	var ackCount int
	for _, id := range messageIDs {
		if payloadToRemove, ok := idMap[id]; ok {
			if err := s.client.LRem(ctx, pendingKey, 1, payloadToRemove).Err(); err == nil {
				ackCount++
			}
		}
	}

	log.Info("Successfully acknowledged hot pending messages", "count", ackCount)
	return nil
}

// MigrateToCold moves all messages from High, Standard, and Pending queues to ColdQueue.
func (s *RedisHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With("user", userURN.String())
	log.Info("Starting hot-to-cold migration")

	// We must now scan THREE lists
	keysToMigrate := []string{
		userPendingKey(userURN),   // 1. Pending (In-flight)
		userHighQueueKey(userURN), // 2. High Priority (Unsent)
		userQueueKey(userURN),     // 3. Standard (Unsent)
	}

	var messagesToPersist []*secure.SecureEnvelope
	var payloadsToDelete []string
	var keysToDelete []string

	for _, key := range keysToMigrate {
		payloads, err := s.client.LRange(ctx, key, 0, -1).Result()
		if err != nil || len(payloads) == 0 {
			continue
		}

		for _, payload := range payloads {
			var msg queuedRedisMessage
			if err := json.Unmarshal([]byte(payload), &msg); err != nil {
				continue
			}

			// Mark for deletion from Redis
			payloadsToDelete = append(payloadsToDelete, payload)
			keysToDelete = append(keysToDelete, key)

			// Migration Logic: Persist if NOT ephemeral
			if !msg.Envelope.IsEphemeral {
				messagesToPersist = append(messagesToPersist, msg.Envelope)
			}
		}
	}

	// 1. Write persistent messages to Cold Queue
	if len(messagesToPersist) > 0 {
		for _, env := range messagesToPersist {
			if err := destination.Enqueue(ctx, env); err != nil {
				return fmt.Errorf("failed to write to cold queue: %w", err)
			}
		}
	}

	// 2. Cleanup Redis
	for i, payload := range payloadsToDelete {
		_ = s.client.LRem(ctx, keysToDelete[i], 1, payload).Err()
	}

	log.Info("Migration complete", "persisted", len(messagesToPersist), "cleaned", len(payloadsToDelete))
	return nil
}

// --- Private Helpers ---

func userQueueKey(urn urn.URN) string     { return fmt.Sprintf("queue:%s", urn.String()) }
func userHighQueueKey(urn urn.URN) string { return fmt.Sprintf("queue:%s:high", urn.String()) }
func userPendingKey(urn urn.URN) string   { return fmt.Sprintf("pending:%s", urn.String()) }
