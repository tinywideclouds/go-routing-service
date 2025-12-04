// --- File: internal/platform/queue/hot_queue_redis_test.go ---
//go:build integration

package queue_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time" // Added time import

	"github.com/illmade-knight/go-test/emulators" // Import your emulator pkg
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
)

// redisTestFixture holds resources for testing the redis hot queue.
type redisTestFixture struct {
	ctx       context.Context // This will be the timed test context
	rdb       *redis.Client
	hotQueue  queue.HotQueue
	coldQueue queue.ColdQueue // Mocked cold queue
}

// mockColdQueue is a simple mock for migration testing.
type mockColdQueue struct {
	enqueued []*secure.SecureEnvelope
}

func (m *mockColdQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	m.enqueued = append(m.enqueued, envelope)
	return nil
}
func (m *mockColdQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	return nil, nil // Not needed
}
func (m *mockColdQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return nil // Not needed
}

// setupRedisSuite initializes the emulator and a real Redis client.
func setupRedisSuite(t *testing.T) (context.Context, *redisTestFixture) {
	t.Helper()
	// This context is for test operations (e.g., Enqueue, Retrieve)
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	// 1. Start Redis Emulator
	// --- FIX: Pass context.Background() for container lifecycle ---
	cfg := emulators.GetDefaultRedisImageContainer()
	connInfo := emulators.SetupRedisContainer(t, context.Background(), cfg)

	// 2. Create Real Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr: connInfo.EmulatorAddress,
		DB:   0,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	err := rdb.FlushDB(testCtx).Err() // Use testCtx for setup operations
	require.NoError(t, err)

	// 3. Create RedisHotQueue
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hotQueue, err := fsqueue.NewRedisHotQueue(rdb, logger)
	require.NoError(t, err)

	return testCtx, &redisTestFixture{
		ctx:       testCtx, // Store the timed context
		rdb:       rdb,
		hotQueue:  hotQueue,
		coldQueue: &mockColdQueue{},
	}
}

func TestRedisHotEnqueueRetrieveAcknowledge(t *testing.T) {
	ctx, fixture := setupRedisSuite(t) // ctx is the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-user-1")
	queueKey := "queue:" + recipientURN.String()
	pendingKey := "pending:" + recipientURN.String()

	msg1 := baseEnvelope(recipientURN, "redis-1")
	msg2 := baseEnvelope(recipientURN, "redis-2")

	// --- 1. Enqueue ---
	err := fixture.hotQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)

	// Assert queue state (LIFO, so msg2 is at index 0)
	qLen, err := fixture.rdb.LLen(ctx, queueKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), qLen)

	pLen, err := fixture.rdb.LLen(ctx, pendingKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pLen)

	// --- 2. Retrieve Batch 1 (limit 1) ---
	// RetrieveBatch uses RPop, so it gets the *oldest* message (msg1)
	batch1, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch1, 1)
	assert.Equal(t, msg1.EncryptedData, batch1[0].Envelope.EncryptedData)

	// Assert state: msg1 moved from queue to pending
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(1), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen)

	// --- 3. Retrieve Batch 2 (Batch 1 is pending) ---
	batch2, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch2, 1)
	assert.Equal(t, msg2.EncryptedData, batch2[0].Envelope.EncryptedData)

	// Assert state: msg2 also moved to pending
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(0), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(2), pLen)

	// --- 4. Retrieve Batch 3 (All pending) ---
	batch3, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch3, 0, "Should be no messages left to retrieve")

	// --- 5. Acknowledge Batch 1 ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch1[0].ID})
	require.NoError(t, err)

	// Assert state: msg1 removed from pending
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen)

	// --- 6. Acknowledge Batch 2 ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch2[0].ID})
	require.NoError(t, err)

	// Assert state: all queues empty
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(0), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(0), pLen)
}

func TestRedisMigrateToCold(t *testing.T) {
	ctx, fixture := setupRedisSuite(t) // ctx is the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-migrate-user")
	queueKey := "queue:" + recipientURN.String()
	pendingKey := "pending:" + recipientURN.String()

	// 1. Seed Hot Queues
	msg1_pending := baseEnvelope(recipientURN, "migrate-1-pending")
	msg2_queue := baseEnvelope(recipientURN, "migrate-2-queue")

	// Enqueue msg1 and retrieve it to put it in pending
	err := fixture.hotQueue.Enqueue(ctx, msg1_pending)
	require.NoError(t, err)
	_, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)

	// Enqueue msg2 and leave it in the main queue
	err = fixture.hotQueue.Enqueue(ctx, msg2_queue)
	require.NoError(t, err)

	// Assert initial state
	qLen, _ := fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(1), qLen)
	pLen, _ := fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen)

	// 2. Act
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// 3. Assert Hot Queues are Empty
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(0), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(0), pLen)

	// 4. Assert Cold Queue (mock) has messages
	mockCold := fixture.coldQueue.(*mockColdQueue)
	require.Len(t, mockCold.enqueued, 2)

	var data1, data2 bool
	for _, env := range mockCold.enqueued {
		if string(env.EncryptedData) == "data-migrate-1-pending" {
			data1 = true
		}
		if string(env.EncryptedData) == "data-migrate-2-queue" {
			data2 = true
		}
	}
	assert.True(t, data1, "Pending message was not migrated")
	assert.True(t, data2, "Queued message was not migrated")
}

// TestRedisMigrateToCold_EphemeralDrops verifies that ephemeral messages
// are deleted from Redis but NOT written to the cold queue during migration.
func TestRedisMigrateToCold_EphemeralDrops(t *testing.T) {
	ctx, fixture := setupRedisSuite(t) // ctx is the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-ephemeral-user")
	queueKey := "queue:" + recipientURN.String()
	pendingKey := "pending:" + recipientURN.String()

	// 1. Prepare Messages
	msgPersistent1 := baseEnvelope(recipientURN, "persistent-1")
	msgPersistent2 := baseEnvelope(recipientURN, "persistent-2")
	msgEphemeral := baseEnvelope(recipientURN, "ephemeral-1")
	msgEphemeral.IsEphemeral = true // FLAG SET

	// 2. Seed Queues
	// Put Persistent 1 in PENDING (retrieve it)
	err := fixture.hotQueue.Enqueue(ctx, msgPersistent1)
	require.NoError(t, err)
	_, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)

	// Put Persistent 2 and Ephemeral in MAIN QUEUE
	err = fixture.hotQueue.Enqueue(ctx, msgPersistent2)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, msgEphemeral)
	require.NoError(t, err)

	// Assert initial state
	qLen, _ := fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(2), qLen) // 2 in queue
	pLen, _ := fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen) // 1 in pending

	// 3. Act: Migrate
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// 4. Assert Hot Queues are Empty (Everything cleaned up)
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(0), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(0), pLen)

	// 5. Assert Cold Queue (mock) has ONLY Persistent messages
	mockCold := fixture.coldQueue.(*mockColdQueue)
	require.Len(t, mockCold.enqueued, 2)

	var dataP1, dataP2, dataE bool
	for _, env := range mockCold.enqueued {
		if string(env.EncryptedData) == "data-persistent-1" {
			dataP1 = true
		}
		if string(env.EncryptedData) == "data-persistent-2" {
			dataP2 = true
		}
		if string(env.EncryptedData) == "data-ephemeral-1" {
			dataE = true
		}
	}
	assert.True(t, dataP1, "Persistent message 1 missing from cold queue")
	assert.True(t, dataP2, "Persistent message 2 missing from cold queue")
	assert.False(t, dataE, "Ephemeral message was incorrectly migrated to cold queue")
}
