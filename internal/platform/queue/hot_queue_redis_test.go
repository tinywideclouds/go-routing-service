// --- File: internal/platform/queue/hot_queue_redis_test.go ---
//go:build integration

package queue_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
)

type redisTestFixture struct {
	ctx       context.Context
	rdb       *redis.Client
	hotQueue  queue.HotQueue
	coldQueue queue.ColdQueue // Mocked cold queue
}

// Mock must store ID now
type mockColdQueue struct {
	enqueuedIDs []string
	enqueued    []*secure.SecureEnvelope
}

func (m *mockColdQueue) Enqueue(ctx context.Context, id string, envelope *secure.SecureEnvelope) error {
	m.enqueuedIDs = append(m.enqueuedIDs, id)
	m.enqueued = append(m.enqueued, envelope)
	return nil
}
func (m *mockColdQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	return nil, nil
}
func (m *mockColdQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return nil
}

func setupRedisSuite(t *testing.T) (context.Context, *redisTestFixture) {
	t.Helper()
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	cfg := emulators.GetDefaultRedisImageContainer()
	connInfo := emulators.SetupRedisContainer(t, context.Background(), cfg)

	rdb := redis.NewClient(&redis.Options{
		Addr: connInfo.EmulatorAddress,
		DB:   0,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	err := rdb.FlushDB(testCtx).Err()
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hotQueue, err := fsqueue.NewRedisHotQueue(rdb, logger)
	require.NoError(t, err)

	return testCtx, &redisTestFixture{
		ctx:       testCtx,
		rdb:       rdb,
		hotQueue:  hotQueue,
		coldQueue: &mockColdQueue{},
	}
}

func TestRedisHotEnqueueRetrieveAcknowledge(t *testing.T) {
	ctx, fixture := setupRedisSuite(t)

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-user-1")
	queueKey := "queue:" + recipientURN.String()

	msg1 := baseEnvelope(recipientURN, "redis-1")
	id1 := uuid.NewString()
	msg2 := baseEnvelope(recipientURN, "redis-2")
	id2 := uuid.NewString()

	// --- 1. Enqueue with IDs ---
	err := fixture.hotQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, id2, msg2)
	require.NoError(t, err)

	qLen, err := fixture.rdb.LLen(ctx, queueKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), qLen)

	// --- 2. Retrieve Batch 1 ---
	batch1, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch1, 1)
	assert.Equal(t, id1, batch1[0].ID)

	// --- 3. Retrieve Batch 2 ---
	batch2, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch2, 1)
	assert.Equal(t, id2, batch2[0].ID)

	// --- 4. Acknowledge ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch1[0].ID})
	require.NoError(t, err)
}

func TestRedisMigrateToCold(t *testing.T) {
	ctx, fixture := setupRedisSuite(t)

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-migrate-user")

	msg1 := baseEnvelope(recipientURN, "migrate-1")
	id1 := uuid.NewString()

	// Enqueue and retrieve to move to pending
	err := fixture.hotQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)
	_, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)

	// Act
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// Assert
	mockCold := fixture.coldQueue.(*mockColdQueue)
	require.Len(t, mockCold.enqueuedIDs, 1)
	assert.Equal(t, id1, mockCold.enqueuedIDs[0])
}
