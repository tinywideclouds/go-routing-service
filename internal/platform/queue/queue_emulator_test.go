// --- File: internal/platform/queue/queue_emulator_test.go ---
//go:build integration

package queue_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// emulatorTestFixture holds all resources for the combined test
type emulatorTestFixture struct {
	ctx                context.Context // This will be the timed test context
	rdb                *redis.Client
	fsClient           *firestore.Client
	hotQueue           queue.HotQueue
	coldQueue          queue.ColdQueue
	coldCollectionName string
}

// setupEmulatorSuite initializes BOTH emulators
func setupEmulatorSuite(t *testing.T) *emulatorTestFixture {
	t.Helper()
	// This context is for test operations (e.g., Enqueue, Retrieve)
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Longer timeout for two emulators
	t.Cleanup(cancel)

	const projectID = "test-project-emulator"
	const coldCollectionName = "emulator-test-cold-queue"

	// --- FIX: Pass context.Background() for container lifecycle ---
	// 1. Start Redis Emulator
	redisCfg := emulators.GetDefaultRedisImageContainer()
	redisConnInfo := emulators.SetupRedisContainer(t, context.Background(), redisCfg)
	rdb := redis.NewClient(&redis.Options{
		Addr: redisConnInfo.EmulatorAddress,
		DB:   0,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	err := rdb.FlushDB(testCtx).Err() // Use testCtx for setup operations
	require.NoError(t, err)

	// 2. Start Firestore Emulator
	fsEmulator := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))

	// Create client with context.Background() so it's not tied to testCtx
	fsClient, err := firestore.NewClient(context.Background(), projectID, fsEmulator.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	// 3. Create Queues
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hotQueue, err := fsqueue.NewRedisHotQueue(rdb, logger)
	require.NoError(t, err)

	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollectionName, logger)
	require.NoError(t, err)

	return &emulatorTestFixture{
		ctx:                testCtx, // Store the timed context
		rdb:                rdb,
		fsClient:           fsClient,
		hotQueue:           hotQueue,
		coldQueue:          coldQueue,
		coldCollectionName: coldCollectionName,
	}
}

// emulatorBaseEnvelope is a self-contained test helper.
func emulatorBaseEnvelope(recipient urn.URN, data string) *secure.SecureEnvelope {
	return &secure.SecureEnvelope{
		RecipientID:   recipient,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

// emulatorDocExists is a self-contained test helper.
func emulatorDocExists(t *testing.T, ctx context.Context, docRef *firestore.DocumentRef) bool {
	t.Helper()
	_, err := docRef.Get(ctx)
	if err == nil {
		return true // Exists
	}
	if status.Code(err) == codes.NotFound {
		return false // Does not exist
	}
	require.NoError(t, err, "docExists helper failed")
	return false
}

// TestEmulator_RedisRetrieveAndAck validates the normal Redis flow
func TestEmulator_RedisRetrieveAndAck(t *testing.T) {
	fixture := setupEmulatorSuite(t)
	ctx := fixture.ctx // Use the timed test context from the fixture

	recipientURN, _ := urn.Parse("urn:sm:user:redis-user-1")
	queueKey := "queue:" + recipientURN.String()
	pendingKey := "pending:" + recipientURN.String()

	msg1 := emulatorBaseEnvelope(recipientURN, "redis-1")
	msg2 := emulatorBaseEnvelope(recipientURN, "redis-2")

	// --- 1. Enqueue ---
	err := fixture.hotQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)

	// --- 2. Retrieve Batch 1 (limit 1) ---
	batch1, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch1, 1)
	assert.Equal(t, msg1.EncryptedData, batch1[0].Envelope.EncryptedData)

	// Assert state: msg1 moved from queue to pending
	qLen, _ := fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(1), qLen)
	pLen, _ := fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen)

	// --- 3. Retrieve Batch 2 (Batch 1 is pending) ---
	batch2, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch2, 1)
	assert.Equal(t, msg2.EncryptedData, batch2[0].Envelope.EncryptedData)

	// --- 4. Acknowledge Batch 1 ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch1[0].ID})
	require.NoError(t, err)

	// Assert state: msg1 removed from pending
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(1), pLen)

	// --- 5. Acknowledge Batch 2 ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch2[0].ID})
	require.NoError(t, err)

	// Assert state: all queues empty
	qLen, _ = fixture.rdb.LLen(ctx, queueKey).Result()
	assert.Equal(t, int64(0), qLen)
	pLen, _ = fixture.rdb.LLen(ctx, pendingKey).Result()
	assert.Equal(t, int64(0), pLen)
}

// TestEmulator_RedisMigrateToCold validates the fallback flow
func TestEmulator_RedisMigrateToCold(t *testing.T) {
	fixture := setupEmulatorSuite(t)
	ctx := fixture.ctx // Use the timed test context from the fixture

	recipientURN, _ := urn.Parse("urn:sm:user:redis-migrate-user")
	queueKey := "queue:" + recipientURN.String()
	pendingKey := "pending:" + recipientURN.String()
	coldCollectionRef := fixture.fsClient.Collection(fixture.coldCollectionName).Doc(recipientURN.String()).Collection("messages")

	// 1. Seed Hot Queues
	msg1_pending := emulatorBaseEnvelope(recipientURN, "migrate-1-pending")
	msg2_queue := emulatorBaseEnvelope(recipientURN, "migrate-2-queue")

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

	// 4. Assert Cold Queue (Firestore) has messages
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		// Use the real cold queue's RetrieveBatch
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(coldBatch) == 2
	}, 10*time.Second, 200*time.Millisecond, "Messages did not appear in cold queue")

	// Verify content
	var data1, data2 bool
	for _, msg := range coldBatch {
		if string(msg.Envelope.EncryptedData) == "data-migrate-1-pending" {
			data1 = true
		}
		if string(msg.Envelope.EncryptedData) == "data-migrate-2-queue" {
			data2 = true
		}
	}
	assert.True(t, data1, "Pending message was not migrated")
	assert.True(t, data2, "Queued message was not migrated")

	// 5. Cleanup (Skipped for emulator, but we can check one doc)
	t.Log("NOTE: Deletion verification is skipped for emulator test.")
	docRef := coldCollectionRef.Doc(coldBatch[0].ID)
	assert.True(t, emulatorDocExists(t, ctx, docRef), "Cold queue message does not exist")
}
