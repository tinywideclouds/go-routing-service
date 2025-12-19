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
	"github.com/google/uuid"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

type emulatorTestFixture struct {
	ctx                context.Context
	rdb                *redis.Client
	fsClient           *firestore.Client
	hotQueue           queue.HotQueue
	coldQueue          queue.ColdQueue
	coldCollectionName string
}

func setupEmulatorSuite(t *testing.T) *emulatorTestFixture {
	t.Helper()
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	const projectID = "test-project-emulator"
	const coldCollectionName = "emulator-test-cold-queue"

	redisCfg := emulators.GetDefaultRedisImageContainer()
	redisConnInfo := emulators.SetupRedisContainer(t, context.Background(), redisCfg)
	rdb := redis.NewClient(&redis.Options{
		Addr: redisConnInfo.EmulatorAddress,
		DB:   0,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	err := rdb.FlushDB(testCtx).Err()
	require.NoError(t, err)

	fsEmulator := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(context.Background(), projectID, fsEmulator.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hotQueue, err := fsqueue.NewRedisHotQueue(rdb, logger)
	require.NoError(t, err)

	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollectionName, logger)
	require.NoError(t, err)

	return &emulatorTestFixture{
		ctx:                testCtx,
		rdb:                rdb,
		fsClient:           fsClient,
		hotQueue:           hotQueue,
		coldQueue:          coldQueue,
		coldCollectionName: coldCollectionName,
	}
}

func emulatorBaseEnvelope(recipient urn.URN, data string) *secure.SecureEnvelope {
	return &secure.SecureEnvelope{
		RecipientID:   recipient,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

func emulatorDocExists(t *testing.T, ctx context.Context, docRef *firestore.DocumentRef) bool {
	t.Helper()
	_, err := docRef.Get(ctx)
	if err == nil {
		return true
	}
	if status.Code(err) == codes.NotFound {
		return false
	}
	require.NoError(t, err, "docExists helper failed")
	return false
}

func TestEmulator_RedisRetrieveAndAck(t *testing.T) {
	fixture := setupEmulatorSuite(t)
	ctx := fixture.ctx

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-user-1")

	msg1 := emulatorBaseEnvelope(recipientURN, "redis-1")
	id1 := uuid.NewString()
	msg2 := emulatorBaseEnvelope(recipientURN, "redis-2")
	id2 := uuid.NewString()

	// --- 1. Enqueue with IDs ---
	err := fixture.hotQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, id2, msg2)
	require.NoError(t, err)

	// --- 2. Retrieve Batch 1 ---
	batch1, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch1, 1)
	assert.Equal(t, id1, batch1[0].ID)

	// --- 3. Acknowledge ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{batch1[0].ID})
	require.NoError(t, err)
}

func TestEmulator_RedisMigrateToCold(t *testing.T) {
	fixture := setupEmulatorSuite(t)
	ctx := fixture.ctx

	recipientURN, _ := urn.Parse("urn:contacts:user:redis-migrate-user")

	msg1_pending := emulatorBaseEnvelope(recipientURN, "migrate-1-pending")
	id1 := uuid.NewString()

	err := fixture.hotQueue.Enqueue(ctx, id1, msg1_pending)
	require.NoError(t, err)
	_, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)

	// 2. Act
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// 4. Assert Cold Queue
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(coldBatch) == 1
	}, 10*time.Second, 200*time.Millisecond)

	assert.Equal(t, id1, coldBatch[0].ID, "Pending message ID was not migrated")
}
