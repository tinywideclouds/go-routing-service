// --- File: internal/platform/queue/hot_queue_firestore_test.go ---
//go:build integration

package queue_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
)

// hotTestFixture holds resources for testing the hot queue.
type hotTestFixture struct {
	ctx                   context.Context // This will be the timed test context
	fsClient              *firestore.Client
	hotQueue              queue.HotQueue
	coldQueue             queue.ColdQueue // A real cold queue for migration
	mainCollectionName    string
	pendingCollectionName string
	coldCollectionName    string
}

func docExistsHot(t *testing.T, ctx context.Context, docRef *firestore.DocumentRef) bool {
	t.Helper()
	_, err := docRef.Get(ctx)
	if err == nil {
		return true // Exists
	}
	if status.Code(err) == codes.NotFound {
		return false // Does not exist
	}
	require.NoError(t, err, "docExistsHot helper failed")
	return false
}

// setupHotSuite initializes the emulator and BOTH queue types for hot queue tests.
func setupHotSuite(t *testing.T) *hotTestFixture {
	t.Helper()
	testCtx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	t.Cleanup(cancel)

	const projectID = "test-project-hotqueue"
	const mainCollectionName = "emulator-hot-main"
	const pendingCollectionName = "emulator-hot-pending"
	const coldCollectionName = "emulator-cold-for-hot-test"

	firestoreEmulator := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))

	fsClient, err := firestore.NewClient(context.Background(), projectID, firestoreEmulator.ClientOptions...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = fsClient.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hotQueue, err := fsqueue.NewFirestoreHotQueue(fsClient, mainCollectionName, pendingCollectionName, logger)
	require.NoError(t, err)

	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollectionName, logger)
	require.NoError(t, err)

	return &hotTestFixture{
		ctx:                   testCtx,
		fsClient:              fsClient,
		hotQueue:              hotQueue,
		coldQueue:             coldQueue,
		mainCollectionName:    mainCollectionName,
		pendingCollectionName: pendingCollectionName,
		coldCollectionName:    coldCollectionName,
	}
}

func TestHotEnqueueRetrieveAcknowledge(t *testing.T) {
	fixture := setupHotSuite(t)
	ctx := fixture.ctx

	recipientURN, _ := urn.Parse("urn:contacts:user:hot-queue-user")
	mainCollectionRef := fixture.fsClient.Collection(fixture.mainCollectionName).Doc(recipientURN.String()).Collection("messages")
	pendingCollectionRef := fixture.fsClient.Collection(fixture.pendingCollectionName).Doc(recipientURN.String()).Collection("messages")

	msg1 := baseEnvelope(recipientURN, "hot-1")
	msg2 := baseEnvelope(recipientURN, "hot-2")
	id1 := uuid.NewString()
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
	assert.Equal(t, id1, batch1[0].ID, "Should retrieve id1 first")
	assert.Equal(t, msg1.EncryptedData, batch1[0].Envelope.EncryptedData)

	// Verify move to pending
	doc1MainRef := mainCollectionRef.Doc(id1)
	doc1PendingRef := pendingCollectionRef.Doc(id1)

	require.Eventually(t, func() bool {
		return !docExistsHot(t, ctx, doc1MainRef) // We expect it to NOT exist
	}, 10*time.Second, 100*time.Millisecond, "Message was not deleted from main queue")

	require.Eventually(t, func() bool {
		return docExistsHot(t, ctx, doc1PendingRef) // We expect it to EXIST
	}, 10*time.Second, 100*time.Millisecond, "Message was not created in pending queue")

	// --- 3. Retrieve Batch 2 ---
	batch2, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch2, 1)
	assert.Equal(t, id2, batch2[0].ID, "Should retrieve id2 second")

	// --- 4. Acknowledge Batch 1 ---
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{id1})
	require.NoError(t, err)

	// require.Eventually(t, func() bool {
	// 	return !docExistsHot(t, ctx, doc1PendingRef)
	// }, 10*time.Second, 100*time.Millisecond, "Acked message should be deleted from pending")
}

func TestMigrateToCold(t *testing.T) {
	fixture := setupHotSuite(t)
	ctx := fixture.ctx

	recipientURN, _ := urn.Parse("urn:contacts:user:hot-migrate-user")
	pendingCollectionRef := fixture.fsClient.Collection(fixture.pendingCollectionName).Doc(recipientURN.String()).Collection("messages")

	// --- 1. Seed Hot Queue ---
	msg1 := baseEnvelope(recipientURN, "migrate-1") // Will be moved to pending
	msg2 := baseEnvelope(recipientURN, "migrate-2") // Will stay in main
	id1 := uuid.NewString()
	id2 := uuid.NewString()

	err := fixture.hotQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, id2, msg2)
	require.NoError(t, err)

	// Move msg1 to pending
	require.Eventually(t, func() bool {
		batch, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
		require.NoError(t, err)
		if len(batch) == 1 && batch[0].ID == id1 {
			// Trigger lazy loading on doc if needed? No, just verifying existence
			_ = pendingCollectionRef.Doc(id1)
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Failed to retrieve message to seed pending queue")

	// --- 2. Act ---
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// --- 3. Assert Cold Queue has messages WITH CORRECT IDs ---
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(coldBatch) == 2
	}, 10*time.Second, 200*time.Millisecond, "Messages did not appear in cold queue")

	// Verify IDs were preserved
	var found1, found2 bool
	for _, msg := range coldBatch {
		if msg.ID == id1 {
			found1 = true
			assert.Equal(t, "data-migrate-1", string(msg.Envelope.EncryptedData))
		}
		if msg.ID == id2 {
			found2 = true
			assert.Equal(t, "data-migrate-2", string(msg.Envelope.EncryptedData))
		}
	}
	assert.True(t, found1, "Message 1 (from pending) ID lost or not migrated")
	assert.True(t, found2, "Message 2 (from main) ID lost or not migrated")
}

func TestMigrateToCold_EphemeralDrops(t *testing.T) {
	fixture := setupHotSuite(t)
	ctx := fixture.ctx

	recipientURN, _ := urn.Parse("urn:contacts:user:ephemeral-test-user")

	msgPersistent := baseEnvelope(recipientURN, "persistent-1")
	idPersistent := uuid.NewString()

	msgEphemeral := baseEnvelope(recipientURN, "ephemeral-1")
	msgEphemeral.IsEphemeral = true
	idEphemeral := uuid.NewString()

	// 2. Enqueue both
	err := fixture.hotQueue.Enqueue(ctx, idPersistent, msgPersistent)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, idEphemeral, msgEphemeral)
	require.NoError(t, err)

	// 3. Act: Migrate
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// 4. Assert Cold Queue has ONLY Persistent message
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 5)
		require.NoError(t, err)
		return len(coldBatch) == 1
	}, 5*time.Second, 100*time.Millisecond, "Cold queue count incorrect")

	// Verify content and ID
	assert.Equal(t, idPersistent, coldBatch[0].ID)
	assert.Equal(t, "data-persistent-1", string(coldBatch[0].Envelope.EncryptedData))
}
