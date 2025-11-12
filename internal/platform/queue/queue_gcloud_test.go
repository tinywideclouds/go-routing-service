// --- File: internal/platform/queue/queue_gcloud_test.go ---
//go:build integration_gcloud

package queue_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"google.golang.org/api/iterator"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gcloudTestFixture holds the clients and config for a live test.
type gcloudTestFixture struct {
	ctx                   context.Context
	fsClient              *firestore.Client
	projectID             string
	coldQueue             queue.ColdQueue
	hotQueue              queue.HotQueue
	coldCollectionName    string // Test-specific collection name
	hotMainCollectionName string // Test-specific collection name
	hotPendingColName     string // Test-specific collection name
}

// setupGCloudSuite initializes a real Firestore client.
func setupGCloudSuite(t *testing.T) *gcloudTestFixture {
	t.Helper()

	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		t.Skip("Skipping gcloud test: GCP_PROJECT_ID environment variable is not set.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// --- FIX: Create client with context.Background() ---
	fsClient, err := firestore.NewClient(context.Background(), projectID)
	require.NoError(t, err, "Failed to create real Firestore client")
	t.Cleanup(func() { _ = fsClient.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	const hotMainCollectionName = "test-hot-main"
	const hotPendingCollectionName = "test-hot-pending"
	const coldCollectionName = "test-cold-queue"

	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, coldCollectionName, logger)
	require.NoError(t, err)

	hotQueue, err := fsqueue.NewFirestoreHotQueue(fsClient, hotMainCollectionName, hotPendingCollectionName, logger)
	require.NoError(t, err)

	return &gcloudTestFixture{
		ctx:                   ctx,
		fsClient:              fsClient,
		projectID:             projectID,
		coldQueue:             coldQueue,
		hotQueue:              hotQueue,
		coldCollectionName:    coldCollectionName,
		hotMainCollectionName: hotMainCollectionName,
		hotPendingColName:     hotPendingCollectionName,
	}
}

// Helper to check if a document exists.
func docExists(t *testing.T, ctx context.Context, docRef *firestore.DocumentRef) (bool, error) {
	t.Helper()
	_, err := docRef.Get(ctx)
	if err == nil {
		return true, nil
	}
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

// Helper to recursively delete a collection in batches.
func deleteCollection(ctx context.Context, client *firestore.Client, ref *firestore.CollectionRef, batchSize int) error {
	for {
		iter := ref.Limit(batchSize).Documents(ctx)
		numDeleted := 0
		batch := client.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			batch.Delete(doc.Ref)
			numDeleted++
		}

		if numDeleted == 0 {
			return nil
		}

		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
	}
}

// createTestURN creates ONE unique user for a test.
func createTestURN(t *testing.T) urn.URN {
	t.Helper()
	userID := fmt.Sprintf("gcloud-test-%s", uuid.NewString())
	recipientURN, err := urn.New(urn.SecureMessaging, urn.EntityTypeUser, userID)
	require.NoError(t, err)
	return recipientURN
}

// createTestEnvelope creates an envelope for a specific URN.
func createTestEnvelope(t *testing.T, recipientURN urn.URN, data string) *secure.SecureEnvelope {
	t.Helper()
	return &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

// cleanupUser is a helper to delete all data for a user from a collection.
func cleanupUser(t *testing.T, ctx context.Context, client *firestore.Client, collectionName string, urn urn.URN) {
	t.Logf("Cleanup: Deleting user doc %s from %s", urn.String(), collectionName)
	collectionRef := client.Collection(collectionName).Doc(urn.String()).Collection("messages")
	err := deleteCollection(ctx, client, collectionRef, 20)
	if err != nil {
		t.Logf("Cleanup failed for subcollection: %v", err)
	}
	_, err = client.Collection(collectionName).Doc(urn.String()).Delete(ctx)
	if err != nil {
		t.Logf("Cleanup failed for user doc: %v", err)
	}
}

func TestGCloud_ColdQueue_Acknowledge(t *testing.T) {
	fixture := setupGCloudSuite(t)
	ctx := fixture.ctx

	// Create ONE user. Create all messages for THAT user.
	recipientURN := createTestURN(t)
	msg1 := createTestEnvelope(t, recipientURN, "cold-ack-1")
	msg2 := createTestEnvelope(t, recipientURN, "cold-ack-2")

	coldCollectionRef := fixture.fsClient.Collection(fixture.coldCollectionName).Doc(recipientURN.String()).Collection("messages")
	t.Cleanup(func() { cleanupUser(t, ctx, fixture.fsClient, fixture.coldCollectionName, recipientURN) })

	// 1. Enqueue two messages
	err := fixture.coldQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.coldQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)

	// 2. Retrieve them to get their generated IDs
	var batch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		batch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(batch) == 2
	}, 20*time.Second, 1*time.Second, "Failed to retrieve both messages")

	id1 := batch[0].ID
	id2 := batch[1].ID
	docRef1 := coldCollectionRef.Doc(id1)
	docRef2 := coldCollectionRef.Doc(id2)

	// 3. Acknowledge just one of them
	err = fixture.coldQueue.Acknowledge(ctx, recipientURN, []string{id1})
	require.NoError(t, err)

	// 4. VERIFY DELETION
	t.Log("Verifying deletion of message 1 (this may take a few seconds)...")
	require.Eventually(t, func() bool {
		exists, err := docExists(t, ctx, docRef1)
		require.NoError(t, err)
		return !exists
	}, 30*time.Second, 2*time.Second, "Acknowledged message was not deleted from cold queue")

	t.Log("Message 1 successfully deleted.")

	// 5. Verify the other message still exists
	exists, err := docExists(t, ctx, docRef2)
	require.NoError(t, err)
	assert.True(t, exists, "Un-acknowledged message was deleted")
}

func TestGCloud_HotQueue_Acknowledge(t *testing.T) {
	fixture := setupGCloudSuite(t)
	ctx := fixture.ctx

	t.Log("INFO: This test uses the two-collection model and requires no special index.")

	recipientURN := createTestURN(t)
	msg1 := createTestEnvelope(t, recipientURN, "hot-ack-1")

	hotCollectionRef := fixture.fsClient.Collection(fixture.hotMainCollectionName).Doc(recipientURN.String()).Collection("messages")
	pendingCollectionRef := fixture.fsClient.Collection(fixture.hotPendingColName).Doc(recipientURN.String()).Collection("messages")
	t.Cleanup(func() {
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotMainCollectionName, recipientURN)
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotPendingColName, recipientURN)
	})

	// 1. Enqueue and Retrieve to get ID
	err := fixture.hotQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)

	var batch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		batch, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 1)
		require.NoError(t, err)
		return len(batch) == 1
	}, 20*time.Second, 1*time.Second, "Failed to retrieve hot message")

	id1 := batch[0].ID
	docRefMain := hotCollectionRef.Doc(id1)
	docRefPending := pendingCollectionRef.Doc(id1) // Path to pending doc

	// Verify the "move" logic
	existsMain, err := docExists(t, ctx, docRefMain)
	require.NoError(t, err)
	assert.False(t, existsMain, "Message was NOT deleted from main hot queue after retrieve")

	existsPending, err := docExists(t, ctx, docRefPending)
	require.NoError(t, err)
	assert.True(t, existsPending, "Message was NOT created in pending hot queue after retrieve")

	// 2. Acknowledge
	err = fixture.hotQueue.Acknowledge(ctx, recipientURN, []string{id1})
	require.NoError(t, err)

	// 3. VERIFY DELETION (from PENDING collection)
	t.Log("Verifying deletion of message from hot PENDING queue (this may take a few seconds)...")
	require.Eventually(t, func() bool {
		exists, err := docExists(t, ctx, docRefPending) // Check PENDING doc
		require.NoError(t, err)
		return !exists
	}, 30*time.Second, 2*time.Second, "Acknowledged message was not deleted from hot PENDING queue")

	t.Log("Message successfully deleted.")
}

func TestGCloud_HotQueue_MigrateToCold(t *testing.T) {
	fixture := setupGCloudSuite(t)
	ctx := fixture.ctx

	t.Log("INFO: This test uses the two-collection model and requires no special index.")

	// Create ONE user. Create all messages for THAT user.
	recipientURN := createTestURN(t)
	msg1 := createTestEnvelope(t, recipientURN, "migrate-1")
	msg2 := createTestEnvelope(t, recipientURN, "migrate-2")

	pendingCollectionRef := fixture.fsClient.Collection(fixture.hotPendingColName).Doc(recipientURN.String()).Collection("messages")
	t.Cleanup(func() {
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotMainCollectionName, recipientURN)
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotPendingColName, recipientURN)
		cleanupUser(t, ctx, fixture.fsClient, fixture.coldCollectionName, recipientURN)
	})

	// 1. Seed Hot Queue
	err := fixture.hotQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.hotQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)

	// Get hot doc IDs for verification
	var hotBatch []*routing.QueuedMessage
	// Wrap in Eventually to handle gcloud eventual consistency.
	require.Eventually(t, func() bool {
		hotBatch, err = fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(hotBatch) == 2
	}, 30*time.Second, 1*time.Second, "Failed to retrieve seeded hot messages")

	// At this point, both messages are in the PENDING collection
	hotDocRef1 := pendingCollectionRef.Doc(hotBatch[0].ID)
	hotDocRef2 := pendingCollectionRef.Doc(hotBatch[1].ID)

	// 2. Act: Migrate
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// 3. VERIFY HOT QUEUE DELETION
	t.Log("Verifying deletion from hot PENDING queue (this may take a few seconds)...")
	require.Eventually(t, func() bool {
		exists1, _ := docExists(t, ctx, hotDocRef1)
		exists2, _ := docExists(t, ctx, hotDocRef2)
		return !exists1 && !exists2
	}, 30*time.Second, 2*time.Second, "Messages were not deleted from hot PENDING queue after migration")
	t.Log("Hot queue successfully emptied.")

	// 4. VERIFY COLD QUEUE CREATION
	t.Log("Verifying creation in cold queue...")
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(coldBatch) == 2
	}, 30*time.Second, 2*time.Second, "Messages did not appear in cold queue")

	// Check content
	var data1, data2 bool
	for _, msg := range coldBatch {
		if string(msg.Envelope.EncryptedData) == "data-migrate-1" {
			data1 = true
		}
		if string(msg.Envelope.EncryptedData) == "data-migrate-2" {
			data2 = true
		}
	}
	assert.True(t, data1, "Message 1 not found in cold queue")
	assert.True(t, data2, "Message 2 not found in cold queue")
}
