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

type gcloudTestFixture struct {
	ctx                   context.Context
	fsClient              *firestore.Client
	projectID             string
	coldQueue             queue.ColdQueue
	hotQueue              queue.HotQueue
	coldCollectionName    string
	hotMainCollectionName string
	hotPendingColName     string
}

func setupGCloudSuite(t *testing.T) *gcloudTestFixture {
	t.Helper()

	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		t.Skip("Skipping gcloud test: GCP_PROJECT_ID environment variable is not set.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

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

func createTestURN(t *testing.T) urn.URN {
	t.Helper()
	userID := fmt.Sprintf("gcloud-test-%s", uuid.NewString())
	recipientURN, err := urn.New(urn.SecureMessaging, urn.EntityTypeUser, userID)
	require.NoError(t, err)
	return recipientURN
}

func createTestEnvelope(t *testing.T, recipientURN urn.URN, data string) *secure.SecureEnvelope {
	t.Helper()
	return &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

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

	recipientURN := createTestURN(t)
	msg1 := createTestEnvelope(t, recipientURN, "cold-ack-1")
	msg2 := createTestEnvelope(t, recipientURN, "cold-ack-2")
	id1 := uuid.NewString()
	id2 := uuid.NewString()

	t.Cleanup(func() { cleanupUser(t, ctx, fixture.fsClient, fixture.coldCollectionName, recipientURN) })

	err := fixture.coldQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)
	err = fixture.coldQueue.Enqueue(ctx, id2, msg2)
	require.NoError(t, err)

	// 3. Acknowledge just one of them
	err = fixture.coldQueue.Acknowledge(ctx, recipientURN, []string{id1})
	require.NoError(t, err)

	collectionRef := fixture.fsClient.Collection(fixture.coldCollectionName).Doc(recipientURN.String()).Collection("messages")
	docRef1 := collectionRef.Doc(id1)
	docRef2 := collectionRef.Doc(id2)

	require.Eventually(t, func() bool {
		exists, err := docExists(t, ctx, docRef1)
		require.NoError(t, err)
		return !exists
	}, 30*time.Second, 2*time.Second)

	exists, err := docExists(t, ctx, docRef2)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestGCloud_HotQueue_MigrateToCold(t *testing.T) {
	fixture := setupGCloudSuite(t)
	ctx := fixture.ctx

	recipientURN := createTestURN(t)
	msg1 := createTestEnvelope(t, recipientURN, "migrate-1")
	id1 := uuid.NewString()

	t.Cleanup(func() {
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotMainCollectionName, recipientURN)
		cleanupUser(t, ctx, fixture.fsClient, fixture.hotPendingColName, recipientURN)
		cleanupUser(t, ctx, fixture.fsClient, fixture.coldCollectionName, recipientURN)
	})

	err := fixture.hotQueue.Enqueue(ctx, id1, msg1)
	require.NoError(t, err)

	// Move to pending
	require.Eventually(t, func() bool {
		hotBatch, err := fixture.hotQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(hotBatch) == 1
	}, 30*time.Second, 1*time.Second)

	// Migrate
	err = fixture.hotQueue.MigrateToCold(ctx, recipientURN, fixture.coldQueue)
	require.NoError(t, err)

	// Verify ID preservation in cold queue
	var coldBatch []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		coldBatch, err = fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(coldBatch) == 1
	}, 30*time.Second, 2*time.Second)

	assert.Equal(t, id1, coldBatch[0].ID)
}
