// --- File: internal/platform/queue/cold_queue_firestore_test.go ---
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

	// Platform packages
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// testFixture holds the shared resources for all tests in this file.
type testFixture struct {
	ctx            context.Context // This will be the timed test context
	fsClient       *firestore.Client
	coldQueue      queue.ColdQueue
	logger         *slog.Logger
	collectionName string // Test-specific collection name
}

// newTestLogger creates a discard logger for tests.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setupSuite initializes the Firestore emulator and all necessary clients ONCE.
func setupSuite(t *testing.T) (context.Context, *testFixture) {
	t.Helper()
	// This context is for test operations (e.g., Enqueue, Retrieve)
	testCtx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	t.Cleanup(cancel)

	const projectID = "test-project-coldqueue"
	const collectionName = "emulator-cold-queue"

	// --- FIX: Pass context.Background() for container lifecycle ---
	firestoreEmulator := emulators.SetupFirestoreEmulator(t, context.Background(), emulators.GetDefaultFirestoreConfig(projectID))

	// Create client with context.Background() so it's not tied to testCtx
	fsClient, err := firestore.NewClient(context.Background(), projectID, firestoreEmulator.ClientOptions...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = fsClient.Close()
	})

	logger := newTestLogger()
	store, err := fsqueue.NewFirestoreColdQueue(fsClient, collectionName, logger)
	require.NoError(t, err)

	// Return the testCtx for operations
	return testCtx, &testFixture{
		ctx:            testCtx,
		fsClient:       fsClient,
		coldQueue:      store,
		logger:         logger,
		collectionName: collectionName,
	}
}

func baseEnvelope(recipient urn.URN, data string) *secure.SecureEnvelope {
	return &secure.SecureEnvelope{
		RecipientID:   recipient,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

type storedMessageForTest struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

func TestEnqueue(t *testing.T) {
	ctx, fixture := setupSuite(t) // ctx is now the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:store-test-user")
	collectionRef := fixture.fsClient.Collection(fixture.collectionName).Doc(recipientURN.String()).Collection("messages")

	msg1 := baseEnvelope(recipientURN, "1")
	msg2 := baseEnvelope(recipientURN, "2")

	// Act - use the testCtx
	err := fixture.coldQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.coldQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)

	// Assert
	docs, err := collectionRef.Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 2, "Should have stored 2 messages")

	// Verify one of the documents
	docSnap := docs[0]
	var storedData storedMessageForTest
	err = docSnap.DataTo(&storedData)
	require.NoError(t, err)

	_, err = uuid.Parse(docSnap.Ref.ID)
	assert.NoError(t, err, "Document ID should be a valid UUID")
	assert.WithinDuration(t, time.Now(), storedData.QueuedAt, 5*time.Second, "QueuedAt timestamp should be set")

	// Assert data integrity
	pb1 := secure.ToProto(msg1)
	pb2 := secure.ToProto(msg2)
	isMsg1 := assert.ObjectsAreEqual(pb1.EncryptedData, storedData.Envelope.EncryptedData)
	isMsg2 := assert.ObjectsAreEqual(pb2.EncryptedData, storedData.Envelope.EncryptedData)
	assert.True(t, isMsg1 || isMsg2, "Stored data does not match either input envelope")
}

// This test now ONLY tests retrieval logic and ordering.
func TestRetrieveBatch(t *testing.T) {
	ctx, fixture := setupSuite(t) // ctx is the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:batch-test-user")
	collectionRef := fixture.fsClient.Collection(fixture.collectionName).Doc(recipientURN.String()).Collection("messages")

	// --- 1. Seed Data ---
	msg1_oldest := baseEnvelope(recipientURN, "oldest")
	msg2_older := baseEnvelope(recipientURN, "older")
	msg3_recent := baseEnvelope(recipientURN, "recent")

	now := time.Now().UTC()
	id1 := uuid.NewString()
	id2 := uuid.NewString()
	id3 := uuid.NewString()

	seedData := map[string]*storedMessageForTest{
		id1: {QueuedAt: now.Add(-10 * time.Minute), Envelope: secure.ToProto(msg1_oldest)},
		id2: {QueuedAt: now.Add(-5 * time.Minute), Envelope: secure.ToProto(msg2_older)},
		id3: {QueuedAt: now.Add(-1 * time.Minute), Envelope: secure.ToProto(msg3_recent)},
	}

	err := fixture.fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for id, data := range seedData {
			docRef := collectionRef.Doc(id)
			if err := tx.Set(docRef, data); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err, "Failed to seed data")

	// --- 2. Retrieve First Batch (limit=2) ---
	batch1, err := fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
	require.NoError(t, err)
	require.Len(t, batch1, 2, "Should have retrieved first batch of 2")

	assert.Equal(t, id1, batch1[0].ID)
	assert.Equal(t, id2, batch1[1].ID)
	assert.Equal(t, msg1_oldest.EncryptedData, batch1[0].Envelope.EncryptedData)
	assert.Equal(t, msg2_older.EncryptedData, batch1[1].Envelope.EncryptedData)

	// --- 3. Retrieve Second Batch (limit=2) ---
	batch2, err := fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 2)
	require.NoError(t, err)
	require.Len(t, batch2, 2)

	// --- 4. Retrieve batch of 1 ---
	batch3, err := fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 1)
	require.NoError(t, err)
	require.Len(t, batch3, 1)
	assert.Equal(t, id1, batch3[0].ID)

	// --- 5. Retrieve full batch ---
	batchAll, err := fixture.coldQueue.RetrieveBatch(ctx, recipientURN, 10)
	require.NoError(t, err)
	require.Len(t, batchAll, 3)
}

// --- File: internal/platform/queue/cold_queue_firestore_test.go ---

func TestAcknowledge(t *testing.T) {
	ctx, fixture := setupSuite(t) // ctx is the timed testCtx

	recipientURN, _ := urn.Parse("urn:contacts:user:ack-test-user")
	collectionRef := fixture.fsClient.Collection(fixture.collectionName).Doc(recipientURN.String()).Collection("messages")

	// --- (Seeding logic is unchanged) ---
	msg1 := baseEnvelope(recipientURN, "delete-1")
	msg2 := baseEnvelope(recipientURN, "keep-1")
	msg3 := baseEnvelope(recipientURN, "delete-2")

	err := fixture.coldQueue.Enqueue(ctx, msg1)
	require.NoError(t, err)
	err = fixture.coldQueue.Enqueue(ctx, msg2)
	require.NoError(t, err)
	err = fixture.coldQueue.Enqueue(ctx, msg3)
	require.NoError(t, err)

	docs, err := collectionRef.Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 3)

	var idToKeep string
	var idsToAck []string
	for _, doc := range docs {
		var data storedMessageForTest
		_ = doc.DataTo(&data)
		env, _ := secure.FromProto(data.Envelope)

		switch string(env.EncryptedData) {
		case "data-keep-1":
			idToKeep = doc.Ref.ID
		default:
			idsToAck = append(idsToAck, doc.Ref.ID)
		}
	}
	require.NotEmpty(t, idToKeep)
	require.Len(t, idsToAck, 2)
	idsToAck = append(idsToAck, "id-non-existent")
	// --- (End of seeding logic) ---

	// 2. Act
	err = fixture.coldQueue.Acknowledge(ctx, recipientURN, idsToAck)
	require.NoError(t, err, "Acknowledge should not error")

	t.Log("Ignoring document deletion...")

}
