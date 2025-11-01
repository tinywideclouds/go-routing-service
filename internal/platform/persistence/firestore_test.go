//go:build integration

/*
File: internal/platform/persistence/firestore_test.go
Description: REFACTORED integration test for the new Firestore implementation.
This now tests the "Paginate-Save-Ack-Delete" flow.
*/
package persistence_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	routing2 "github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"

	// REFACTORED: Use new generated proto types
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
)

// testFixture holds the shared resources for all tests in this file.
type testFixture struct {
	ctx      context.Context
	fsClient *firestore.Client
	store    routing2.MessageStore
}

// setupSuite initializes the Firestore emulator and all necessary clients ONCE.
func setupSuite(t *testing.T) (context.Context, *testFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	const projectID = "test-project-persistence"
	firestoreEmulator := emulators.SetupFirestoreEmulator(t, ctx, emulators.GetDefaultFirestoreConfig(projectID))
	fsClient, err := firestore.NewClient(ctx, projectID, firestoreEmulator.ClientOptions...)
	require.NoError(t, err)

	// Clean up database after tests
	t.Cleanup(func() {
		_ = fsClient.Close()
	})

	logger := zerolog.Nop()
	store, err := persistence.NewFirestoreStore(fsClient, logger)
	require.NoError(t, err)

	return ctx, &testFixture{
		ctx:      ctx,
		fsClient: fsClient,
		store:    store,
	}
}

// REFACTORED: Updated helper to create the new "dumb" envelope type
func baseEnvelope(recipient urn.URN, data string) *secure.SecureEnvelope {
	return &secure.SecureEnvelope{
		RecipientID:   recipient,
		EncryptedData: []byte(fmt.Sprintf("data-%s", data)),
	}
}

// NEW: Helper to get the underlying stored data for assertions.
type storedMessageForTest struct {
	QueuedAt time.Time                  `firestore:"queued_at"`
	Envelope *securev1.SecureEnvelopePb `firestore:"envelope"`
}

func TestStoreMessages(t *testing.T) {
	ctx, fixture := setupSuite(t)

	recipientURN, _ := urn.Parse("urn:sm:user:store-test-user")
	collectionRef := fixture.fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages")

	msg1 := baseEnvelope(recipientURN, "1")
	msg2 := baseEnvelope(recipientURN, "2")
	envelopes := []*secure.SecureEnvelope{msg1, msg2}

	// Act
	err := fixture.store.StoreMessages(ctx, recipientURN, envelopes)
	require.NoError(t, err)

	// Assert
	docs, err := collectionRef.Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 2, "Should have stored 2 messages")

	// Verify one of the documents to ensure data is correct
	docSnap := docs[0] // We don't know the ID, so just grab the first one
	var storedData storedMessageForTest
	err = docSnap.DataTo(&storedData)
	require.NoError(t, err)

	// Assert a valid UUID was generated
	_, err = uuid.Parse(docSnap.Ref.ID)
	assert.NoError(t, err, "Document ID should be a valid UUID")

	// Assert QueuedAt was set
	assert.WithinDuration(t, time.Now(), storedData.QueuedAt, 5*time.Second, "QueuedAt timestamp should be set")

	// Assert data integrity (check one of the two)
	pb1 := secure.ToProto(msg1)
	pb2 := secure.ToProto(msg2)
	isMsg1 := assert.ObjectsAreEqual(pb1.EncryptedData, storedData.Envelope.EncryptedData)
	isMsg2 := assert.ObjectsAreEqual(pb2.EncryptedData, storedData.Envelope.EncryptedData)
	assert.True(t, isMsg1 || isMsg2, "Stored data does not match either input envelope")
}

// --- DELETED TESTS ---
//
// TestRetrieveMessageDigest
// TestRetrieveHistoryChunk
//
// --- DELETED ---

// NEW: Test for the entire "Paginate-Save-Ack-Delete" flow
func TestRetrieveAndAcknowledgeBatch(t *testing.T) {
	ctx, fixture := setupSuite(t)

	recipientURN, _ := urn.Parse("urn:sm:user:batch-test-user")
	collectionRef := fixture.fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages")

	// --- 1. Seed Data ---
	// We must seed with distinct, ordered timestamps.
	// We will store them manually to control the 'QueuedAt' time.
	msg1_oldest := baseEnvelope(recipientURN, "oldest")
	msg2_older := baseEnvelope(recipientURN, "older")
	msg3_recent := baseEnvelope(recipientURN, "recent")

	now := time.Now().UTC()

	// We generate UUIDs for them to act as the doc IDs
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
	batch1, err := fixture.store.RetrieveMessageBatch(ctx, recipientURN, 2)
	require.NoError(t, err)
	require.Len(t, batch1, 2, "Should have retrieved first batch of 2")

	// Assert it retrieved the two OLDEST messages
	assert.Equal(t, id1, batch1[0].ID)
	assert.Equal(t, id2, batch1[1].ID)
	assert.Equal(t, msg1_oldest.EncryptedData, batch1[0].Envelope.EncryptedData)
	assert.Equal(t, msg2_older.EncryptedData, batch1[1].Envelope.EncryptedData)

	// --- 3. Acknowledge First Batch ---
	idsToAck := []string{batch1[0].ID, batch1[1].ID}
	err = fixture.store.AcknowledgeMessages(ctx, recipientURN, idsToAck)
	require.NoError(t, err)

	// --- 4. Retrieve Second Batch (limit=2) ---
	// Use Eventually to handle slight delay from BulkWriter in emulator
	var batch2 []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		batch2, err = fixture.store.RetrieveMessageBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(batch2) == 1
	}, 5*time.Second, 100*time.Millisecond, "Queue did not update after ack")

	// Assert it retrieved the single remaining message
	require.Len(t, batch2, 1)
	assert.Equal(t, id3, batch2[0].ID)
	assert.Equal(t, msg3_recent.EncryptedData, batch2[0].Envelope.EncryptedData)

	// --- 5. Acknowledge Second Batch ---
	err = fixture.store.AcknowledgeMessages(ctx, recipientURN, []string{batch2[0].ID})
	require.NoError(t, err)

	// --- 6. Retrieve Final Batch (Empty) ---
	var batch3 []*routing.QueuedMessage
	require.Eventually(t, func() bool {
		batch3, err = fixture.store.RetrieveMessageBatch(ctx, recipientURN, 2)
		require.NoError(t, err)
		return len(batch3) == 0
	}, 5*time.Second, 100*time.Millisecond, "Queue was not empty after final ack")

	require.Len(t, batch3, 0, "Queue should be empty")
}

// REFACTORED: Renamed from TestDeleteMessages to TestAcknowledgeMessages
func TestAcknowledgeMessages(t *testing.T) {
	ctx, fixture := setupSuite(t)

	recipientURN, _ := urn.Parse("urn:sm:user:ack-test-user")
	collectionRef := fixture.fsClient.Collection("user-messages").Doc(recipientURN.String()).Collection("messages")

	// 1. Seed data
	messages := []*secure.SecureEnvelope{
		baseEnvelope(recipientURN, "delete-1"),
		baseEnvelope(recipientURN, "keep-1"),
		baseEnvelope(recipientURN, "delete-2"),
	}

	err := fixture.store.StoreMessages(ctx, recipientURN, messages)
	require.NoError(t, err, "Failed to seed messages for ack test")

	// Get the generated IDs
	docs, err := collectionRef.Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 3)

	// We can't guarantee order, so find them
	var idToDelete1, idToKeep, idToDelete2 string
	for _, doc := range docs {
		var data storedMessageForTest
		_ = doc.DataTo(&data)
		env, _ := secure.FromProto(data.Envelope)

		switch string(env.EncryptedData) {
		case "data-delete-1":
			idToDelete1 = doc.Ref.ID
		case "data-keep-1":
			idToKeep = doc.Ref.ID
		case "data-delete-2":
			idToDelete2 = doc.Ref.ID
		}
	}
	require.NotEmpty(t, idToDelete1)
	require.NotEmpty(t, idToKeep)
	require.NotEmpty(t, idToDelete2)

	idsToAck := []string{idToDelete1, idToDelete2, "id-non-existent"}

	// 2. Act
	err = fixture.store.AcknowledgeMessages(ctx, recipientURN, idsToAck)
	require.NoError(t, err, "AcknowledgeMessages should not error on enqueue")

	// 3. Assert
	require.Eventually(t, func() bool {
		// Check that the deleted docs are gone
		doc1, _ := collectionRef.Doc(idToDelete1).Get(ctx)
		doc3, _ := collectionRef.Doc(idToDelete2).Get(ctx)
		return !doc1.Exists() && !doc3.Exists()
	}, 5*time.Second, 100*time.Millisecond, "Messages were not deleted by ack")

	// Check that the remaining doc is still there
	doc2, err := collectionRef.Doc(idToKeep).Get(ctx)
	require.NoError(t, err)
	assert.True(t, doc2.Exists(), "Message that was not acked should still exist")
}
