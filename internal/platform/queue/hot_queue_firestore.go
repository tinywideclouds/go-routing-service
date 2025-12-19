// --- File: internal/platform/queue/hot_queue_firestore.go ---
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

type storedHotMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreHotQueue implements queue.HotQueue with Priority Bands.
type FirestoreHotQueue struct {
	client             *firestore.Client
	logger             *slog.Logger
	mainCollectionName string
	// Implicitly derived name for High Priority: {mainCollectionName}_high
	pendingCollectionName string
}

func NewFirestoreHotQueue(client *firestore.Client, mainCollection, pendingCollection string, logger *slog.Logger) (queue.HotQueue, error) {
	if client == nil || mainCollection == "" || pendingCollection == "" {
		return nil, fmt.Errorf("invalid arguments")
	}
	return &FirestoreHotQueue{
		client:                client,
		logger:                logger.With("component", "firestore_hot_queue"),
		mainCollectionName:    mainCollection,
		pendingCollectionName: pendingCollection,
	}, nil
}

// Enqueue saves to either the High or Standard collection based on Priority.
// It uses the provided messageID as the Document ID.
func (s *FirestoreHotQueue) Enqueue(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	var collectionRef *firestore.CollectionRef
	log := s.logger.With("user", envelope.RecipientID.String(), "msg_id", messageID)

	if messageID == "" {
		return fmt.Errorf("messageID cannot be empty")
	}

	// 1. Determine Target Band
	if envelope.Priority >= 5 {
		collectionRef = s.highCollection(envelope.RecipientID)
		log.Info("Enqueuing to HIGH priority collection")
	} else {
		collectionRef = s.mainCollection(envelope.RecipientID)
		log.Debug("Enqueuing to STANDARD priority collection")
	}

	pb := secure.ToProto(envelope)
	if pb == nil {
		return nil
	}

	storedMsg := &storedHotMessage{
		QueuedAt: time.Now().UTC(),
		Envelope: pb,
	}

	// Use the deterministic messageID as the Document ID
	docRef := collectionRef.Doc(messageID)
	_, err := docRef.Set(ctx, storedMsg)
	if err != nil {
		log.Error("Failed to enqueue message", "err", err)
		return err
	}
	return nil
}

// RetrieveBatch queries High Priority first, then fills remainder from Standard.
func (s *FirestoreHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	highCollectionRef := s.highCollection(userURN)
	mainCollectionRef := s.mainCollection(userURN)
	pendingCollectionRef := s.pendingCollection(userURN)

	queuedMessages := make([]*routing.QueuedMessage, 0, limit)
	log := s.logger.With("user", userURN.String())

	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// 1. Query High Priority
		highQuery := highCollectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)
		highDocs, err := tx.Documents(highQuery).GetAll()
		if err != nil {
			return err
		}

		// 2. Query Standard Priority (if needed)
		remainingLimit := limit - len(highDocs)
		var stdDocs []*firestore.DocumentSnapshot

		if remainingLimit > 0 {
			stdQuery := mainCollectionRef.OrderBy("queued_at", firestore.Asc).Limit(remainingLimit)
			stdDocs, err = tx.Documents(stdQuery).GetAll()
			if err != nil {
				return err
			}
		}

		// 3. Process & Move ALL found docs to Pending
		allDocs := append(highDocs, stdDocs...)
		if len(allDocs) == 0 {
			return nil // Empty
		}

		queuedMessages = make([]*routing.QueuedMessage, 0, len(allDocs))

		for _, doc := range allDocs {
			var storedMsg storedHotMessage
			if err := doc.DataTo(&storedMsg); err != nil {
				continue
			}
			nativeEnv, err := secure.FromProto(storedMsg.Envelope)
			if err != nil {
				continue
			}

			queuedMessages = append(queuedMessages, &routing.QueuedMessage{
				ID:       doc.Ref.ID, // This ID is preserved from Enqueue
				Envelope: nativeEnv,
			})

			// Move to Pending
			pendingDocRef := pendingCollectionRef.Doc(doc.Ref.ID)
			if err := tx.Create(pendingDocRef, storedMsg); err != nil {
				return err
			}
			if err := tx.Delete(doc.Ref); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Error("Failed to retrieve batch transaction", "err", err)
		return nil, err
	}

	return queuedMessages, nil
}

// Acknowledge deletes from Pending collection.
func (s *FirestoreHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return s.deleteDocsFromCollection(ctx, s.pendingCollection(userURN), messageIDs, s.logger)
}

// MigrateToCold moves High, Standard, and Pending to ColdQueue, preserving IDs.
func (s *FirestoreHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With("user", userURN.String())
	log.Info("Starting hot-to-cold migration")

	// 1. Migrate Pending
	if _, err := s.migrateCollection(ctx, s.pendingCollection(userURN), destination, log); err != nil {
		return err
	}
	// 2. Migrate High Priority
	if _, err := s.migrateCollection(ctx, s.highCollection(userURN), destination, log); err != nil {
		return err
	}
	// 3. Migrate Standard Priority
	if _, err := s.migrateCollection(ctx, s.mainCollection(userURN), destination, log); err != nil {
		return err
	}

	log.Info("Migration complete")
	return nil
}

// --- Private Helpers ---

func (s *FirestoreHotQueue) mainCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.mainCollectionName).Doc(urn.String()).Collection("messages")
}

// Helper for High Priority Collection
func (s *FirestoreHotQueue) highCollection(urn urn.URN) *firestore.CollectionRef {
	// We append "_high" to the main collection name config to keep it grouped logically
	return s.client.Collection(s.mainCollectionName + "_high").Doc(urn.String()).Collection("messages")
}

func (s *FirestoreHotQueue) pendingCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.pendingCollectionName).Doc(urn.String()).Collection("messages")
}

func (s *FirestoreHotQueue) deleteDocsFromCollection(ctx context.Context, collectionRef *firestore.CollectionRef, docIDs []string, _ *slog.Logger) error {
	if len(docIDs) == 0 {
		return nil
	}
	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error
	for _, msgID := range docIDs {
		if _, err := bulkWriter.Delete(collectionRef.Doc(msgID)); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	bulkWriter.End()
	return firstErr
}

// migrateCollection reads docs, enqueues them to cold storage using the EXISTING ID, and deletes them from hot.
func (s *FirestoreHotQueue) migrateCollection(ctx context.Context, collectionRef *firestore.CollectionRef, destination queue.ColdQueue, log *slog.Logger) (int, error) {
	query := collectionRef.OrderBy("queued_at", firestore.Asc)
	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil || len(docSnaps) == 0 {
		return 0, err
	}

	var docIDsToDelete []string
	var count int

	for _, doc := range docSnaps {
		var storedMsg storedHotMessage
		doc.DataTo(&storedMsg)
		nativeEnv, _ := secure.FromProto(storedMsg.Envelope)

		docIDsToDelete = append(docIDsToDelete, doc.Ref.ID)
		if !nativeEnv.IsEphemeral {
			// Enqueue to Cold Queue using the EXISTING Doc ID.
			if err := destination.Enqueue(ctx, doc.Ref.ID, nativeEnv); err != nil {
				log.Error("Failed to migrate message to cold queue", "msg_id", doc.Ref.ID, "err", err)
				return count, err
			}
			count++
		}
	}

	if len(docIDsToDelete) > 0 {
		s.deleteDocsFromCollection(ctx, collectionRef, docIDsToDelete, log)
	}
	return count, nil
}
