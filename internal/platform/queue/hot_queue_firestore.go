// --- File: internal/platform/queue/hot_queue_firestore.go ---
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/tinywideclouds/go-routing-service/internal/queue"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// storedHotMessage is the wrapper struct for the hot queue.
type storedHotMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreHotQueue implements the queue.HotQueue interface using Google Cloud Firestore.
type FirestoreHotQueue struct {
	client                *firestore.Client
	logger                *slog.Logger
	mainCollectionName    string
	pendingCollectionName string
}

// NewFirestoreHotQueue constructor...
func NewFirestoreHotQueue(client *firestore.Client, mainCollection, pendingCollection string, logger *slog.Logger) (queue.HotQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}
	if mainCollection == "" || pendingCollection == "" {
		return nil, fmt.Errorf("collection names cannot be empty")
	}
	if mainCollection == pendingCollection {
		return nil, fmt.Errorf("main and pending collection names must be different")
	}
	return &FirestoreHotQueue{
		client:                client,
		logger:                logger.With("component", "firestore_hot_queue"),
		mainCollectionName:    mainCollection,
		pendingCollectionName: pendingCollection,
	}, nil
}

// Enqueue saves a single message envelope to the main hot queue.
func (s *FirestoreHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	collectionRef := s.mainCollection(envelope.RecipientID)
	log := s.logger.With("user", envelope.RecipientID.String())

	pb := secure.ToProto(envelope)
	if pb == nil {
		log.Warn("Skipping nil envelope")
		return nil
	}

	storedMsg := &storedHotMessage{
		QueuedAt: time.Now().UTC(),
		Envelope: pb,
	}

	docRef := collectionRef.Doc(uuid.NewString())
	_, err := docRef.Create(ctx, storedMsg)
	if err != nil {
		log.Error("Failed to enqueue message to hot queue", "err", err)
		return err
	}
	log.Debug("Enqueued message to hot queue", "doc_id", docRef.ID)
	return err
}

// RetrieveBatch transactionally moves a batch of messages...
func (s *FirestoreHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	mainCollectionRef := s.mainCollection(userURN)
	pendingCollectionRef := s.pendingCollection(userURN)
	queuedMessages := make([]*routing.QueuedMessage, 0, limit)
	log := s.logger.With("user", userURN.String())

	log.Debug("Retrieving hot message batch", "limit", limit)
	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		query := mainCollectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)

		docSnaps, err := tx.Documents(query).GetAll()
		if err != nil {
			return err
		}

		if len(docSnaps) == 0 {
			log.Debug("No hot messages found for user")
			return nil
		}

		queuedMessages = make([]*routing.QueuedMessage, 0, len(docSnaps))
		for _, doc := range docSnaps {
			var storedMsg storedHotMessage
			if err := doc.DataTo(&storedMsg); err != nil {
				log.Error("Failed to unmarshal stored hot message, skipping", "err", err, "doc_id", doc.Ref.ID)
				continue
			}

			nativeEnv, err := secure.FromProto(storedMsg.Envelope)
			if err != nil {
				log.Error("Failed to convert protobuf to native envelope, skipping", "err", err, "doc_id", doc.Ref.ID)
				continue
			}

			queuedMessages = append(queuedMessages, &routing.QueuedMessage{
				ID:       doc.Ref.ID,
				Envelope: nativeEnv,
			})

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
		log.Error("Failed to retrieve hot message batch transaction", "err", err)
		return nil, fmt.Errorf("failed to retrieve hot message batch: %w", err)
	}

	if len(queuedMessages) > 0 {
		log.Debug("Retrieved and moved hot message batch to pending", "count", len(queuedMessages))
	}
	return queuedMessages, nil
}

// Acknowledge permanently deletes a list of messages from the *pending* queue...
func (s *FirestoreHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	log := s.logger.With("user", userURN.String())
	collectionRef := s.pendingCollection(userURN)

	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	log.Debug("Enqueuing hot pending messages for deletion", "count", len(messageIDs))

	for _, msgID := range messageIDs {
		docRef := collectionRef.Doc(msgID)
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error("Failed to enqueue hot pending doc for deletion", "err", err, "doc_id", msgID)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	bulkWriter.End()

	if firstErr != nil {
		log.Error("Failed to enqueue one or more hot pending messages for deletion", "err", firstErr)
		return fmt.Errorf("failed to enqueue one or more hot pending messages for deletion: %w", firstErr)
	}

	log.Info("Successfully acknowledged (deleted) hot pending messages", "count", len(messageIDs))
	return nil
}

// MigrateToCold moves all messages for a user from both the main and pending hot
// collections to the ColdQueue.
func (s *FirestoreHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With("user", userURN.String())

	log.Debug("Starting hot-to-cold migration")

	// 1. Migrate the pending queue first (they are older)
	log.Debug("Migrating pending collection...")
	pendingCount, err := s.migrateCollection(ctx, s.pendingCollection(userURN), destination, log)
	if err != nil {
		log.Error("Failed to migrate pending collection", "err", err)
		return fmt.Errorf("failed to migrate pending collection: %w", err)
	}

	// 2. Migrate the main queue
	log.Debug("Migrating main collection...")
	mainCount, err := s.migrateCollection(ctx, s.mainCollection(userURN), destination, log)
	if err != nil {
		log.Error("Failed to migrate main collection", "err", err)
		return fmt.Errorf("failed to migrate main collection: %w", err)
	}

	total := pendingCount + mainCount
	if total > 0 {
		log.Info("Successfully migrated hot queue to cold queue", "count", total)
	} else {
		log.Info("No hot queue messages to migrate.")
	}
	return nil
}

// --- Private Helpers ---

func (s *FirestoreHotQueue) mainCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.mainCollectionName).Doc(urn.String()).Collection("messages")
}

func (s *FirestoreHotQueue) pendingCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.pendingCollectionName).Doc(urn.String()).Collection("messages")
}

func (s *FirestoreHotQueue) deleteDocsFromCollection(ctx context.Context, collectionRef *firestore.CollectionRef, docIDs []string, log *slog.Logger) error {
	if len(docIDs) == 0 {
		return nil
	}
	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	for _, msgID := range docIDs {
		docRef := collectionRef.Doc(msgID)
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error("Failed to enqueue doc for deletion", "err", err, "doc_id", msgID)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	bulkWriter.End()

	return firstErr
}

// migrateCollection is a helper to move all docs from one collection to the cold queue.
// REFACTOR: Added logic to DROP ephemeral messages while ensuring they are still deleted from hot.
func (s *FirestoreHotQueue) migrateCollection(ctx context.Context, collectionRef *firestore.CollectionRef, destination queue.ColdQueue, log *slog.Logger) (int, error) {
	var messagesToPersist []*secure.SecureEnvelope
	var docIDsToDelete []string

	log.Debug("Querying collection for migration")
	query := collectionRef.OrderBy("queued_at", firestore.Asc)
	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		log.Error("Failed to read collection for migration", "err", err)
		return 0, fmt.Errorf("failed to read collection for migration: %w", err)
	}
	if len(docSnaps) == 0 {
		log.Debug("Collection empty, no migration needed")
		return 0, nil
	}
	log.Debug("Found messages for migration", "count", len(docSnaps))

	// 1. Unmarshal and Filter
	for _, doc := range docSnaps {
		var storedMsg storedHotMessage
		if err := doc.DataTo(&storedMsg); err != nil {
			log.Error("Failed to unmarshal hot message for migration, skipping", "err", err, "doc_id", doc.Ref.ID)
			continue
		}
		nativeEnv, err := secure.FromProto(storedMsg.Envelope)
		if err != nil {
			log.Error("Failed to convert hot envelope for migration, skipping", "err", err, "doc_id", doc.Ref.ID)
			continue
		}

		// --- NEW: Ephemeral Drop Logic ---
		// Always mark for deletion from Hot Queue
		docIDsToDelete = append(docIDsToDelete, doc.Ref.ID)

		// Only add to persist list if NOT ephemeral
		if !nativeEnv.IsEphemeral {
			messagesToPersist = append(messagesToPersist, nativeEnv)
		} else {
			log.Debug("Dropping ephemeral message during migration", "doc_id", doc.Ref.ID)
		}
		// --- END NEW ---
	}

	// 2. Enqueue PERSISTENT messages into the cold store.
	if len(messagesToPersist) > 0 {
		log.Debug("Writing messages to cold queue", "count", len(messagesToPersist))
		for _, env := range messagesToPersist {
			if err := destination.Enqueue(ctx, env); err != nil {
				log.Error("Failed to write to cold queue during migration", "err", err)
				return 0, fmt.Errorf("failed to write to cold queue during migration: %w", err)
			}
		}
	} else {
		log.Debug("No persistent messages to migrate (all ephemeral or empty)")
	}

	// 3. All writes to cold succeeded (or none needed). Now delete ALL from hot.
	log.Debug("Deleting messages from hot collection", "count", len(docIDsToDelete))
	if err := s.deleteDocsFromCollection(ctx, collectionRef, docIDsToDelete, log); err != nil {
		log.Error("Failed to delete messages from hot collection after migration. Duplicates will exist.", "err", err)
	}

	return len(messagesToPersist), nil
}
