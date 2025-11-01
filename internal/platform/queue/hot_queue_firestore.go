/*
File: internal/platform/queue/firestore_hot_queue.go
Description: REFACTORED to implement the "like-redis" model using two collections
(a main queue and a pending-ack queue) to remove the need for composite indexes.
*/
package queue

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/internal/queue" // Import new interfaces

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// storedHotMessage is the wrapper struct for the hot queue.
// It just needs a timestamp for ordering.
type storedHotMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreHotQueue implements the queue.HotQueue interface using Google Cloud Firestore.
type FirestoreHotQueue struct {
	client                *firestore.Client
	logger                zerolog.Logger
	mainCollectionName    string // e.g., "user-hot-messages"
	pendingCollectionName string // e.g., "user-hot-pending"
}

// NewFirestoreHotQueue is the constructor for the FirestoreHotQueue.
func NewFirestoreHotQueue(client *firestore.Client, mainCollection, pendingCollection string, logger zerolog.Logger) (queue.HotQueue, error) {
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
		logger:                logger,
		mainCollectionName:    mainCollection,
		pendingCollectionName: pendingCollection,
	}, nil
}

// mainCollection is a helper to get the subcollection ref for the main queue
func (s *FirestoreHotQueue) mainCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.mainCollectionName).Doc(urn.String()).Collection("messages")
}

// pendingCollection is a helper to get the subcollection ref for the pending queue
func (s *FirestoreHotQueue) pendingCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.pendingCollectionName).Doc(urn.String()).Collection("messages")
}

// Enqueue saves a single message envelope to the main hot queue.
func (s *FirestoreHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	collectionRef := s.mainCollection(envelope.RecipientID)

	pb := secure.ToProto(envelope)
	if pb == nil {
		s.logger.Warn().Msg("Skipping nil envelope")
		return nil
	}

	storedMsg := &storedHotMessage{
		QueuedAt: time.Now().UTC(),
		Envelope: pb,
	}

	docRef := collectionRef.Doc(uuid.NewString())
	_, err := docRef.Create(ctx, storedMsg)
	return err
}

// RetrieveBatch transactionally moves messages from the main queue to the pending queue.
// This is the Firestore equivalent of RPOPLPUSH.
// This query only uses a single OrderBy, so no composite index is needed.
func (s *FirestoreHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	mainCollectionRef := s.mainCollection(userURN)
	pendingCollectionRef := s.pendingCollection(userURN)
	queuedMessages := make([]*routing.QueuedMessage, 0, limit)

	err := s.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		// 1. Find the oldest messages in the main queue.
		query := mainCollectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)

		docSnaps, err := tx.Documents(query).GetAll()
		if err != nil {
			return err
		}

		if len(docSnaps) == 0 {
			return nil // No messages to retrieve
		}

		// 2. Move each doc from the main collection to the pending collection.
		queuedMessages = make([]*routing.QueuedMessage, 0, len(docSnaps)) // Clear slice
		for _, doc := range docSnaps {
			var storedMsg storedHotMessage
			if err := doc.DataTo(&storedMsg); err != nil {
				s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to unmarshal stored hot message, skipping")
				continue
			}

			// Convert from Protobuf back to the native Go struct
			nativeEnv, err := secure.FromProto(storedMsg.Envelope)
			if err != nil {
				s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to convert protobuf to native envelope, skipping")
				continue
			}

			// Add to our return batch
			queuedMessages = append(queuedMessages, &routing.QueuedMessage{
				ID:       doc.Ref.ID, // Use the *original* doc ID as the ACK ID
				Envelope: nativeEnv,
			})

			// 3. Perform the "move": Create in pending, then Delete from main.
			// The new doc in the pending collection will have the *same ID*
			// as the original document.
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
		return nil, fmt.Errorf("failed to retrieve hot message batch: %w", err)
	}

	return queuedMessages, nil
}

// Acknowledge permanently deletes a list of messages from the *pending* queue.
func (s *FirestoreHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	log := s.logger.With().Str("user", userURN.String()).Logger()
	collectionRef := s.pendingCollection(userURN) // Target the PENDING collection

	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	for _, msgID := range messageIDs {
		docRef := collectionRef.Doc(msgID)
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error().Err(err).Str("doc_id", msgID).Msg("Failed to enqueue hot pending doc for deletion")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	bulkWriter.End()

	if firstErr != nil {
		return fmt.Errorf("failed to enqueue one or more hot pending messages for deletion: %w", firstErr)
	}

	log.Info().Int("count", len(messageIDs)).Msg("Successfully acknowledged (deleted) hot pending messages.")
	return nil
}

// migrateCollection is a helper to move all docs from one collection to the cold queue
func (s *FirestoreHotQueue) migrateCollection(ctx context.Context, collectionRef *firestore.CollectionRef, userURN urn.URN, destination queue.ColdQueue, log zerolog.Logger) (int, error) {
	var allEnvelopes []*secure.SecureEnvelope
	var docIDsToDelete []string

	query := collectionRef.OrderBy("queued_at", firestore.Asc)
	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		return 0, fmt.Errorf("failed to read collection for migration: %w", err)
	}
	if len(docSnaps) == 0 {
		return 0, nil
	}

	// 1. Unmarshal all messages
	for _, doc := range docSnaps {
		var storedMsg storedHotMessage
		if err := doc.DataTo(&storedMsg); err != nil {
			log.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to unmarshal hot message for migration, skipping")
			continue
		}
		nativeEnv, err := secure.FromProto(storedMsg.Envelope)
		if err != nil {
			log.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to convert hot envelope for migration, skipping")
			continue
		}
		allEnvelopes = append(allEnvelopes, nativeEnv)
		docIDsToDelete = append(docIDsToDelete, doc.Ref.ID)
	}

	// 2. Enqueue them into the cold store.
	for _, env := range allEnvelopes {
		if err := destination.Enqueue(ctx, env); err != nil {
			return 0, fmt.Errorf("failed to write to cold queue during migration: %w", err)
		}
	}

	// 3. All writes to cold succeeded. Now delete from hot.
	if err := s.Acknowledge(ctx, userURN, docIDsToDelete); err != nil {
		// This is non-fatal for the migration, but we must log it.
		log.Error().Err(err).Msg("Failed to delete messages from hot collection after migration. Duplicates will exist.")
	}

	return len(allEnvelopes), nil
}

// MigrateToCold moves all messages for a user from both the main and pending hot
// collections to the ColdQueue.
func (s *FirestoreHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination queue.ColdQueue) error {
	log := s.logger.With().Str("user", userURN.String()).Logger()

	// 1. Migrate the pending queue first (they are older)
	pendingCount, err := s.migrateCollection(ctx, s.pendingCollection(userURN), userURN, destination, log)
	if err != nil {
		return fmt.Errorf("failed to migrate pending collection: %w", err)
	}

	// 2. Migrate the main queue
	mainCount, err := s.migrateCollection(ctx, s.mainCollection(userURN), userURN, destination, log)
	if err != nil {
		return fmt.Errorf("failed to migrate main collection: %w", err)
	}

	total := pendingCount + mainCount
	if total > 0 {
		log.Info().Int("count", total).Msg("Successfully migrated hot queue to cold queue.")
	} else {
		log.Info().Msg("No hot queue messages to migrate.")
	}
	return nil
}
