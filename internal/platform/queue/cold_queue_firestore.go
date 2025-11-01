/*
File: internal/platform/queue/firestore_cold_queue.go
Description: REFACTORED to remove hardcoded collection names.
The collection is now passed in via the constructor.
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

// storedMessage is the wrapper struct we will store in Firestore.
type storedMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreColdQueue implements the queue.ColdQueue interface using Google Cloud Firestore.
type FirestoreColdQueue struct {
	client         *firestore.Client
	logger         zerolog.Logger
	collectionName string // Configured collection name
}

// NewFirestoreColdQueue is the constructor for the FirestoreColdQueue.
func NewFirestoreColdQueue(client *firestore.Client, collectionName string, logger zerolog.Logger) (queue.ColdQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}
	if collectionName == "" {
		return nil, fmt.Errorf("collectionName cannot be empty")
	}
	return &FirestoreColdQueue{
		client:         client,
		logger:         logger,
		collectionName: collectionName,
	}, nil
}

// messagesCollection is a helper to get the subcollection ref
func (s *FirestoreColdQueue) messagesCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.collectionName).Doc(urn.String()).Collection("messages")
}

// Enqueue saves a single message envelope for a specific recipient URN in Firestore.
func (s *FirestoreColdQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	collectionRef := s.messagesCollection(envelope.RecipientID)

	pb := secure.ToProto(envelope)
	if pb == nil {
		s.logger.Warn().Msg("Skipping nil envelope")
		return nil
	}

	storedMsg := &storedMessage{
		QueuedAt: time.Now().UTC(),
		Envelope: pb,
	}

	docRef := collectionRef.Doc(uuid.NewString())
	_, err := docRef.Create(ctx, storedMsg)
	return err
}

// RetrieveMessageBatch fetches the next available batch of queued messages.
func (s *FirestoreColdQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	collectionRef := s.messagesCollection(userURN)

	query := collectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)

	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve message batch: %w", err)
	}

	if len(docSnaps) == 0 {
		return []*routing.QueuedMessage{}, nil
	}

	queuedMessages := make([]*routing.QueuedMessage, 0, len(docSnaps))
	for _, doc := range docSnaps {
		var storedMsg storedMessage
		if err := doc.DataTo(&storedMsg); err != nil {
			s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to unmarshal stored message, skipping")
			continue
		}

		nativeEnv, err := secure.FromProto(storedMsg.Envelope)
		if err != nil {
			s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to convert protobuf to native envelope, skipping")
			continue
		}

		queuedMessages = append(queuedMessages, &routing.QueuedMessage{
			ID:       doc.Ref.ID,
			Envelope: nativeEnv,
		})
	}

	return queuedMessages, nil
}

// Acknowledge permanently deletes a list of messages by their MessageIDs.
func (s *FirestoreColdQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	log := s.logger.With().Str("user", userURN.String()).Logger()
	collectionRef := s.messagesCollection(userURN)

	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	log.Debug().Int("count", len(messageIDs)).Msg("Enqueuing messages for deletion.")

	for _, msgID := range messageIDs {
		docRef := collectionRef.Doc(msgID)
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error().Err(err).Str("doc_id", msgID).Msg("Failed to enqueue document for deletion")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	bulkWriter.End()

	if firstErr != nil {
		return fmt.Errorf("failed to enqueue one or more messages for deletion: %w", firstErr)
	}

	log.Info().Int("count", len(messageIDs)).Msg("Successfully acknowledged (deleted) messages.")
	return nil
}
