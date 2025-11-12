// --- File: internal/platform/queue/cold_queue_firestore.go ---
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"github.com/tinywideclouds/go-routing-service/internal/queue" // Import new interfaces

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// storedMessage is the wrapper struct we will store in Firestore.
// It includes the envelope and a timestamp for ordering.
type storedMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreColdQueue implements the queue.ColdQueue interface using Google Cloud Firestore.
// It provides durable, long-term storage for messages.
type FirestoreColdQueue struct {
	client         *firestore.Client
	logger         *slog.Logger
	collectionName string // Configured collection name
}

// NewFirestoreColdQueue is the constructor for the FirestoreColdQueue.
// It requires a valid Firestore client and a non-empty collection name.
func NewFirestoreColdQueue(client *firestore.Client, collectionName string, logger *slog.Logger) (queue.ColdQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}
	if collectionName == "" {
		return nil, fmt.Errorf("collectionName cannot be empty")
	}
	return &FirestoreColdQueue{
		client:         client,
		logger:         logger.With("component", "firestore_cold_queue", "collection", collectionName),
		collectionName: collectionName,
	}, nil
}

// messagesCollection is a helper to get the subcollection ref for a specific user.
// Messages are stored at: /<collectionName>/{userURN}/messages/{messageID}
func (s *FirestoreColdQueue) messagesCollection(urn urn.URN) *firestore.CollectionRef {
	return s.client.Collection(s.collectionName).Doc(urn.String()).Collection("messages")
}

// Enqueue saves a single message envelope for a specific recipient URN in Firestore.
// A new UUID is generated for the document ID.
func (s *FirestoreColdQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	collectionRef := s.messagesCollection(envelope.RecipientID)
	log := s.logger.With("user", envelope.RecipientID.String())

	pb := secure.ToProto(envelope)
	if pb == nil {
		log.Warn("Skipping nil envelope")
		return nil
	}

	storedMsg := &storedMessage{
		QueuedAt: time.Now().UTC(),
		Envelope: pb,
	}

	docRef := collectionRef.Doc(uuid.NewString())
	_, err := docRef.Create(ctx, storedMsg)
	if err != nil {
		log.Error("Failed to enqueue message to cold queue", "err", err)
		return err
	}
	log.Debug("Enqueued message to cold queue", "doc_id", docRef.ID)
	return nil
}

// RetrieveBatch fetches the next available batch of queued messages, ordered
// oldest to newest.
func (s *FirestoreColdQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	collectionRef := s.messagesCollection(userURN)
	log := s.logger.With("user", userURN.String())

	query := collectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)

	log.Debug("Retrieving cold message batch", "limit", limit)
	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		log.Error("Failed to retrieve cold message batch", "err", err)
		return nil, fmt.Errorf("failed to retrieve message batch: %w", err)
	}

	if len(docSnaps) == 0 {
		log.Debug("No cold messages found for user")
		return []*routing.QueuedMessage{}, nil
	}

	log.Debug("Retrieved cold message batch", "count", len(docSnaps))
	queuedMessages := make([]*routing.QueuedMessage, 0, len(docSnaps))
	for _, doc := range docSnaps {
		var storedMsg storedMessage
		if err := doc.DataTo(&storedMsg); err != nil {
			log.Error("Failed to unmarshal stored cold message, skipping", "err", err, "doc_id", doc.Ref.ID)
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
	}

	return queuedMessages, nil
}

// Acknowledge permanently deletes a list of messages by their MessageIDs
// using a Firestore BulkWriter for efficiency.
func (s *FirestoreColdQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	log := s.logger.With("user", userURN.String())
	collectionRef := s.messagesCollection(userURN)

	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	log.Debug("Enqueuing cold messages for deletion", "count", len(messageIDs))

	for _, msgID := range messageIDs {
		docRef := collectionRef.Doc(msgID)
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error("Failed to enqueue cold document for deletion", "err", err, "doc_id", msgID)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	bulkWriter.End() // Flushes any remaining writes

	if firstErr != nil {
		log.Error("Failed to enqueue one or more cold messages for deletion", "err", firstErr)
		return fmt.Errorf("failed to enqueue one or more messages for deletion: %w", firstErr)
	}

	log.Info("Successfully acknowledged (deleted) cold messages", "count", len(messageIDs))
	return nil
}
