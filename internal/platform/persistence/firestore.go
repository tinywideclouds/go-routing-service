/*
File: internal/platform/persistence/firestore.go
Description: REFACTORED to implement the new "Reliable Dumb Queue" interface.
This version generates internal UUIDs for messages and uses the new
native types and facades.
*/
package persistence

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid" // NEW: Import UUID
	routing2 "github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/rs/zerolog"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

const (
	usersCollection    = "user-messages"
	messagesCollection = "messages"
)

// storedMessage is the wrapper struct we will store in Firestore.
// It adds the server-side timestamp needed for ordering the queue.
type storedMessage struct {
	QueuedAt time.Time                `firestore:"queued_at"`
	Envelope *secure.SecureEnvelopePb `firestore:"envelope"`
}

// FirestoreStore implements the routing.MessageStore interface using Google Cloud Firestore.
type FirestoreStore struct {
	client *firestore.Client
	logger zerolog.Logger
}

// NewFirestoreStore is the constructor for the FirestoreStore.
func NewFirestoreStore(client *firestore.Client, logger zerolog.Logger) (routing2.MessageStore, error) {
	if client == nil {
		return nil, fmt.Errorf("firestore client cannot be nil")
	}
	return &FirestoreStore{
		client: client,
		logger: logger,
	}, nil
}

// StoreMessages saves a slice of message envelopes for a specific recipient URN in Firestore.
// REFACTORED: This now generates a unique UUID for each message as its Document ID.
func (s *FirestoreStore) StoreMessages(ctx context.Context, recipient urn.URN, envelopes []*secure.SecureEnvelope) error {
	recipientKey := recipient.String()
	collectionRef := s.client.Collection(usersCollection).Doc(recipientKey).Collection(messagesCollection)

	// Use RunTransaction for atomic batch writes.
	return s.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for _, env := range envelopes {
			// REFACTORED: Convert to protobuf using the facade
			pb := secure.ToProto(env)
			if pb == nil {
				s.logger.Warn().Msg("Skipping nil envelope")
				continue
			}

			// NEW: Wrap the envelope in 'storedMessage' with the server-side timestamp.
			storedMsg := &storedMessage{
				QueuedAt: time.Now().UTC(),
				Envelope: pb,
			}

			// NEW: Generate a new UUID. This is the internal-only queue ID.
			// The client *never* provides this.
			docRef := collectionRef.Doc(uuid.NewString())
			if err := tx.Set(docRef, storedMsg); err != nil {
				return err // Transaction will be rolled back.
			}
		}
		return nil
	})
}

// RetrieveMessageBatch fetches the next available batch of queued messages.
// REFACTORED: This now returns []*routing.QueuedMessage.
func (s *FirestoreStore) RetrieveMessageBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	collectionRef := s.client.Collection(usersCollection).Doc(userURN.String()).Collection(messagesCollection)

	// Query for the oldest messages, ordered by 'queued_at'.
	query := collectionRef.OrderBy("queued_at", firestore.Asc).Limit(limit)

	docSnaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve message batch: %w", err)
	}

	if len(docSnaps) == 0 {
		// REFACTORED: Return empty list of the new type
		return []*routing.QueuedMessage{}, nil
	}

	// REFACTORED: Create a slice of the new wrapper type
	queuedMessages := make([]*routing.QueuedMessage, 0, len(docSnaps))
	for _, doc := range docSnaps {
		var storedMsg storedMessage
		if err := doc.DataTo(&storedMsg); err != nil {
			s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to unmarshal stored message, skipping")
			continue
		}

		// Convert from Protobuf back to the native Go struct
		nativeEnv, err := secure.FromProto(storedMsg.Envelope)
		if err != nil {
			s.logger.Error().Err(err).Str("doc_id", doc.Ref.ID).Msg("Failed to convert protobuf to native envelope, skipping")
			continue
		}

		// NEW: Create the QueuedMessage wrapper, using the
		// Firestore Document ID as the public-facing 'ID' for the ACK.
		queuedMessages = append(queuedMessages, &routing.QueuedMessage{
			ID:       doc.Ref.ID, // This is the UUID we generated
			Envelope: nativeEnv,
		})
	}

	return queuedMessages, nil
}

// AcknowledgeMessages permanently deletes a list of messages by their MessageIDs.
// REFACTORED: Renamed from DeleteMessages. The logic remains the same.
func (s *FirestoreStore) AcknowledgeMessages(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	log := s.logger.With().Str("user", userURN.String()).Logger()
	collectionRef := s.client.Collection(usersCollection).Doc(userURN.String()).Collection(messagesCollection)

	// Use a BulkWriter. This is the correct, scalable way to handle
	// deleting an arbitrary list of IDs.
	bulkWriter := s.client.BulkWriter(ctx)
	var firstErr error

	log.Debug().Int("count", len(messageIDs)).Msg("Enqueuing messages for deletion.")

	for _, msgID := range messageIDs {
		docRef := collectionRef.Doc(msgID)
		// bulkWriter.Delete() returns an error if it fails to *enqueue*.
		// We capture the first error, but continue trying to enqueue the rest.
		if _, err := bulkWriter.Delete(docRef); err != nil {
			log.Error().Err(err).Str("doc_id", msgID).Msg("Failed to enqueue document for deletion")
			if firstErr == nil {
				firstErr = err // Capture the first enqueue error
			}
		}
	}

	// End() calls Flush() and returns void. It blocks until all writes are done.
	// Errors during the *actual deletion* are not returned here, they are
	// handled by the BulkWriter's ErrorHandler (which we haven't set,
	// so it defaults to logging).
	bulkWriter.End()

	// We return the first *enqueue* error, if any.
	if firstErr != nil {
		return fmt.Errorf("failed to enqueue one or more messages for deletion: %w", firstErr)
	}

	log.Info().Int("count", len(messageIDs)).Msg("Successfully acknowledged (deleted) messages.")
	return nil
}
