/*
File: internal/pipeline/transformer_routing.go
Description: REFACTORED to use the new 'secure.SecureEnvelope'
facade and types.
*/
package pipeline

import (
	"context"
	"fmt"
	"log/slog" // IMPORTED

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform 'secure' package and 'gen-platform' types
	securev1 "github.com/tinywideclouds/gen-platform/go/types/secure/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// EnvelopeTransformer is a dataflow Transformer stage that safely unmarshals
// a raw message payload into a structured secure.SecureEnvelope.
// REFACTORED: Now returns *secure.SecureEnvelope
func EnvelopeTransformer(_ context.Context, msg *messagepipeline.Message) (*secure.SecureEnvelope, bool, error) {
	// REFACTORED: Use new proto type
	envelopePB := &securev1.SecureEnvelopePb{}

	err := protojson.Unmarshal(msg.Payload, envelopePB)
	if err != nil {
		// If unmarshalling fails, we skip the message and return an error
		// so the StreamingService can Nack it.
		// ADDED: Log the poison message payload for debugging.
		slog.Error("Failed to unmarshal secure envelope", "err", err, "msg_id", msg.ID, "payload", string(msg.Payload))
		return nil, true, fmt.Errorf("failed to unmarshal secure envelope from message %s: %w", msg.ID, err)
	}

	// REFACTORED: Use the new 'secure.FromProto' facade.
	// This facade contains the validation logic (e.g., checking for a valid URN).
	envelope, err := secure.FromProto(envelopePB)
	if err != nil {
		// If we can't convert to a 'real' Envelope (e.g., invalid URN), fail.
		// ADDED: Log the validation/conversion failure.
		slog.Error("Failed to convert/validate envelope", "err", err, "msg_id", msg.ID)
		return nil, true, fmt.Errorf("failed to convert/validate envelope from message %s: %w", msg.ID, err)
	}

	// DELETED: Old validation is no longer needed as the facade handles it.
	// if envelope.SenderID.IsZero() || envelope.RecipientID.IsZero() { ... }

	// On success, we pass the structured envelope to the next stage (the Processor).
	return envelope, false, nil
}
