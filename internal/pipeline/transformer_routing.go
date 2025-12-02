// --- File: internal/pipeline/transformer_routing.go ---
package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// EnvelopeTransformer is a dataflow Transformer stage that safely unmarshals
// a raw message payload from the message bus into a validated secure.SecureEnvelope.
//
// It performs two steps:
//  1. Unmarshals the raw JSON payload into a Protobuf struct.
//  2. Converts the Protobuf struct into the native Go facade struct, which
//     includes critical validation (e.g., ensuring URNs are valid).
//
// If either step fails, the message is marked for skipping (and NACK'ing).
func EnvelopeTransformer(_ context.Context, msg *messagepipeline.Message) (*secure.SecureEnvelope, bool, error) {
	envelopePB := &secure.SecureEnvelopePb{}

	err := protojson.Unmarshal(msg.Payload, envelopePB)
	if err != nil {
		// If unmarshalling fails, we skip the message and return an error
		// so the StreamingService can Nack it.
		slog.Error("Failed to unmarshal secure envelope", "err", err, "msg_id", msg.ID, "payload", string(msg.Payload))
		return nil, true, fmt.Errorf("failed to unmarshal secure envelope from message %s: %w", msg.ID, err)
	}

	// Use the 'secure.FromProto' facade.
	// This facade contains the validation logic (e.g., checking for a valid URN).
	envelope, err := secure.FromProto(envelopePB)
	if err != nil {
		// If we can't convert to a 'real' Envelope (e.g., invalid URN), fail.
		slog.Error("Failed to convert/validate envelope", "err", err, "msg_id", msg.ID)
		return nil, true, fmt.Errorf("failed to convert/validate envelope from message %s: %w", msg.ID, err)
	}

	// On success, we pass the structured envelope to the next stage (the Processor).
	return envelope, false, nil
}
