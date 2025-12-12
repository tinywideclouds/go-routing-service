// --- File: internal/pipeline/transformer_routing.go ---
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

func EnvelopeTransformer(_ context.Context, msg *messagepipeline.Message) (*secure.SecureEnvelope, bool, error) {
	var envelope secure.SecureEnvelope

	// REFACTORED: Use standard JSON unmarshal (uses Facade pattern)
	if err := json.Unmarshal(msg.Payload, &envelope); err != nil {
		slog.Error("Failed to unmarshal secure envelope", "err", err, "msg_id", msg.ID)
		return nil, true, fmt.Errorf("failed to unmarshal secure envelope from message %s: %w", msg.ID, err)
	}

	// Additional validation checks if needed (Facade handles URN parsing)
	if envelope.RecipientID.String() == "" {
		return nil, true, fmt.Errorf("envelope missing recipient ID")
	}

	slog.Debug("Transformed envelope",
		"msg_id", msg.ID,
		"recipient", envelope.RecipientID.String(),
		"priority", envelope.Priority,
	)

	return &envelope, false, nil
}
