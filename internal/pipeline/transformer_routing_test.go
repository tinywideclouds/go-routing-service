/*
File: internal/pipeline/transformer_routing_test.go
Description: REFACTORED to fix the 'Failure_-_Invalid_URN' test
by providing a payload that is guaranteed to fail URN parsing.
*/
package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

func TestEnvelopeTransformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	// REFACTORED: Create a valid "dumb" envelope
	recipientURN, err := urn.Parse("urn:sm:user:user-bob")
	require.NoError(t, err)

	validEnvelope := secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("test"),
	}
	// The transformer expects a Protobuf JSON payload.
	validPayload, err := protojson.Marshal(secure.ToProto(&validEnvelope))
	require.NoError(t, err, "Setup: failed to marshal valid envelope")

	// --- THIS IS THE FIX ---
	// REFACTORED: This payload will now correctly fail the
	// "entity type must not be empty" check in urn.Parse().
	invalidPayload := []byte(`{"recipientId": "urn:sm::invalid-id"}`)
	// --- END FIX ---

	testCases := []struct {
		name                  string
		inputMessage          *messagepipeline.Message
		expectedEnvelope      *secure.SecureEnvelope // REFACTORED: Updated type
		expectedSkip          bool
		expectError           bool
		expectedErrorContains string
	}{
		{
			name: "Success - Valid Payload",
			inputMessage: &messagepipeline.Message{
				MessageData: messagepipeline.MessageData{
					ID:      "msg-123",
					Payload: validPayload,
				},
			},
			expectedEnvelope: &validEnvelope,
			expectedSkip:     false,
			expectError:      false,
		},
		{
			name: "Failure - Malformed JSON Payload",
			inputMessage: &messagepipeline.Message{
				MessageData: messagepipeline.MessageData{
					ID:      "msg-456",
					Payload: []byte("{ not-valid-json }"),
				},
			},
			expectedEnvelope:      nil,
			expectedSkip:          true,
			expectError:           true,
			expectedErrorContains: "failed to unmarshal secure envelope",
		},
		{
			name: "Failure - Invalid URN (Validation Fail)",
			inputMessage: &messagepipeline.Message{
				MessageData: messagepipeline.MessageData{
					ID:      "msg-789",
					Payload: invalidPayload, // Now using the corrected payload
				},
			},
			expectedEnvelope:      nil,
			expectedSkip:          true,
			expectError:           true,
			expectedErrorContains: "failed to convert/validate envelope", // Error from secure.FromProto
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			actualEnvelope, actualSkip, actualErr := pipeline.EnvelopeTransformer(ctx, tc.inputMessage)

			// Assert
			assert.Equal(t, tc.expectedEnvelope, actualEnvelope)
			assert.Equal(t, tc.expectedSkip, actualSkip)

			if tc.expectError {
				require.Error(t, actualErr)
				assert.Contains(t, actualErr.Error(), tc.expectedErrorContains)
			} else {
				require.NoError(t, actualErr)
			}
		})
	}
}
