// --- File: internal/pipeline/transformer_routing_test.go ---
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

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

func TestEnvelopeTransformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	recipientURN, err := urn.Parse("urn:contacts:user:user-bob")
	require.NoError(t, err)

	validEnvelope := secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("test"),
	}
	// The transformer expects a Protobuf JSON payload.
	validPayload, err := protojson.Marshal(secure.ToProto(&validEnvelope))
	require.NoError(t, err, "Setup: failed to marshal valid envelope")

	// This payload will correctly fail the "entity type must not be empty"
	// check in urn.Parse() when secure.FromProto() is called.
	invalidPayload := []byte(`{"recipientId": "urn:message::invalid-id"}`)

	testCases := []struct {
		name                  string
		inputMessage          *messagepipeline.Message
		expectedEnvelope      *secure.SecureEnvelope
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
					Payload: invalidPayload,
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
