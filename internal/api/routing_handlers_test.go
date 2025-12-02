/*
File: internal/api/routing_handlers_test.go
Description: Unit tests for the routing service API handlers.
*/
package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/internal/api"
)

const defaultBatchLimit = 50

// --- Mocks (Same as before) ---
type mockIngestionProducer struct{ mock.Mock }

func (m *mockIngestionProducer) Publish(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}

type mockMessageQueue struct{ mock.Mock }

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	var result []*routing.QueuedMessage
	if val, ok := args.Get(0).([]*routing.QueuedMessage); ok {
		result = val
	}
	return result, args.Error(1)
}
func (m *mockMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return m.Called(ctx, userURN, messageIDs).Error(0)
}
func (m *mockMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	return m.Called(ctx, userURN).Error(0)
}

// --- Test Setup ---
var (
	testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Identity
	authedUserID     = "urn:auth:google:123"
	authedUserURN, _ = urn.Parse(authedUserID)

	// Handle
	authedHandle       = "urn:lookup:email:test@test.com"
	authedHandleURN, _ = urn.Parse(authedHandle)

	// Contexts
	ctxWithIdentity = middleware.ContextWithUser(context.Background(), authedUserID, "", "")
	ctxWithHandle   = middleware.ContextWithUser(context.Background(), authedUserID, authedHandle, "")
)

func newTestEnvelope(t *testing.T) *secure.SecureEnvelope {
	t.Helper()
	recipientURN, err := urn.Parse("urn:contacts:user:recipient-bob")
	require.NoError(t, err)
	return &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("test-data"),
	}
}

// --- Test Cases ---

func TestGetMessageBatchHandler_HandleIsKing(t *testing.T) {
	testQueuedMsg := &routing.QueuedMessage{
		ID:       "test-queue-id-123",
		Envelope: newTestEnvelope(t),
	}
	testBatch := []*routing.QueuedMessage{testQueuedMsg}

	t.Run("Success - Uses HANDLE when present", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)

		// EXPECTATION: RetrieveBatch is called with the HANDLE URN
		queue.On("RetrieveBatch", mock.Anything, authedHandleURN, defaultBatchLimit).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(ctxWithHandle) // Context has both, should prefer Handle
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		queue.AssertExpectations(t)
	})

	t.Run("Success - Fallback to IDENTITY when handle missing", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)

		// EXPECTATION: RetrieveBatch is called with the IDENTITY URN
		queue.On("RetrieveBatch", mock.Anything, authedUserURN, defaultBatchLimit).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(ctxWithIdentity) // Context has only Identity
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		queue.AssertExpectations(t)
	})
}

func TestAcknowledgeMessagesHandler_HandleIsKing(t *testing.T) {
	ackBody := `{"messageIds": ["id-1"]}`
	expectedIDs := []string{"id-1"}

	t.Run("Success - Acks using HANDLE when present", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)

		// EXPECTATION: Acknowledge called with HANDLE URN
		queue.On("Acknowledge", mock.Anything, authedHandleURN, expectedIDs).Return(nil)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		req = req.WithContext(ctxWithHandle)
		rr := httptest.NewRecorder()

		apiHandler.AcknowledgeMessagesHandler(rr, req)

		assert.Equal(t, http.StatusNoContent, rr.Code)
		apiHandler.Wait()
		queue.AssertExpectations(t)
	})
}
