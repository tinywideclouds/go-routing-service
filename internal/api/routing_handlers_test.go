/*
File: internal/api/routing_handlers_test.go
Description: REFACTORED to test the new API.
- Mock store is updated.
- DELETED: Tests for GetDigestHandler and GetHistoryHandler.
- REFACTORED: TestSendHandler no longer tests for 403 (spoofing).
- ADDED: Tests for GetMessageBatchHandler and AcknowledgeMessagesHandler.
*/
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/api"

	// REFACTORED: Use new platform packages

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

const defaultBatchLimit = 4

// --- Mocks ---
type mockIngestionProducer struct {
	mock.Mock
}

// REFACTORED: Mock uses new envelope type
func (m *mockIngestionProducer) Publish(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}

// REFACTORED: Mock store's interface matches the new routing.MessageStore
type mockMessageStore struct {
	mock.Mock
}

func (m *mockMessageStore) StoreMessages(ctx context.Context, recipient urn.URN, envelopes []*secure.SecureEnvelope) error {
	args := m.Called(ctx, recipient, envelopes)
	return args.Error(0)
}

// NEW
func (m *mockMessageStore) RetrieveMessageBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	var result []*routing.QueuedMessage
	if val, ok := args.Get(0).([]*routing.QueuedMessage); ok {
		result = val
	}
	return result, args.Error(1)
}

// REFACTORED: Renamed from DeleteMessages
func (m *mockMessageStore) AcknowledgeMessages(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}

// --- Test Setup ---
var (
	testLogger       = zerolog.Nop()
	authedUserID     = "test-user-id-123"
	authedUserURN, _ = urn.New(urn.SecureMessaging, urn.EntityTypeUser, authedUserID)
	testAuthContext  = middleware.ContextWithUserID(context.Background(), authedUserID)
	testErr          = errors.New("something went wrong")
)

// REFACTORED: Helper creates the new "dumb" envelope
func newTestEnvelope(t *testing.T) *secure.SecureEnvelope {
	t.Helper()
	recipientURN, err := urn.Parse("urn:sm:user:recipient-bob")
	require.NoError(t, err)

	return &secure.SecureEnvelope{
		RecipientID:   recipientURN,
		EncryptedData: []byte("test-data"),
	}
}

// --- Test Cases ---

func TestSendHandler(t *testing.T) {
	// REFACTORED: Use new "dumb" envelope. No SenderID.
	testEnvelope := newTestEnvelope(t)
	// Marshal using the facade's JSON marshaler
	testBody, err := json.Marshal(testEnvelope)
	require.NoError(t, err)

	t.Run("Success - 202 Accepted", func(t *testing.T) {
		// Arrange
		producer := new(mockIngestionProducer)
		apiHandler := api.NewAPI(producer, nil, testLogger)
		producer.On("Publish", mock.Anything, testEnvelope).Return(nil)

		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.SendHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusAccepted, rr.Code)
		producer.AssertExpectations(t)
	})

	// --- DELETED TEST ---
	// t.Run("Failure - 403 Forbidden on Sender Spoofing")
	// This test is now obsolete, as this check was removed.
	// --- DELETED ---

	t.Run("Failure - 401 Unauthorized", func(t *testing.T) {
		// Arrange
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		// NO auth context
		rr := httptest.NewRecorder()

		// Act
		apiHandler.SendHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("Failure - 400 Bad Request on bad JSON", func(t *testing.T) {
		// Arrange
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodPost, "/api/send", strings.NewReader("{bad-json}"))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.SendHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("Failure - 500 Internal Server Error on Publish failure", func(t *testing.T) {
		// Arrange
		producer := new(mockIngestionProducer)
		apiHandler := api.NewAPI(producer, nil, testLogger)
		producer.On("Publish", mock.Anything, testEnvelope).Return(testErr)

		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.SendHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		producer.AssertExpectations(t)
	})
}

// --- DELETED TESTS ---
//
// TestGetDigestHandler
// TestGetHistoryHandler
//
// --- DELETED ---

// --- NEW TESTS ---

func TestGetMessageBatchHandler(t *testing.T) {
	// Arrange: Create the expected response data
	testQueuedMsg := &routing.QueuedMessage{
		ID:       "test-queue-id-123",
		Envelope: newTestEnvelope(t),
	}
	testBatch := []*routing.QueuedMessage{testQueuedMsg}

	// This is the JSON we expect our facade to produce
	expectedJSON, err := json.Marshal(routing.QueuedMessageList{Messages: testBatch})
	require.NoError(t, err)

	t.Run("Success - 200 OK with default limit", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)
		// Mock the store to expect the default limit
		store.On("RetrieveMessageBatch", mock.Anything, authedUserURN, defaultBatchLimit).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.GetMessageBatchHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.JSONEq(t, string(expectedJSON), rr.Body.String())
		store.AssertExpectations(t)
	})

	t.Run("Success - 200 OK with custom limit", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)
		store.On("RetrieveMessageBatch", mock.Anything, authedUserURN, 10).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=10", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.GetMessageBatchHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusOK, rr.Code)
		store.AssertExpectations(t)
	})

	t.Run("Failure - 400 Bad Request on invalid limit", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)

		req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=ten", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.GetMessageBatchHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusBadRequest, rr.Code)
		store.AssertNotCalled(t, "RetrieveMessageBatch")
	})

	t.Run("Failure - 500 Internal Server Error on Store failure", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)
		store.On("RetrieveMessageBatch", mock.Anything, authedUserURN, defaultBatchLimit).Return(nil, testErr)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.GetMessageBatchHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		store.AssertExpectations(t)
	})
}

func TestAcknowledgeMessagesHandler(t *testing.T) {
	ackBody := `{"messageIds": ["id-1", "id-2"]}`
	expectedIDs := []string{"id-1", "id-2"}

	t.Run("Success - 204 No Content", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)
		store.On("AcknowledgeMessages", mock.Anything, authedUserURN, expectedIDs).Return(nil)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.AcknowledgeMessagesHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusNoContent, rr.Code)

		// Wait for the background goroutine to finish
		apiHandler.Wait()
		store.AssertExpectations(t)
	})

	t.Run("Failure - 400 Bad Request on bad JSON", func(t *testing.T) {
		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader("{bad-json}"))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.AcknowledgeMessagesHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusBadRequest, rr.Code)
		store.AssertNotCalled(t, "AcknowledgeMessages")
	})

	t.Run("Success - Store failure is handled in background", func(t *testing.T) {
		// This test ensures that even if the store fails, the client
		// still gets a 204, and the error is simply logged.

		// Arrange
		store := new(mockMessageStore)
		apiHandler := api.NewAPI(nil, store, testLogger)
		store.On("AcknowledgeMessages", mock.Anything, authedUserURN, expectedIDs).Return(testErr)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		// Act
		apiHandler.AcknowledgeMessagesHandler(rr, req)

		// Assert
		assert.Equal(t, http.StatusNoContent, rr.Code) // Client gets 204

		// Wait for the background goroutine to finish
		apiHandler.Wait()
		store.AssertExpectations(t) // Verify the mock was called (and returned an error)
	})
}
