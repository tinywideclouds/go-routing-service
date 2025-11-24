/*
File: internal/api/routing_handlers_test.go
Description: Unit tests for the routing service API handlers.
*/
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// defaultBatchLimit matches the handler's default.
const defaultBatchLimit = 50

// --- Mocks ---
type mockIngestionProducer struct {
	mock.Mock
}

func (m *mockIngestionProducer) Publish(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}

// mockMessageQueue implements the queue.MessageQueue interface.
type mockMessageQueue struct {
	mock.Mock
}

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
	return args.Error(0)
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
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}
func (m *mockMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error {
	args := m.Called(ctx, userURN)
	return args.Error(0)
}

// --- Test Setup ---
var (
	testLogger       = slog.New(slog.NewTextHandler(io.Discard, nil))
	authedUserID     = "test-user-id-123"
	authedUserURN, _ = urn.New(urn.SecureMessaging, urn.EntityTypeUser, authedUserID)
	testAuthContext  = middleware.ContextWithUserID(context.Background(), authedUserID)
	errTest          = errors.New("something went wrong")
)

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
	testEnvelope := newTestEnvelope(t)
	testBody, err := json.Marshal(testEnvelope)
	require.NoError(t, err)

	t.Run("Success - 202 Accepted", func(t *testing.T) {
		producer := new(mockIngestionProducer)
		apiHandler := api.NewAPI(producer, nil, testLogger) // Pass nil for queue
		producer.On("Publish", mock.Anything, testEnvelope).Return(nil)

		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.SendHandler(rr, req)

		assert.Equal(t, http.StatusAccepted, rr.Code)
		producer.AssertExpectations(t)
	})

	t.Run("Failure - 401 Unauthorized", func(t *testing.T) {
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		rr := httptest.NewRecorder()

		apiHandler.SendHandler(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("Failure - 400 Bad Request on bad JSON", func(t *testing.T) {
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodPost, "/api/send", strings.NewReader("{bad-json}"))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.SendHandler(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("Failure - 500 Internal Server Error on Publish failure", func(t *testing.T) {
		producer := new(mockIngestionProducer)
		apiHandler := api.NewAPI(producer, nil, testLogger)
		producer.On("Publish", mock.Anything, testEnvelope).Return(errTest)

		req := httptest.NewRequest(http.MethodPost, "/api/send", bytes.NewReader(testBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.SendHandler(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		producer.AssertExpectations(t)
	})
}

func TestGetMessageBatchHandler(t *testing.T) {
	testQueuedMsg := &routing.QueuedMessage{
		ID:       "test-queue-id-123",
		Envelope: newTestEnvelope(t),
	}
	testBatch := []*routing.QueuedMessage{testQueuedMsg}
	expectedJSON, err := json.Marshal(routing.QueuedMessageList{Messages: testBatch})
	require.NoError(t, err)

	t.Run("Success - 200 OK with default limit", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)
		queue.On("RetrieveBatch", mock.Anything, authedUserURN, defaultBatchLimit).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.JSONEq(t, string(expectedJSON), rr.Body.String())
		queue.AssertExpectations(t)
	})

	t.Run("Success - 200 OK with custom limit", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)
		queue.On("RetrieveBatch", mock.Anything, authedUserURN, 10).Return(testBatch, nil)

		req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=10", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		queue.AssertExpectations(t)
	})

	t.Run("Failure - 401 Unauthorized", func(t *testing.T) {
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("Failure - 400 Bad Request on invalid limit", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)

		req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=ten", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
		queue.AssertNotCalled(t, "RetrieveBatch")
	})

	t.Run("Failure - 500 Internal Server Error on Store failure", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)
		queue.On("RetrieveBatch", mock.Anything, authedUserURN, defaultBatchLimit).Return(nil, errTest)

		req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.GetMessageBatchHandler(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		queue.AssertExpectations(t)
	})
}

func TestAcknowledgeMessagesHandler(t *testing.T) {
	ackBody := `{"messageIds": ["id-1", "id-2"]}`
	expectedIDs := []string{"id-1", "id-2"}

	t.Run("Success - 204 No Content", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)
		queue.On("Acknowledge", mock.Anything, authedUserURN, expectedIDs).Return(nil)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.AcknowledgeMessagesHandler(rr, req)

		assert.Equal(t, http.StatusNoContent, rr.Code)

		apiHandler.Wait() // Wait for background goroutine
		queue.AssertExpectations(t)
	})

	t.Run("Failure - 401 Unauthorized", func(t *testing.T) {
		apiHandler := api.NewAPI(nil, nil, testLogger)
		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		rr := httptest.NewRecorder()

		apiHandler.AcknowledgeMessagesHandler(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})

	t.Run("Failure - 400 Bad Request on bad JSON", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader("{bad-json}"))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.AcknowledgeMessagesHandler(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
		queue.AssertNotCalled(t, "Acknowledge")
	})

	t.Run("Success - Store failure is handled in background", func(t *testing.T) {
		queue := new(mockMessageQueue)
		apiHandler := api.NewAPI(nil, queue, testLogger)
		queue.On("Acknowledge", mock.Anything, authedUserURN, expectedIDs).Return(errTest)

		req := httptest.NewRequest(http.MethodPost, "/api/messages/ack", strings.NewReader(ackBody))
		req = req.WithContext(testAuthContext)
		rr := httptest.NewRecorder()

		apiHandler.AcknowledgeMessagesHandler(rr, req)

		assert.Equal(t, http.StatusNoContent, rr.Code) // Client gets 204

		apiHandler.Wait()           // Wait for background goroutine
		queue.AssertExpectations(t) // Verify the mock was called
	})
}
/*
File: internal/api/routing_handlers_test.go
Description: Unit tests for the routing service API handlers.
*/
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	testLogger       = slog.New(slog.NewTextHandler(io.Discard, nil))
	
	// Identity
	authedUserID     = "urn:auth:google:123"
	authedUserURN, _ = urn.Parse(authedUserID)
	
	// Handle
	authedHandle     = "urn:lookup:email:test@test.com"
	authedHandleURN, _ = urn.Parse(authedHandle)

	// Contexts
	ctxWithIdentity = middleware.ContextWithUser(context.Background(), authedUserID, "", "")
	ctxWithHandle   = middleware.ContextWithUser(context.Background(), authedUserID, authedHandle, "")
	
	errTest          = errors.New("something went wrong")
)

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