// --- File: internal/queue/message_queue_test.go ---
package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	// Platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks ---

type mockHotQueue struct {
	mock.Mock
}

// REFACTORED: Accept messageID
func (m *mockHotQueue) Enqueue(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, messageID, envelope)
	return args.Error(0)
}
func (m *mockHotQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	if res, ok := args.Get(0).([]*routing.QueuedMessage); ok {
		return res, args.Error(1)
	}
	return nil, args.Error(1)
}
func (m *mockHotQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}
func (m *mockHotQueue) MigrateToCold(ctx context.Context, userURN urn.URN, destination ColdQueue) error {
	args := m.Called(ctx, userURN, destination)
	return args.Error(0)
}

type mockColdQueue struct {
	mock.Mock
}

// REFACTORED: Accept messageID
func (m *mockColdQueue) Enqueue(ctx context.Context, messageID string, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, messageID, envelope)
	return args.Error(0)
}
func (m *mockColdQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	if res, ok := args.Get(0).([]*routing.QueuedMessage); ok {
		return res, args.Error(1)
	}
	return nil, args.Error(1)
}
func (m *mockColdQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}

// --- Fixture ---

var (
	testURN, _ = urn.Parse("urn:contacts:user:test")
	testEnv    = &secure.SecureEnvelope{RecipientID: testURN}
	testMsg    = &routing.QueuedMessage{ID: "msg-1", Envelope: testEnv}
	errTest    = errors.New("queue error")
	testID     = "msg-uuid-123" // Fixed ID for tests
)

type testFixture struct {
	hot  *mockHotQueue
	cold *mockColdQueue
	comp MessageQueue
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func setup(t *testing.T) *testFixture {
	hot := new(mockHotQueue)
	cold := new(mockColdQueue)
	comp, err := NewCompositeMessageQueue(hot, cold, newTestLogger())
	require.NoError(t, err)

	return &testFixture{
		hot:  hot,
		cold: cold,
		comp: comp,
	}
}

// --- Tests ---

func TestEnqueueHot_Success(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Hot queue succeeds
	fx.hot.On("Enqueue", ctx, testID, testEnv).Return(nil)

	err := fx.comp.EnqueueHot(ctx, testID, testEnv)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
	fx.cold.AssertNotCalled(t, "Enqueue")
}

func TestEnqueueHot_Fallback(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Hot queue fails
	fx.hot.On("Enqueue", ctx, testID, testEnv).Return(errTest)
	// Cold queue succeeds, MUST be called with same ID
	fx.cold.On("Enqueue", ctx, testID, testEnv).Return(nil)

	err := fx.comp.EnqueueHot(ctx, testID, testEnv)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
	fx.cold.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
}

func TestEnqueueHot_Fatal(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Hot queue fails
	fx.hot.On("Enqueue", ctx, testID, testEnv).Return(errTest)
	// Cold queue also fails
	fx.cold.On("Enqueue", ctx, testID, testEnv).Return(errTest)

	err := fx.comp.EnqueueHot(ctx, testID, testEnv)
	require.Error(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
	fx.cold.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
}

func TestEnqueueCold(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	fx.cold.On("Enqueue", ctx, testID, testEnv).Return(nil)

	err := fx.comp.EnqueueCold(ctx, testID, testEnv)
	require.NoError(t, err)

	fx.cold.AssertCalled(t, "Enqueue", ctx, testID, testEnv)
	fx.hot.AssertNotCalled(t, "Enqueue")
}

func TestRetrieveBatch_HotHit(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	hotResult := []*routing.QueuedMessage{testMsg}
	// 1. Hot returns 1 message (Limit is 10)
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return(hotResult, nil)

	// 2. Composite logic detects partial batch (1 < 10) and asks Cold for remaining (9)
	// We mock this to return empty so the test asserts we got the Hot message back.
	fx.cold.On("RetrieveBatch", ctx, testURN, 9).Return([]*routing.QueuedMessage{}, nil)

	res, err := fx.comp.RetrieveBatch(ctx, testURN, 10)
	require.NoError(t, err)
	assert.Equal(t, hotResult, res)

	fx.hot.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
	fx.cold.AssertCalled(t, "RetrieveBatch", ctx, testURN, 9)
}

func TestRetrieveBatch_ColdHit(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	coldResult := []*routing.QueuedMessage{testMsg}
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return([]*routing.QueuedMessage{}, nil)
	fx.cold.On("RetrieveBatch", ctx, testURN, 10).Return(coldResult, nil)

	res, err := fx.comp.RetrieveBatch(ctx, testURN, 10)
	require.NoError(t, err)
	assert.Equal(t, coldResult, res)

	fx.hot.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
	fx.cold.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
}

func TestRetrieveBatch_HotErrorFallback(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	coldResult := []*routing.QueuedMessage{testMsg}
	// Hot queue returns error
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return(nil, errTest)
	// Cold queue returns messages
	fx.cold.On("RetrieveBatch", ctx, testURN, 10).Return(coldResult, nil)

	res, err := fx.comp.RetrieveBatch(ctx, testURN, 10)
	require.NoError(t, err)
	assert.Equal(t, coldResult, res)

	fx.hot.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
	fx.cold.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
}

func TestAcknowledge(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()
	ids := []string{"msg-1"}

	fx.hot.On("Acknowledge", ctx, testURN, ids).Return(nil)
	fx.cold.On("Acknowledge", ctx, testURN, ids).Return(nil)

	err := fx.comp.Acknowledge(ctx, testURN, ids)
	require.NoError(t, err)

	// Note: We removed Eventually logic for unit testing as these mocks are synchronous
	fx.hot.AssertCalled(t, "Acknowledge", ctx, testURN, ids)
	fx.cold.AssertCalled(t, "Acknowledge", ctx, testURN, ids)
}

func TestMigrateHotToCold(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	fx.hot.On("MigrateToCold", ctx, testURN, fx.cold).Return(nil)

	err := fx.comp.MigrateHotToCold(ctx, testURN)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "MigrateToCold", ctx, testURN, fx.cold)
}

func TestEnqueueHot_Fallback_Ephemeral(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	ephemeralEnv := &secure.SecureEnvelope{
		RecipientID: testURN,
		IsEphemeral: true, // FLAG SET
	}

	// 1. Hot queue fails
	fx.hot.On("Enqueue", ctx, testID, ephemeralEnv).Return(errTest)

	// Act
	err := fx.comp.EnqueueHot(ctx, testID, ephemeralEnv)

	// Assert
	require.NoError(t, err) // Should succeed (swallow error)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testID, ephemeralEnv)
	// CRITICAL: Verify Cold was NOT called
	fx.cold.AssertNotCalled(t, "Enqueue", ctx, mock.Anything, mock.Anything)
}

func TestEnqueueHot_Fallback_Persistent(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	persistentEnv := &secure.SecureEnvelope{
		RecipientID: testURN,
		IsEphemeral: false,
	}

	// 1. Hot queue fails
	fx.hot.On("Enqueue", ctx, testID, persistentEnv).Return(errTest)
	// 2. Cold queue succeeds with SAME ID
	fx.cold.On("Enqueue", ctx, testID, persistentEnv).Return(nil)

	err := fx.comp.EnqueueHot(ctx, testID, persistentEnv)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testID, persistentEnv)
	fx.cold.AssertCalled(t, "Enqueue", ctx, testID, persistentEnv)
}
