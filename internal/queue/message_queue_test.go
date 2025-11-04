/*
File: internal/queue/composite_queue_test.go
Description: NEW FILE. Unit test for the CompositeMessageQueue.
*/
package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
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

func (m *mockHotQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
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

func (m *mockColdQueue) Enqueue(ctx context.Context, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, envelope)
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
	testURN, _ = urn.Parse("urn:sm:user:test")
	testEnv    = &secure.SecureEnvelope{RecipientID: testURN}
	testMsg    = &routing.QueuedMessage{ID: "msg-1", Envelope: testEnv}
	testErr    = errors.New("queue error")
)

type testFixture struct {
	hot  *mockHotQueue
	cold *mockColdQueue
	comp MessageQueue
}

func setup(t *testing.T) *testFixture {
	hot := new(mockHotQueue)
	cold := new(mockColdQueue)
	comp, err := NewCompositeMessageQueue(hot, cold, zerolog.Nop())
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
	fx.hot.On("Enqueue", ctx, testEnv).Return(nil)

	err := fx.comp.EnqueueHot(ctx, testEnv)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testEnv)
	fx.cold.AssertNotCalled(t, "Enqueue")
}

func TestEnqueueHot_Fallback(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Hot queue fails
	fx.hot.On("Enqueue", ctx, testEnv).Return(testErr)
	// Cold queue succeeds
	fx.cold.On("Enqueue", ctx, testEnv).Return(nil)

	err := fx.comp.EnqueueHot(ctx, testEnv)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testEnv)
	fx.cold.AssertCalled(t, "Enqueue", ctx, testEnv)
}

func TestEnqueueHot_Fatal(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Hot queue fails
	fx.hot.On("Enqueue", ctx, testEnv).Return(testErr)
	// Cold queue also fails
	fx.cold.On("Enqueue", ctx, testEnv).Return(testErr)

	err := fx.comp.EnqueueHot(ctx, testEnv)
	require.Error(t, err)

	fx.hot.AssertCalled(t, "Enqueue", ctx, testEnv)
	fx.cold.AssertCalled(t, "Enqueue", ctx, testEnv)
}

func TestEnqueueCold(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	fx.cold.On("Enqueue", ctx, testEnv).Return(nil)

	err := fx.comp.EnqueueCold(ctx, testEnv)
	require.NoError(t, err)

	fx.cold.AssertCalled(t, "Enqueue", ctx, testEnv)
	fx.hot.AssertNotCalled(t, "Enqueue")
}

func TestRetrieveBatch_HotHit(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	hotResult := []*routing.QueuedMessage{testMsg}
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return(hotResult, nil)

	res, err := fx.comp.RetrieveBatch(ctx, testURN, 10)
	require.NoError(t, err)
	assert.Equal(t, hotResult, res)

	fx.hot.AssertCalled(t, "RetrieveBatch", ctx, testURN, 10)
	fx.cold.AssertNotCalled(t, "RetrieveBatch")
}

func TestRetrieveBatch_ColdHit(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	coldResult := []*routing.QueuedMessage{testMsg}
	// Hot queue returns empty
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return([]*routing.QueuedMessage{}, nil)
	// Cold queue returns messages
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
	fx.hot.On("RetrieveBatch", ctx, testURN, 10).Return(nil, testErr)
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

	// Both should be called
	fx.hot.On("Acknowledge", ctx, testURN, ids).Return(nil)
	fx.cold.On("Acknowledge", ctx, testURN, ids).Return(nil)

	err := fx.comp.Acknowledge(ctx, testURN, ids)
	require.NoError(t, err)

	// Need to use Eventually because they run in parallel
	require.Eventually(t, func() bool {
		fx.hot.AssertCalled(t, "Acknowledge", ctx, testURN, ids)
		fx.cold.AssertCalled(t, "Acknowledge", ctx, testURN, ids)
		return true
	}, 1*time.Second, 10*time.Millisecond)
}

func TestMigrateHotToCold(t *testing.T) {
	fx := setup(t)
	ctx := context.Background()

	// Assert that MigrateToCold is called on the hot queue,
	// and that the cold queue instance is passed to it.
	fx.hot.On("MigrateToCold", ctx, testURN, fx.cold).Return(nil)

	err := fx.comp.MigrateHotToCold(ctx, testURN)
	require.NoError(t, err)

	fx.hot.AssertCalled(t, "MigrateToCold", ctx, testURN, fx.cold)
}
