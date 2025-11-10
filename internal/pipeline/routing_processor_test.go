/*
File: internal/pipeline/routing_processor_test.go
Description: REFACTORED to test the new 'queue.MessageQueue'
and the explicit PushNotifier.PokeOnline/NotifyOffline methods.
*/
package pipeline_test

import (
	"context"
	"errors"
	"io" // <-- ADDED
	"log/slog"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingv1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks using testify/mock ---

type mockFetcher[K comparable, V any] struct {
	mock.Mock
}

func (m *mockFetcher[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	args := m.Called(ctx, key)
	var result V
	if val, ok := args.Get(0).(V); ok {
		result = val
	}
	return result, args.Error(1)
}
func (m *mockFetcher[K, V]) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockPresenceCache[K comparable, V any] struct {
	mock.Mock
}

func (m *mockPresenceCache[K, V]) Set(ctx context.Context, key K, val V) error {
	args := m.Called(ctx, key, val)
	return args.Error(0)
}
func (m *mockPresenceCache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	args := m.Called(ctx, key)
	var result V
	if val, ok := args.Get(0).(V); ok {
		result = val
	}
	return result, args.Error(1)
}
func (m *mockPresenceCache[K, V]) Delete(ctx context.Context, key K) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}
func (m *mockPresenceCache[K, V]) Close() error {
	args := m.Called()
	return args.Error(0)
}

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
func (m *mockMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingv1.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	var result []*routingv1.QueuedMessage
	if val, ok := args.Get(0).([]*routingv1.QueuedMessage); ok {
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

// --- START OF REFACTOR ---
// REFACTORED: Mock for routing.PushNotifier
type mockPushNotifier struct {
	mock.Mock
}

func (m *mockPushNotifier) NotifyOffline(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, tokens, envelope)
	return args.Error(0)
}

func (m *mockPushNotifier) PokeOnline(ctx context.Context, recipient urn.URN) error {
	args := m.Called(ctx, recipient)
	return args.Error(0)
}

// --- END OF REFACTOR ---

// --- Test Setup ---
var (
	// --- (FIX) Use io.Discard for a truly NOP logger ---
	nopLogger    = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testConfig   = &config.AppConfig{}
	testURN, _   = urn.Parse("urn:sm:user:test-user")
	testEnvelope = &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("test"),
	}
	testMessage = messagepipeline.Message{}
	errTest     = errors.New("something went wrong")
)

// --- Test Cases ---

func TestRoutingProcessor_OnlineUser(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	messageQueue := new(mockMessageQueue)
	pushNotifier := new(mockPushNotifier)
	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
		PushNotifier:  pushNotifier,
	}

	// 1. User is online
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, nil)
	// 2. Expect publish to HOT queue
	messageQueue.On("EnqueueHot", mock.Anything, testEnvelope).Return(nil)

	// --- START OF REFACTOR ---
	// 3. Expect a "poke" notification
	pushNotifier.On("PokeOnline", mock.Anything, testURN).Return(nil)
	// --- END OF REFACTOR ---

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, messageQueue, pushNotifier)
	messageQueue.AssertNotCalled(t, "EnqueueCold", mock.Anything, mock.Anything)
	// --- (NEW) Assert NotifyOffline was not called ---
	pushNotifier.AssertNotCalled(t, "NotifyOffline", mock.Anything, mock.Anything, mock.Anything)
}

func TestRoutingProcessor_OfflineUser_WithMobileTokens(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	pushNotifier := new(mockPushNotifier)
	messageQueue := new(mockMessageQueue)
	deps := &routing.ServiceDependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       pushNotifier,
		MessageQueue:       messageQueue,
	}

	testTokens := []routing.DeviceToken{
		{Token: "ios-token", Platform: "ios"},
		{Token: "web-token", Platform: "web"}, // Should be ignored
	}
	expectedMobileTokens := []routing.DeviceToken{
		{Token: "ios-token", Platform: "ios"},
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, errTest)
	// 2. Fetches tokens
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return(testTokens, nil)

	// --- START OF REFACTOR ---
	// 3. Sends push notification (with full envelope)
	pushNotifier.On("NotifyOffline", mock.Anything, expectedMobileTokens, testEnvelope).Return(nil)
	// --- END OF REFACTOR ---

	// 4. Stores message in COLD queue
	messageQueue.On("EnqueueCold", mock.Anything, testEnvelope).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, pushNotifier, messageQueue)
	messageQueue.AssertNotCalled(t, "EnqueueHot", mock.Anything, mock.Anything)
	// --- (NEW) Assert PokeOnline was not called ---
	pushNotifier.AssertNotCalled(t, "PokeOnline", mock.Anything, mock.Anything)
}

func TestRoutingProcessor_OfflineUser_NoTokens(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	pushNotifier := new(mockPushNotifier)
	messageQueue := new(mockMessageQueue)
	deps := &routing.ServiceDependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       pushNotifier,
		MessageQueue:       messageQueue,
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, errTest)
	// 2. Fetches tokens (but finds none)
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return([]routing.DeviceToken{}, nil)
	// 3. Stores message in COLD queue
	messageQueue.On("EnqueueCold", mock.Anything, testEnvelope).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, messageQueue)
	// --- (NEW) Assert neither notifier method was called ---
	pushNotifier.AssertNotCalled(t, "NotifyOffline", mock.Anything, mock.Anything, mock.Anything)
	pushNotifier.AssertNotCalled(t, "PokeOnline", mock.Anything, mock.Anything)
}

func TestRoutingProcessor_CriticalStoreFailure(t *testing.T) {
	// Arrange: Offline user
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	messageQueue := new(mockMessageQueue)
	deps := &routing.ServiceDependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		MessageQueue:       messageQueue,
		PushNotifier:       new(mockPushNotifier), // Not called, but need non-nil
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, errTest)
	// 2. Fetches tokens (finds none)
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return([]routing.DeviceToken{}, nil)
	// 3. EnqueueCold fails (this IS critical)
	messageQueue.On("EnqueueCold", mock.Anything, testEnvelope).Return(errTest)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert: The error from EnqueueCold should be propagated
	require.Error(t, err)
	assert.Equal(t, errTest, errors.Unwrap(err))
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, messageQueue)
}

func TestRoutingProcessor_HotEnqueueFailure(t *testing.T) {
	// Arrange: Online user
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	messageQueue := new(mockMessageQueue)
	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
		PushNotifier:  new(mockPushNotifier),
	}

	// 1. User is online
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, nil)
	// 2. EnqueueHot fails
	messageQueue.On("EnqueueHot", mock.Anything, testEnvelope).Return(errTest)

	// --- (NEW) Assert PokeOnline is NOT called if EnqueueHot fails ---

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert: The error is propagated so the message is NACK'd
	require.Error(t, err)
	assert.Equal(t, errTest, errors.Unwrap(err))
	mock.AssertExpectationsForObjects(t, presenceCache, messageQueue)
	// --- (NEW) Assert PokeOnline was not called ---
	deps.PushNotifier.(*mockPushNotifier).AssertNotCalled(t, "PokeOnline", mock.Anything, mock.Anything)
}
