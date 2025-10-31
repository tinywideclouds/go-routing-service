//go:build unit

/*
File: internal/pipeline/routing_processor_test.go
Description: REFACTORED to test the processor with the
new 'secure.SecureEnvelope' and new mock interfaces.
*/
package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-routing-service/internal/pipeline"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/illmade-knight/go-routing-service/routingservice/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingV1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
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

func (m *mockFetcher[K, V]) Delete(ctx context.Context, key K) error {
	args := m.Called(ctx, key)

	return args.Error(1)
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

// REFACTORED: Mock matches new interface
type mockMessageStore struct {
	mock.Mock
}

func (m *mockMessageStore) StoreMessages(ctx context.Context, recipient urn.URN, envelopes []*secure.SecureEnvelope) error {
	args := m.Called(ctx, recipient, envelopes)
	return args.Error(0)
}

func (m *mockMessageStore) RetrieveMessageBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routingV1.QueuedMessage, error) {
	args := m.Called(ctx, userURN, limit)
	var result []*routingV1.QueuedMessage
	if val, ok := args.Get(0).([]*routingV1.QueuedMessage); ok {
		result = val
	}
	return result, args.Error(1)
}

func (m *mockMessageStore) AcknowledgeMessages(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	args := m.Called(ctx, userURN, messageIDs)
	return args.Error(0)
}

// REFACTORED: Mock matches new interface
type mockDeliveryProducer struct {
	mock.Mock
}

func (m *mockDeliveryProducer) Publish(ctx context.Context, topicID string, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, topicID, envelope)
	return args.Error(0)
}

// REFACTORED: Mock matches new interface
type mockPushNotifierb struct {
	mock.Mock
}

func (m *mockPushNotifierb) Notify(ctx context.Context, tokens []routing.DeviceToken, envelope *secure.SecureEnvelope) error {
	args := m.Called(ctx, tokens, envelope)
	return args.Error(0)
}

// --- Test Setup ---
var (
	nopLogger  = zerolog.Nop()
	testConfig = &config.AppConfig{DeliveryTopicID: "test-delivery-topic"}
	testURN, _ = urn.Parse("urn:sm:user:test-user")
	// REFACTORED: Use new "dumb" envelope
	testEnvelope = &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("test"),
	}
	testMessage = messagepipeline.Message{}
	testErr     = errors.New("something went wrong")
)

// --- Test Cases ---

func TestRoutingProcessor_OnlineUser(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deliveryProducer := new(mockDeliveryProducer)
	messageStore := new(mockMessageStore)
	deps := &routing.Dependencies{
		PresenceCache:    presenceCache,
		DeliveryProducer: deliveryProducer,
		MessageStore:     messageStore,
	}

	// 1. User is online
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, nil)
	// 2. Expect publish to delivery bus
	deliveryProducer.On("Publish", mock.Anything, testConfig.DeliveryTopicID, testEnvelope).Return(nil)
	// 3. Expect message to ALSO be stored (for multi-device)
	messageStore.On("StoreMessages", mock.Anything, testURN, []*secure.SecureEnvelope{testEnvelope}).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deliveryProducer, messageStore)
}

func TestRoutingProcessor_OfflineUser_WithMobileTokens(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	pushNotifier := new(mockPushNotifier)
	messageStore := new(mockMessageStore)
	deps := &routing.Dependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       pushNotifier,
		MessageStore:       messageStore,
	}

	testTokens := []routing.DeviceToken{
		{Token: "ios-token", Platform: "ios"},
		{Token: "web-token", Platform: "web"}, // Should be ignored
	}
	expectedMobileTokens := []routing.DeviceToken{
		{Token: "ios-token", Platform: "ios"},
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, testErr)
	// 2. Fetches tokens
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return(testTokens, nil)
	// 3. Sends push notification (with only mobile tokens)
	pushNotifier.On("Notify", mock.Anything, expectedMobileTokens, testEnvelope).Return(nil)
	// 4. Stores message
	messageStore.On("StoreMessages", mock.Anything, testURN, []*secure.SecureEnvelope{testEnvelope}).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, pushNotifier, messageStore)
}

func TestRoutingProcessor_OfflineUser_NoTokens(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	pushNotifier := new(mockPushNotifier) // This will be used
	messageStore := new(mockMessageStore)
	deps := &routing.Dependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       pushNotifier,
		MessageStore:       messageStore,
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, testErr)
	// 2. Fetches tokens (but finds none)
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return([]routing.DeviceToken{}, nil)
	// 3. Stores message
	messageStore.On("StoreMessages", mock.Anything, testURN, []*secure.SecureEnvelope{testEnvelope}).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, messageStore)
	// 4. Asserts push notifier was NOT called
	pushNotifier.AssertNotCalled(t, "Notify", mock.Anything, mock.Anything, mock.Anything)
}

func TestRoutingProcessor_OfflineUser_TokenFetchFails(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	pushNotifier := new(mockPushNotifier) // This will be used
	messageStore := new(mockMessageStore)
	deps := &routing.Dependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		PushNotifier:       pushNotifier,
		MessageStore:       messageStore,
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, testErr)
	// 2. Token fetch fails
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return(nil, testErr)
	// 3. Stores message (this should happen anyway)
	messageStore.On("StoreMessages", mock.Anything, testURN, []*secure.SecureEnvelope{testEnvelope}).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert: The processor should NOT return an error, as token fetch is non-critical
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, messageStore)
	// 4. Asserts push notifier was NOT called
	pushNotifier.AssertNotCalled(t, "Notify", mock.Anything, mock.Anything, mock.Anything)
}

func TestRoutingProcessor_CriticalStoreFailure(t *testing.T) {
	// Arrange
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	deviceTokenFetcher := new(mockFetcher[urn.URN, []routing.DeviceToken])
	messageStore := new(mockMessageStore)
	deps := &routing.Dependencies{
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: deviceTokenFetcher,
		MessageStore:       messageStore,
	}

	// 1. User is offline
	presenceCache.On("Fetch", mock.Anything, testURN).Return(routing.ConnectionInfo{}, testErr)
	// 2. Fetches tokens (finds none)
	deviceTokenFetcher.On("Fetch", mock.Anything, testURN).Return([]routing.DeviceToken{}, nil)
	// 3. StoreMessages fails (this IS critical)
	messageStore.On("StoreMessages", mock.Anything, testURN, []*secure.SecureEnvelope{testEnvelope}).Return(testErr)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	// Act
	err := processor(context.Background(), testMessage, testEnvelope)

	// Assert: The error from StoreMessages should be propagated
	require.Error(t, err)
	assert.Equal(t, testErr, err)
	mock.AssertExpectationsForObjects(t, presenceCache, deviceTokenFetcher, messageStore)
}
