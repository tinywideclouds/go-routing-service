// --- File: internal/pipeline/routing_processor_test.go ---
package pipeline_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/pipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"

	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"

	routing_v1 "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// --- Mocks ---

type mockPresenceCache[K comparable, V any] struct {
	mock.Mock
}

func (m *mockPresenceCache[K, V]) AddSession(ctx context.Context, key K, sessionID string, val V) error {
	return m.Called(ctx, key, sessionID, val).Error(0)
}
func (m *mockPresenceCache[K, V]) RemoveSession(ctx context.Context, key K, sessionID string) (int, error) {
	args := m.Called(ctx, key, sessionID)
	return args.Int(0), args.Error(1)
}
func (m *mockPresenceCache[K, V]) FetchSessions(ctx context.Context, key K) (map[string]V, error) {
	args := m.Called(ctx, key)
	if val, ok := args.Get(0).(map[string]V); ok {
		return val, args.Error(1)
	}
	return nil, args.Error(1)
}
func (m *mockPresenceCache[K, V]) Clear(ctx context.Context, key K) error { return nil }
func (m *mockPresenceCache[K, V]) Close() error                           { return nil }

type mockMessageQueue struct {
	mock.Mock
}

func (m *mockMessageQueue) EnqueueHot(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) EnqueueCold(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockMessageQueue) RetrieveBatch(ctx context.Context, userURN urn.URN, limit int) ([]*routing_v1.QueuedMessage, error) {
	return nil, nil
}
func (m *mockMessageQueue) Acknowledge(ctx context.Context, userURN urn.URN, messageIDs []string) error {
	return nil
}
func (m *mockMessageQueue) MigrateHotToCold(ctx context.Context, userURN urn.URN) error { return nil }

type mockPushNotifier struct {
	mock.Mock
}

func (m *mockPushNotifier) NotifyOffline(ctx context.Context, envelope *secure.SecureEnvelope) error {
	return m.Called(ctx, envelope).Error(0)
}
func (m *mockPushNotifier) PokeOnline(ctx context.Context, recipient urn.URN) error {
	return m.Called(ctx, recipient).Error(0)
}

// --- Test Setup ---
var (
	nopLogger    = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	testConfig   = &config.AppConfig{}
	testURN, _   = urn.Parse("urn:contacts:user:test-user")
	testEnvelope = &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("test"),
		Priority:      1,
	}
	testMessage = messagepipeline.Message{}
	errTest     = errors.New("something went wrong")
)

// --- Test Cases ---

func TestRoutingProcessor_OnlineUser(t *testing.T) {
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	messageQueue := new(mockMessageQueue)
	pushNotifier := new(mockPushNotifier)
	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
		PushNotifier:  pushNotifier,
	}

	// Mock Online: FetchSessions returns a map with 1 entry
	activeSessions := map[string]routing.ConnectionInfo{"s1": {}}
	presenceCache.On("FetchSessions", mock.Anything, testURN).Return(activeSessions, nil)

	messageQueue.On("EnqueueHot", mock.Anything, testEnvelope).Return(nil)
	pushNotifier.On("PokeOnline", mock.Anything, testURN).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)
	err := processor(context.Background(), testMessage, testEnvelope)

	require.NoError(t, err)
}

func TestRoutingProcessor_ExpressLane_Offline_Ephemeral(t *testing.T) {
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	messageQueue := new(mockMessageQueue)
	pushNotifier := new(mockPushNotifier)

	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
		PushNotifier:  pushNotifier,
	}

	syncEnvelope := &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("sync-key"),
		IsEphemeral:   true,
		Priority:      5,
	}

	// 1. User is Offline (Empty map or error)
	presenceCache.On("FetchSessions", mock.Anything, testURN).Return(map[string]routing.ConnectionInfo{}, errTest)

	messageQueue.On("EnqueueHot", mock.Anything, syncEnvelope).Return(nil)
	pushNotifier.On("NotifyOffline", mock.Anything, syncEnvelope).Return(nil)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)

	err := processor(context.Background(), testMessage, syncEnvelope)

	require.NoError(t, err)
	messageQueue.AssertCalled(t, "EnqueueHot", mock.Anything, syncEnvelope)
	pushNotifier.AssertCalled(t, "NotifyOffline", mock.Anything, syncEnvelope)
}

func TestRoutingProcessor_StandardLane_Offline_Ephemeral_Drop(t *testing.T) {
	presenceCache := new(mockPresenceCache[urn.URN, routing.ConnectionInfo])
	messageQueue := new(mockMessageQueue)
	deps := &routing.ServiceDependencies{
		PresenceCache: presenceCache,
		MessageQueue:  messageQueue,
	}

	typingEnvelope := &secure.SecureEnvelope{
		RecipientID:   testURN,
		EncryptedData: []byte("typing"),
		IsEphemeral:   true,
		Priority:      1,
	}

	presenceCache.On("FetchSessions", mock.Anything, testURN).Return(map[string]routing.ConnectionInfo{}, errTest)

	processor := pipeline.NewRoutingProcessor(deps, testConfig, nopLogger)
	err := processor(context.Background(), testMessage, typingEnvelope)

	require.NoError(t, err)
	messageQueue.AssertNotCalled(t, "EnqueueHot", mock.Anything, mock.Anything)
}
