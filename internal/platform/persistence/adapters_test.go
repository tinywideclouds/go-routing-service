/*
File: internal/platform/persistence/adapters_test.go
Description: NEW unit test for the persistence adapters.
*/
package persistence_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

// --- Mocks ---

type mockStringFetcher[V any] struct {
	mock.Mock
}

func (m *mockStringFetcher[V]) Fetch(ctx context.Context, key string) (V, error) {
	args := m.Called(ctx, key)
	var result V
	if val, ok := args.Get(0).(V); ok {
		result = val
	}
	return result, args.Error(1)
}

func (m *mockStringFetcher[V]) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Tests ---

func TestFirestoreTokenAdapter(t *testing.T) {
	ctx := context.Background()
	testTokens := []routing.DeviceToken{{Token: "test-token"}}
	testDoc := persistence.DeviceTokenDoc{Tokens: testTokens}
	testErr := errors.New("not found")

	t.Run("Success", func(t *testing.T) {
		// Arrange
		docFetcher := new(mockStringFetcher[persistence.DeviceTokenDoc])
		adapter := &persistence.FirestoreTokenAdapter{DocFetcher: docFetcher}
		docFetcher.On("Fetch", ctx, "test-key").Return(testDoc, nil)

		// Act
		tokens, err := adapter.Fetch(ctx, "test-key")

		// Assert
		require.NoError(t, err)
		assert.Equal(t, testTokens, tokens)
		docFetcher.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		// Arrange
		docFetcher := new(mockStringFetcher[persistence.DeviceTokenDoc])
		adapter := &persistence.FirestoreTokenAdapter{DocFetcher: docFetcher}
		docFetcher.On("Fetch", ctx, "test-key").Return(persistence.DeviceTokenDoc{}, testErr)

		// Act
		tokens, err := adapter.Fetch(ctx, "test-key")

		// Assert
		require.Error(t, err)
		assert.Equal(t, testErr, err)
		assert.Nil(t, tokens)
		docFetcher.AssertExpectations(t)
	})
}

func TestURNTokenFetcherAdapter(t *testing.T) {
	ctx := context.Background()
	testTokens := []routing.DeviceToken{{Token: "test-token"}}
	testErr := errors.New("not found")
	testURN, _ := urn.Parse("urn:sm:user:test-user")
	testKey := "urn:sm:user:test-user"

	t.Run("Success", func(t *testing.T) {
		// Arrange
		stringFetcher := new(mockStringFetcher[[]routing.DeviceToken])
		adapter := persistence.NewURNTokenFetcherAdapter(stringFetcher)
		stringFetcher.On("Fetch", ctx, testKey).Return(testTokens, nil)

		// Act
		tokens, err := adapter.Fetch(ctx, testURN)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, testTokens, tokens)
		stringFetcher.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		// Arrange
		stringFetcher := new(mockStringFetcher[[]routing.DeviceToken])
		adapter := persistence.NewURNTokenFetcherAdapter(stringFetcher)
		stringFetcher.On("Fetch", ctx, testKey).Return([]routing.DeviceToken(nil), testErr)

		// Act
		tokens, err := adapter.Fetch(ctx, testURN)

		// Assert
		require.Error(t, err)
		assert.Equal(t, testErr, err)
		assert.Nil(t, tokens)
		stringFetcher.AssertExpectations(t)
	})
}
