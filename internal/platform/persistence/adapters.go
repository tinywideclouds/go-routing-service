/*
File: internal/platform/persistence/adapters.go
Description: REFACTORED to use the new platform 'urn' package.
This fixes the type mismatches for all URN-keyed fetchers.
*/
// Package persistence contains components for interacting with data stores.
package persistence

import (
	"context"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use the new platform URN package
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

// DeviceTokenDoc is the shape of the data stored in Firestore for device tokens.
type DeviceTokenDoc struct {
	Tokens []routing.DeviceToken `firestore:"tokens"` // Added firestore tag
}

// FirestoreTokenAdapter is a helper that wraps a generic Firestore document
// fetcher and extracts the `Tokens` field from the returned struct.
type FirestoreTokenAdapter struct {
	DocFetcher cache.Fetcher[string, DeviceTokenDoc]
}

// Fetch satisfies the cache.Fetcher[string, []routing.DeviceToken] interface.
func (a *FirestoreTokenAdapter) Fetch(ctx context.Context, key string) ([]routing.DeviceToken, error) {
	doc, err := a.DocFetcher.Fetch(ctx, key)
	if err != nil {
		return nil, err
	}
	return doc.Tokens, nil
}

// Close satisfies the cache.Fetcher interface.
func (a *FirestoreTokenAdapter) Close() error {
	return a.DocFetcher.Close()
}

// URNTokenFetcherAdapter is a helper that wraps a string-keyed Fetcher
// to make it compatible with the URN-keyed Fetcher interface.
type URNTokenFetcherAdapter struct {
	stringFetcher cache.Fetcher[string, []routing.DeviceToken]
}

// NewURNTokenFetcherAdapter is the constructor for the adapter.
func NewURNTokenFetcherAdapter(stringFetcher cache.Fetcher[string, []routing.DeviceToken]) *URNTokenFetcherAdapter {
	adapter := &URNTokenFetcherAdapter{
		stringFetcher: stringFetcher,
	}
	return adapter
}

// Fetch satisfies the cache.Fetcher[urn.URN, []routing.DeviceToken] interface.
// REFACTORED: 'key' is now the new 'urn.URN' type.
func (a *URNTokenFetcherAdapter) Fetch(ctx context.Context, key urn.URN) ([]routing.DeviceToken, error) {
	// The logic is the same: call the underlying fetcher with the string key.
	return a.stringFetcher.Fetch(ctx, key.String())
}

// Close satisfies the cache.Fetcher interface.
func (a *URNTokenFetcherAdapter) Close() error {
	return a.stringFetcher.Close()
}
