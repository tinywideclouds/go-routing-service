// Package test provides public test helpers for setting up end-to-end and
// integration tests for the routing-service.
package test

import (
	"context"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-secure-messaging/pkg/urn"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
)

// URNTokenFetcherAdapter is a test helper that wraps a string-keyed Fetcher
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
func (a *URNTokenFetcherAdapter) Fetch(ctx context.Context, key urn.URN) ([]routing.DeviceToken, error) {
	return a.stringFetcher.Fetch(ctx, key.String())
}

// Close satisfies the cache.Fetcher interface.
func (a *URNTokenFetcherAdapter) Close() error {
	return a.stringFetcher.Close()
}

// FirestoreTokenAdapter is a helper that wraps a generic Firestore document
// fetcher and extracts the `Tokens` field from the returned struct.
type FirestoreTokenAdapter struct {
	docFetcher cache.Fetcher[string, deviceTokenDoc]
}

// Fetch satisfies the cache.Fetcher[string, []routing.DeviceToken] interface.
func (a *FirestoreTokenAdapter) Fetch(ctx context.Context, key string) ([]routing.DeviceToken, error) {
	doc, err := a.docFetcher.Fetch(ctx, key)
	if err != nil {
		return nil, err
	}
	return doc.Tokens, nil
}

// Close satisfies the cache.Fetcher interface.
func (a *FirestoreTokenAdapter) Close() error {
	return a.docFetcher.Close()
}
