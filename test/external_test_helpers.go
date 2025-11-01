// REFACTOR: This file contains corrected test helpers that now correctly
// provide URN-keyed dependencies by creating string-keyed concrete types and
// wrapping them in the necessary adapters.

package test

import (
	"context"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-secure-messaging/pkg/urn"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	psadapter "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
)

// deviceTokenDoc is the shape of the data stored in Firestore for device tokens.
type deviceTokenDoc struct {
	Tokens []routing.DeviceToken
}

// NewTestFirestoreTokenFetcher creates a URN-keyed fetcher for testing.
// It does this by creating a concrete, string-keyed Firestore fetcher and
// wrapping it in our URN adapter. This encapsulates the logic for the test.
func NewTestFirestoreTokenFetcher(
	ctx context.Context,
	fsClient *firestore.Client,
	projectID string,
	logger zerolog.Logger,
) (cache.Fetcher[urn.URN, []routing.DeviceToken], error) {
	// 1. Create the concrete, string-keyed Firestore fetcher.
	stringDocFetcher, err := cache.NewFirestore[string, deviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: projectID, CollectionName: "device-tokens"},
		fsClient,
		logger,
	)
	if err != nil {
		return nil, err
	}

	// 2. Create an adapter that extracts the token slice from the doc.
	stringTokenFetcher := &FirestoreTokenAdapter{docFetcher: stringDocFetcher}

	// 3. Wrap the string-keyed fetcher in our URN adapter.
	urnAdapter := NewURNTokenFetcherAdapter(stringTokenFetcher)

	return urnAdapter, nil
}

// NewTestConsumer creates a concrete GooglePubsubConsumer for testing purposes.
func NewTestConsumer(
	subID string,
	client *pubsub.Client,
	logger zerolog.Logger,
) (messagepipeline.MessageConsumer, error) {
	cfg := messagepipeline.NewGooglePubsubConsumerDefaults(subID)
	return messagepipeline.NewGooglePubsubConsumer(cfg, client, logger)
}

// NewTestProducer creates a concrete PubsubProducer for testing purposes.
func NewTestProducer(topic *pubsub.Publisher) routing.IngestionProducer {
	return psadapter.NewProducer(topic)
}

// NewTestMessageStore creates a concrete FirestoreStore for testing purposes.
func NewTestMessageStore(
	fsClient *firestore.Client,
	logger zerolog.Logger,
) (routing.MessageStore, error) {
	// This helper encapsulates the import of the internal persistence package,
	// providing a controlled entrypoint for external tests.
	return persistence.NewFirestoreStore(fsClient, logger)
}
