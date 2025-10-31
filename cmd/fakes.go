package cmd

import (
	"context"

	"github.com/illmade-knight/go-routing-service/internal/test/fakes"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/illmade-knight/go-routing-service/routingservice/config"
	"github.com/rs/zerolog"
)

// newFakeDependencies creates in-memory fakes for local development.
func NewFakeDependencies(ctx context.Context, cfg *config.AppConfig, logger zerolog.Logger) (*routing.Dependencies, error) {
	fakePubsub := fakes.NewPubsub(logger)
	// REFACTORED: Use new URN type
	fakePresenceCache := fakes.NewPresenceCache[urn.URN, routing.ConnectionInfo](logger)
	fakeTokenFetcher := fakes.NewTokenFetcher[urn.URN, []routing.DeviceToken](logger)

	return &routing.Dependencies{
		IngestionProducer:  fakePubsub.NewProducer(cfg.IngressTopicID),
		DeliveryProducer:   fakePubsub.NewProducer(cfg.DeliveryTopicID),
		IngestionConsumer:  fakePubsub.NewConsumer(cfg.IngressSubscriptionID),
		DeliveryConsumer:   fakePubsub.NewConsumer(cfg.DeliveryBusSubscriptionExpiration),
		MessageStore:       fakes.NewMessageStore(logger),
		PresenceCache:      fakePresenceCache,
		DeviceTokenFetcher: fakeTokenFetcher,
		PushNotifier:       fakes.NewPushNotifier(logger),
	}, nil
}
