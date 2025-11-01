/*
File: cmd/runroutingservice/runroutingservice.go
Description: REFACTORED to use the new 'tinywideclouds' and
'go-platform' packages, new 'urn' types, and to CORRECTLY
create Pub/Sub subscriptions using the AdminClient pattern.
*/
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tinywideclouds/go-routing-service/cmd"
	"github.com/tinywideclouds/go-routing-service/internal/app"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	psub "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
	"github.com/tinywideclouds/go-routing-service/internal/platform/websocket"
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

func main() {
	// 1. Setup structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger := log.With().Str("service", "go-routing-service").Logger()

	// 2. Load config.yaml
	cfg, err := cmd.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// 3. Create dependencies
	ctx := context.Background()
	deps, err := newDependencies(ctx, cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize dependencies")
	}

	// 4. Create Authentication Middleware
	authMiddleware, err := newAuthMiddleware(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize authentication middleware")
	}

	// 5. Create the two main services
	apiService, err := routingservice.New(
		cfg,
		deps,
		authMiddleware,
		logger.With().Str("component", "ApiService").Logger(),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create API service")
	}

	// REFACTORED: Correctly check for error from NewConnectionManager
	connManager, err := realtime.NewConnectionManager(
		cfg.WebSocketPort,
		authMiddleware,
		deps.PresenceCache,
		deps.DeliveryConsumer,
		logger.With().Str("component", "ConnManager").Logger(),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create Connection Manager")
	}

	// 6. Run the application
	app.Run(ctx, logger, apiService, connManager)
}

// newAuthMiddleware creates the JWT-validating middleware.
func newAuthMiddleware(cfg *config.AppConfig, logger zerolog.Logger) (func(http.Handler) http.Handler, error) {
	// REFACTORED: Use JWKS Auth
	// Step 1: Call the identity service's discovery endpoint to get the JWKS URL
	// and validate it supports our required algorithm.
	jwksURL, err := middleware.DiscoverAndValidateJWTConfig(cfg.IdentityServiceURL, "RS256", logger)
	if err != nil {
		return nil, fmt.Errorf("failed to discover OIDC config: %w", err)
	}

	// Step 2: Create the middleware, passing it the discovered JWKS URL.
	// The middleware itself will handle fetching and caching the keys.
	// (The line creating 'jwksManager' was incorrect and has been removed).
	return middleware.NewJWKSAuthMiddleware(jwksURL)
}

// newDependencies builds the service dependency container.
func newDependencies(ctx context.Context, cfg *config.AppConfig, logger zerolog.Logger) (*routing.Dependencies, error) {
	if cfg.RunMode == "local" {
		logger.Warn().Msg("Running in 'local' mode. All external dependencies will be faked.")
		return cmd.NewFakeDependencies(ctx, cfg, logger)
	}
	return newProdDependencies(ctx, cfg, logger)
}

// newProdDependencies creates real, production-ready dependencies.
func newProdDependencies(ctx context.Context, cfg *config.AppConfig, logger zerolog.Logger) (*routing.Dependencies, error) {
	// Connect to GCP
	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to firestore: %w", err)
	}
	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to pubsub: %w", err)
	}

	// Create concrete dependencies
	ingestProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))
	store, err := persistence.NewFirestoreStore(fsClient, logger)
	if err != nil {
		return nil, err
	}
	// REFACTORED: Use new URN types
	presenceCache, err := newPresenceCache(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, err
	}
	// REFACTORED: Use new URN types
	tokenFetcher, err := newFirestoreTokenFetcher(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, err
	}
	// REFACTORED: Use new admin client creation pattern
	ingestConsumer, err := newIngestionConsumer(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}
	deliveryProducer, err := newDeliveryProducer(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}
	// REFACTORED: Use new admin client creation pattern
	deliveryConsumer, err := newDeliveryConsumer(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}
	pushNotifier, err := newPushNotifier(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}

	return &routing.Dependencies{
		IngestionProducer:  ingestProducer,
		DeliveryProducer:   deliveryProducer,
		IngestionConsumer:  ingestConsumer,
		DeliveryConsumer:   deliveryConsumer,
		MessageStore:       store,
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: tokenFetcher,
		PushNotifier:       pushNotifier,
	}, nil
}

// --- Production Dependency Constructors ---

// newIngestionConsumer creates the persistent subscription for the main ingress topic.
// CORRECTED: This now uses the AdminClient to create the subscription
// idempotently, including the Dead-Letter-Queue configuration.
func newIngestionConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (messagepipeline.MessageConsumer, error) {

	topicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.IngressTopicID)
	subPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, cfg.IngressSubscriptionID)
	dlqTopicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.IngressTopicDLQID)

	subConfig := &pubsubpb.Subscription{
		Name:               subPath,
		Topic:              topicPath,
		AckDeadlineSeconds: 10,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopicPath,
			MaxDeliveryAttempts: 5, // Or make this configurable
		},
		EnableMessageOrdering: false,
	}

	// Try to get the subscription. If it exists, we're done.
	sub, err := psClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subPath})
	if err != nil {
		// If it's anything other than "Not Found", it's a real error.
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("failed to get subscription %s: %w", subPath, err)
		}

		// It's "Not Found", so let's create it.
		logger.Info().Str("subscription", subPath).Msg("Subscription not found, creating it...")
		sub, err = psClient.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription %s: %w", subPath, err)
		}
	}

	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(sub.Name), psClient, logger,
	)
}

func newDeliveryProducer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (routing.DeliveryProducer, error) {
	deliveryPubsubProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.DeliveryTopicID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}
	return websocket.NewDeliveryProducer(deliveryPubsubProducer), nil
}

// newDeliveryConsumer creates the ephemeral, auto-deleting subscription for the delivery bus.
// CORRECTED: This now uses the AdminClient pattern from your snippet.
func newDeliveryConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (messagepipeline.MessageConsumer, error) {
	// Parse the expiration duration from config, with a safe default.
	exp, err := time.ParseDuration(cfg.DeliveryBusSubscriptionExpiration)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid delivery_bus_sub_expiration, defaulting to 24h")
		exp = 24 * time.Hour
	}

	topicPath := fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, cfg.DeliveryTopicID)
	// Create a unique subscription name for this instance
	subID := fmt.Sprintf("delivery-bus-consumer-%s", uuid.NewString())
	subPath := fmt.Sprintf("projects/%s/subscriptions/%s", cfg.ProjectID, subID)

	subConfig := &pubsubpb.Subscription{
		Name:               subPath,
		Topic:              topicPath,
		AckDeadlineSeconds: 10,
		ExpirationPolicy:   &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(exp)},
	}

	// Create the ephemeral subscription
	sub, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ephemeral subscription: %w", err)
	}

	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(sub.Name), psClient, logger,
	)
}

// newPresenceCache creates a Firestore-backed presence cache.
// REFACTORED: Uses new URN types
func newPresenceCache(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger zerolog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) {
	cacheType := cfg.PresenceCache.Type
	logger.Info().Str("type", cacheType).Msg("Initializing presence cache...")
	switch cacheType {
	case "firestore":
		// This uses the FirestorePresenceCache from the dataflow library
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.CollectionName,
		)
	default:
		return nil, fmt.Errorf("invalid presence_cache type: %s", cacheType)
	}
}

// newFirestoreTokenFetcher creates a Firestore-backed token fetcher.
// REFACTORED: Uses new URN types
func newFirestoreTokenFetcher(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger zerolog.Logger) (cache.Fetcher[urn.URN, []routing.DeviceToken], error) {
	stringDocFetcher, err := cache.NewFirestore[string, persistence.DeviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: cfg.ProjectID, CollectionName: "device-tokens"},
		fsClient,
		logger,
	)
	if err != nil {
		return nil, err
	}
	stringTokenFetcher := &persistence.FirestoreTokenAdapter{DocFetcher: stringDocFetcher}
	return persistence.NewURNTokenFetcherAdapter(stringTokenFetcher), nil
}

// newPushNotifier creates a Pub/Sub-backed push notifier.
func newPushNotifier(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (routing.PushNotifier, error) {
	pushProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}
	return push.NewPubSubNotifier(pushProducer, logger)
}
