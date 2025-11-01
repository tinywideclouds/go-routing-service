/*
File: cmd/runroutingservice/runroutingservice.go
Description: REFACTORED to wire up the new 'CompositeMessageQueue'
and remove all logic related to the old 'DeliveryBus'.
*/
package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/redis/go-redis/v9" // NEW: Import redis
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tinywideclouds/go-routing-service/cmd"
	"github.com/tinywideclouds/go-routing-service/internal/app"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	psub "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue" // NEW
	"github.com/tinywideclouds/go-routing-service/internal/queue"                  // NEW
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	// REFACTORED: The 'DeliveryConsumer' dependency is removed.
	connManager, err := realtime.NewConnectionManager(
		cfg.WebSocketPort,
		authMiddleware,
		deps.PresenceCache,
		deps.MessageQueue, // NEW: Pass the unified MessageQueue
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
	jwksURL, err := middleware.DiscoverAndValidateJWTConfig(cfg.IdentityServiceURL, "RS256", logger)
	if err != nil {
		return nil, fmt.Errorf("failed to discover OIDC config: %w", err)
	}
	return middleware.NewJWKSAuthMiddleware(jwksURL)
}

// newDependencies builds the service dependency container.
func newDependencies(ctx context.Context, cfg *config.AppConfig, logger zerolog.Logger) (*routing.ServiceDependencies, error) {
	if cfg.RunMode == "local" {
		logger.Warn().Msg("Running in 'local' mode. All external dependencies will be faked.")
		return cmd.NewFakeDependencies(ctx, cfg, logger)
	}
	return newProdDependencies(ctx, cfg, logger)
}

// newProdDependencies creates real, production-ready dependencies.
func newProdDependencies(ctx context.Context, cfg *config.AppConfig, logger zerolog.Logger) (*routing.ServiceDependencies, error) {
	// Connect to GCP
	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to firestore: %w", err)
	}
	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to pubsub: %w", err)
	}
	// Note: Redis client connection is handled in 'newHotQueue'

	// --- Create new Queue components ---
	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, cfg.ColdQueueCollection, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create cold queue: %w", err)
	}

	hotQueue, err := newHotQueue(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create hot queue: %w", err)
	}

	messageQueue, err := queue.NewCompositeMessageQueue(hotQueue, coldQueue, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create composite queue: %w", err)
	}
	// --- End Queue components ---

	// Create other concrete dependencies
	ingestProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))

	presenceCache, err := newPresenceCache(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, err
	}
	tokenFetcher, err := newFirestoreTokenFetcher(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, err
	}
	ingestConsumer, err := newIngestionConsumer(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}
	pushNotifier, err := newPushNotifier(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}

	return &routing.ServiceDependencies{
		IngestionProducer:  ingestProducer,
		IngestionConsumer:  ingestConsumer,
		MessageQueue:       messageQueue,
		PresenceCache:      presenceCache,
		DeviceTokenFetcher: tokenFetcher,
		PushNotifier:       pushNotifier,
	}, nil
}

// --- NEW HELPER ---
// newHotQueue creates the pluggable HotQueue based on config.
func newHotQueue(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger zerolog.Logger) (queue.HotQueue, error) {
	cacheType := cfg.HotQueue.Type
	logger.Info().Str("type", cacheType).Msg("Initializing hot queue...")

	switch cacheType {
	case "firestore":
		// Use the prototype Firestore-backed hot queue
		mainCol := cfg.HotQueue.Firestore.MainCollectionName
		pendingCol := cfg.HotQueue.Firestore.PendingCollectionName
		if mainCol == "" || pendingCol == "" {
			return nil, fmt.Errorf("hot_queue type is firestore but collection names are not configured")
		}
		return fsqueue.NewFirestoreHotQueue(
			fsClient,
			mainCol,
			pendingCol,
			logger,
		)

	case "redis":
		// Use the production Redis-backed hot queue
		redisAddr := cfg.HotQueue.Redis.Addr
		if redisAddr == "" {
			return nil, fmt.Errorf("hot_queue type is redis but no address is configured")
		}
		rdb := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		// Test the connection
		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil, fmt.Errorf("failed to connect to redis hot queue at %s: %w", redisAddr, err)
		}
		logger.Info().Str("addr", redisAddr).Msg("Connected to Redis hot queue")
		return fsqueue.NewRedisHotQueue(rdb, logger)

	default:
		return nil, fmt.Errorf("invalid hot_queue type: %s (must be 'firestore' or 'redis')", cacheType)
	}
}

// --- Production Dependency Constructors (mostly unchanged) ---

// newIngestionConsumer creates the persistent subscription for the main ingress topic.
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
			MaxDeliveryAttempts: 5,
		},
		EnableMessageOrdering: false,
	}
	sub, err := psClient.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subPath})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("failed to get subscription %s: %w", subPath, err)
		}
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

// newPresenceCache creates a Firestore-backed presence cache.
func newPresenceCache(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger zerolog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) {
	cacheType := cfg.PresenceCache.Type
	logger.Info().Str("type", cacheType).Msg("Initializing presence cache...")
	switch cacheType {
	case "firestore":
		// This uses the FirestorePresenceCache from the dataflow library
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.MainCollectionName, // Use MainCollectionName
		)
	default:
		return nil, fmt.Errorf("invalid presence_cache type: %s", cacheType)
	}
}

// newFirestoreTokenFetcher creates a Firestore-backed token fetcher.
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
