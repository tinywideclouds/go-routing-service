/*
File: cmd/runroutingservice/runroutingservice.go
Description: REFACTORED to use the standard two-stage config
loading pattern (embed, Stage 1, Stage 2).
*/
package main

import (
	"context"
	_ "embed" // Required for go:embed
	"fmt"
	"net/http"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	// CORRECTED IMPORT:
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	// "github.com/tinywideclouds/go-routing-service/cmd" // No longer needed
	"github.com/tinywideclouds/go-routing-service/internal/app"
	"github.com/tinywideclouds/go-routing-service/internal/platform/persistence"
	psub "github.com/tinywideclouds/go-routing-service/internal/platform/pubsub"
	"github.com/tinywideclouds/go-routing-service/internal/platform/push"
	fsqueue "github.com/tinywideclouds/go-routing-service/internal/platform/queue"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/internal/realtime"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
	"github.com/tinywideclouds/go-routing-service/routingservice"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3" // Added for unmarshaling

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
)

//go:embed config.yaml
var configFile []byte

func main() {
	// 1. Setup structured logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger := log.With().Str("service", "go-routing-service").Logger()

	// 2. Load Configuration (Stage 0: Unmarshal)
	var yamlCfg config.YamlConfig
	if err := yaml.Unmarshal(configFile, &yamlCfg); err != nil {
		logger.Fatal().Err(err).Msg("Failed to unmarshal embedded yaml config")
	}

	// 3. Build Base Config (Stage 1: YAML to Base Struct)
	baseCfg, err := config.NewConfigFromYaml(&yamlCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to build base configuration from YAML")
	}

	// 4. Apply Overrides & Validate (Stage 2: Env Vars)
	cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to finalize configuration with environment overrides")
	}

	// I really hate what google did with ps v2
	cfg.IngressTopicID = convertPubsub(cfg.ProjectID, cfg.IngressTopicID, Pub)
	cfg.IngressSubscriptionID = convertPubsub(cfg.ProjectID, cfg.IngressSubscriptionID, Sub)
	cfg.IngressTopicDLQID = convertPubsub(cfg.ProjectID, cfg.IngressTopicDLQID, Pub)

	// 5. Create dependencies
	ctx := context.Background()

	deps, err := newDependencies(ctx, cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize dependencies")
	}

	// 6. Create Authentication Middleware
	authMiddleware, err := newAuthMiddleware(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize authentication middleware")
	}

	// 7. Create the two main services
	apiService, err := routingservice.New(
		cfg,
		deps,
		authMiddleware,
		logger.With().Str("component", "ApiService").Logger(),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create API service")
	}

	connManager, err := realtime.NewConnectionManager(
		":"+cfg.WebSocketPort, // Prepend ':'
		authMiddleware,
		deps.PresenceCache,
		deps.MessageQueue,
		logger.With().Str("component", "ConnManager").Logger(),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create Connection Manager")
	}

	// 8. Run the application
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
	// Always builds production dependencies.
	// Emulators are handled via environment variables, not a config flag.
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

	err = ensureTopics(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, err
	}

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
	pushNotifier, err := newPushNotifier(cfg, psClient, logger)
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
			return nil, fmt.Errorf("hot_queue type is redis but no address is configured (check REDIS_ADDR env var)")
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

// --- Production Dependency Constructors (unchanged) ---

func ensureTopics(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) error {
	ingressTopic := &pubsubpb.Topic{
		Name: cfg.IngressTopicID,
	}
	_, err := psClient.TopicAdminClient.CreateTopic(ctx, ingressTopic)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			logger.Err(err).Msg("pubsub error")
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicID)
		}
	}

	ingressTopicDLQ := &pubsubpb.Topic{
		Name: cfg.IngressTopicDLQID,
	}
	_, err = psClient.TopicAdminClient.CreateTopic(ctx, ingressTopicDLQ)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			logger.Err(err).Msg("pubsub error")
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicDLQID)
		}
	}
	return nil
}

func newIngestionConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (messagepipeline.MessageConsumer, error) {
	subConfig := &pubsubpb.Subscription{
		Name:               cfg.IngressSubscriptionID,
		Topic:              cfg.IngressTopicID,
		AckDeadlineSeconds: 10,
		DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     cfg.IngressTopicDLQID,
			MaxDeliveryAttempts: 5,
		},
		EnableMessageOrdering: false,
	}
	sub, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			logger.Err(err).Msg("pubsub error")
			return nil, fmt.Errorf("could not create sub: %s", cfg.IngressSubscriptionID)
		}
	}

	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(sub.Name), psClient, logger,
	)
}

func newPresenceCache(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger zerolog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) {
	cacheType := cfg.PresenceCache.Type
	logger.Info().Str("type", cacheType).Msg("Initializing presence cache...")
	switch cacheType {
	case "firestore":
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.MainCollectionName,
		)
	default:
		return nil, fmt.Errorf("invalid presence_cache type: %s", cacheType)
	}
}

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

// CORRECTED:
func newPushNotifier(cfg *config.AppConfig, psClient *pubsub.Client, logger zerolog.Logger) (routing.PushNotifier, error) {
	pushProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}
	return push.NewPubSubNotifier(pushProducer, logger)
}

type PS string

const (
	Sub PS = "subscriptions"
	Pub PS = "topics"
)

func convertPubsub(project, id string, ps PS) string {
	return fmt.Sprintf("projects/%s/%s/%s", project, ps, id)
}
