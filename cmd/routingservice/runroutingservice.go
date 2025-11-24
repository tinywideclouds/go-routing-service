/*
File: cmd/runroutingservice/runroutingservice.go
Description: Main entrypoint for the routing service.
Handles config loading, dependency injection, and starting the application.
*/
package main

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"os"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/illmade-knight/go-dataflow/pkg/cache"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog" // Required for interoperability with some libs DO NOT REMOVE

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
	"gopkg.in/yaml.v3"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
)

//go:embed config.yaml
var configFile []byte

func main() {
	// --- 1. Setup structured logging (slog) ---
	var logLevel slog.Level
	switch os.Getenv("LOG_LEVEL") {
	case "debug", "DEBUG":
		logLevel = slog.LevelDebug
	case "info", "INFO":
		logLevel = slog.LevelInfo
	case "warn", "WARN":
		logLevel = slog.LevelWarn
	case "error", "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})).With("service", "go-routing-service")

	slog.SetDefault(logger)

	// --- 2. Load Configuration (Stage 0: Unmarshal) ---
	var yamlCfg config.YamlConfig
	err := yaml.Unmarshal(configFile, &yamlCfg)
	if err != nil {
		logger.Error("Failed to unmarshal embedded yaml config", "err", err)
		os.Exit(1)
	}

	// --- 3. Build Base Config (Stage 1: YAML to Base Struct) ---
	baseCfg, err := config.NewConfigFromYaml(&yamlCfg, logger)
	if err != nil {
		logger.Error("Failed to build base configuration from YAML", "err", err)
		os.Exit(1)
	}

	// --- 4. Apply Overrides & Validate (Stage 2: Env Vars) ---
	cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)
	if err != nil {
		logger.Error("Failed to finalize configuration with environment overrides", "err", err)
		os.Exit(1)
	}

	// Convert topic/sub IDs to full GCP resource names
	cfg.IngressTopicID = convertPubsub(cfg.ProjectID, cfg.IngressTopicID, Pub)
	cfg.IngressSubscriptionID = convertPubsub(cfg.ProjectID, cfg.IngressSubscriptionID, Sub)
	cfg.IngressTopicDLQID = convertPubsub(cfg.ProjectID, cfg.IngressTopicDLQID, Pub)

	// --- 5. Create dependencies ---
	ctx := context.Background()

	deps, err := newDependencies(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize dependencies", "err", err)
		os.Exit(1)
	}

	// --- 6. Create Authentication Middlewares ---
	// 6a. Discover the JWKS URL once
	jwksURL, err := middleware.DiscoverAndValidateJWTConfig(cfg.IdentityServiceURL, middleware.RSA256, logger)
	if err != nil {
		logger.Error("Failed to discover OIDC config", "err", err)
		os.Exit(1)
	}

	// 6b. Create the HTTP auth middleware
	httpAuthMiddleware, err := middleware.NewJWKSAuthMiddleware(jwksURL, logger)
	if err != nil {
		logger.Error("Failed to initialize HTTP authentication middleware", "err", err)
		os.Exit(1)
	}

	// 6c. Create the WebSocket auth middleware
	wsAuthMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(jwksURL, logger)
	if err != nil {
		logger.Error("Failed to initialize WebSocket authentication middleware", "err", err)
		os.Exit(1)
	}

	// --- 7. Create the two main services ---
	apiService, err := routingservice.New(
		cfg,
		deps,
		httpAuthMiddleware,
		logger.With("component", "ApiService"),
	)
	if err != nil {
		logger.Error("Failed to create API service", "err", err)
		os.Exit(1)
	}

	connManager, err := realtime.NewConnectionManager(
		":"+cfg.WebSocketPort, // Prepend ':' for listener
		wsAuthMiddleware,
		deps.PresenceCache,
		deps.MessageQueue,
		logger.With("component", "ConnManager"),
	)
	if err != nil {
		logger.Error("Failed to create Connection Manager", "err", err)
		os.Exit(1)
	}

	// --- 8. Run the application ---
	app.Run(ctx, logger, apiService, connManager)
}

// newDependencies builds the service dependency container.
func newDependencies(ctx context.Context, cfg *config.AppConfig, logger *slog.Logger) (*routing.ServiceDependencies, error) {
	// Always builds production dependencies.
	// Emulators are handled via environment variables, not a config flag.
	return newProdDependencies(ctx, cfg, logger)
}

// newProdDependencies creates real, production-ready dependencies.
func newProdDependencies(ctx context.Context, cfg *config.AppConfig, logger *slog.Logger) (*routing.ServiceDependencies, error) {
	// Connect to GCP
	logger.Debug("Connecting to Firestore", "project_id", cfg.ProjectID)
	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to firestore: %w", err)
	}
	logger.Debug("Connecting to PubSub", "project_id", cfg.ProjectID)
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
	logger.Debug("Creating ingestion producer", "topic", cfg.IngressTopicID)
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

	logger.Debug("All production dependencies initialized")

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
func newHotQueue(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (queue.HotQueue, error) {
	cacheType := cfg.HotQueue.Type
	logger.Info("Initializing hot queue...", "type", cacheType)

	switch cacheType {
	case "firestore":
		// Use the prototype Firestore-backed hot queue
		mainCol := cfg.HotQueue.Firestore.MainCollectionName
		pendingCol := cfg.HotQueue.Firestore.PendingCollectionName
		if mainCol == "" || pendingCol == "" {
			logger.Error("hot_queue type is firestore but collection names are not configured")
			return nil, fmt.Errorf("hot_queue type is firestore but collection names are not configured")
		}
		logger.Debug("Using Firestore hot queue", "main_collection", mainCol, "pending_collection", pendingCol)
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
			logger.Error("hot_queue type is redis but no address is configured (check REDIS_ADDR env var)")
			return nil, fmt.Errorf("hot_queue type is redis but no address is configured (check REDIS_ADDR env var)")
		}
		logger.Debug("Connecting to Redis hot queue", "addr", redisAddr)
		rdb := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		// Test the connection
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Error("Failed to connect to redis hot queue", "addr", redisAddr, "err", err)
			return nil, fmt.Errorf("failed to connect to redis hot queue at %s: %w", redisAddr, err)
		}
		logger.Info("Connected to Redis hot queue", "addr", redisAddr)
		return fsqueue.NewRedisHotQueue(rdb, logger)

	default:
		return nil, fmt.Errorf("invalid hot_queue type: %s (must be 'firestore' or 'redis')", cacheType)
	}
}

// ensureTopics creates Pub/Sub topics if they don't already exist.
func ensureTopics(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) error {
	ingressTopic := &pubsubpb.Topic{
		Name: cfg.IngressTopicID,
	}
	logger.Debug("Ensuring topic exists", "topic", cfg.IngressTopicID)
	_, err := psClient.TopicAdminClient.CreateTopic(ctx, ingressTopic)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Topic already exists, skipping creation", "topic", cfg.IngressTopicID)
		} else {
			logger.Error("Failed to create topic", "topic", cfg.IngressTopicID, "err", err)
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicID)
		}
	}

	ingressTopicDLQ := &pubsubpb.Topic{
		Name: cfg.IngressTopicDLQID,
	}
	logger.Debug("Ensuring topic exists", "topic", cfg.IngressTopicDLQID)
	_, err = psClient.TopicAdminClient.CreateTopic(ctx, ingressTopicDLQ)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Topic already exists, skipping creation", "topic", cfg.IngressTopicDLQID)
		} else {
			logger.Error("Failed to create topic", "topic", cfg.IngressTopicDLQID, "err", err)
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicDLQID)
		}
	}
	return nil
}

// newIngestionConsumer creates the Pub/Sub subscription and consumer.
func newIngestionConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (messagepipeline.MessageConsumer, error) {
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
	logger.Debug("Ensuring subscription exists", "sub", subConfig.Name, "topic", subConfig.Topic)
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Subscription already exists, skipping creation", "sub", subConfig.Name)
		} else {
			logger.Error("Failed to create subscription", "sub", subConfig.Name, "err", err)
			return nil, fmt.Errorf("could not create sub: %s", cfg.IngressSubscriptionID)
		}
	}

	// This external library expects a zerolog.Logger. We pass zerolog.Nop()
	// as our service-wide logger is slog.
	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(subConfig.Name), psClient, zerolog.Nop(),
	)
}

// newPresenceCache creates the pluggable PresenceCache based on config.
func newPresenceCache(_ context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) {
	cacheType := cfg.PresenceCache.Type
	logger.Info("Initializing presence cache...", "type", cacheType)
	switch cacheType {
	case "firestore":
		logger.Debug("Using Firestore presence cache", "collection", cfg.PresenceCache.Firestore.MainCollectionName)
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.MainCollectionName,
		)
	default:
		return nil, fmt.Errorf("invalid presence_cache type: %s", cacheType)
	}
}

// newFirestoreTokenFetcher creates the adapter for fetching device tokens from Firestore.
func newFirestoreTokenFetcher(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (cache.Fetcher[urn.URN, []routing.DeviceToken], error) {
	logger.Debug("Initializing Firestore token fetcher", "collection", "device-tokens")

	// This external library also expects a zerolog.Logger. We pass Nop.
	stringDocFetcher, err := cache.NewFirestore[string, persistence.DeviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: cfg.ProjectID, CollectionName: "device-tokens"},
		fsClient,
		zerolog.Nop(),
	)
	if err != nil {
		return nil, err
	}
	stringTokenFetcher := &persistence.FirestoreTokenAdapter{DocFetcher: stringDocFetcher}
	return persistence.NewURNTokenFetcherAdapter(stringTokenFetcher), nil
}

// newPushNotifier creates the producer for sending push notifications.
func newPushNotifier(cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (routing.PushNotifier, error) {
	logger.Debug("Initializing push notifier producer", "topic", cfg.PushNotificationsTopicID)

	// This external library also expects a zerolog.Logger. We pass Nop.
	pushProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, zerolog.Nop(),
	)
	if err != nil {
		return nil, err
	}
	return push.NewPubSubNotifier(pushProducer, logger)
}

// PS is a type for Pub/Sub resource types (Topic or Subscription).
type PS string

const (
	// Sub identifies a subscription resource.
	Sub PS = "subscriptions"
	// Pub identifies a topic resource.
	Pub PS = "topics"
)

// convertPubsub formats a short ID into a full GCP resource name.
func convertPubsub(project, id string, ps PS) string {
	return fmt.Sprintf("projects/%s/%s/%s", project, ps, id)
}
