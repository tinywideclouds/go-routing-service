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
	"log/slog" // IMPORTED
	"os"       // IMPORTED

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/illmade-knight/go-dataflow/pkg/cache"

	// CORRECTED IMPORT:
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog" // REMOVED

	// "github.com/rs/zerolog/log" // REMOVED
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
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
)

//go:embed config.yaml
var configFile []byte

func main() {
	// --- 1. Setup structured logging (REFACTORED TO SLOG) ---
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
	// --- END REFACTOR ---

	// 2. Load Configuration (Stage 0: Unmarshal)
	var yamlCfg config.YamlConfig
	err := yaml.Unmarshal(configFile, &yamlCfg)
	if err != nil {
		logger.Error("Failed to unmarshal embedded yaml config", "err", err) // CHANGED
		os.Exit(1)
	}

	// 3. Build Base Config (Stage 1: YAML to Base Struct)
	baseCfg, err := config.NewConfigFromYaml(&yamlCfg, logger) // CHANGED
	if err != nil {
		logger.Error("Failed to build base configuration from YAML", "err", err) // CHANGED
		os.Exit(1)
	}

	// 4. Apply Overrides & Validate (Stage 2: Env Vars)
	cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger) // CHANGED
	if err != nil {
		logger.Error("Failed to finalize configuration with environment overrides", "err", err) // CHANGED
		os.Exit(1)
	}

	// I really hate what google did with ps v2
	cfg.IngressTopicID = convertPubsub(cfg.ProjectID, cfg.IngressTopicID, Pub)
	cfg.IngressSubscriptionID = convertPubsub(cfg.ProjectID, cfg.IngressSubscriptionID, Sub)
	cfg.IngressTopicDLQID = convertPubsub(cfg.ProjectID, cfg.IngressTopicDLQID, Pub)

	// 5. Create dependencies
	ctx := context.Background()

	deps, err := newDependencies(ctx, cfg, logger) // CHANGED
	if err != nil {
		logger.Error("Failed to initialize dependencies", "err", err) // CHANGED
		os.Exit(1)
	}

	// 6a. Discover the JWKS URL once
	jwksURL, err := middleware.DiscoverAndValidateJWTConfig(cfg.IdentityServiceURL, middleware.RSA256, logger)
	if err != nil {
		logger.Error("Failed to discover OIDC config", "err", err) // CHANGED
		os.Exit(1)
	}

	// 6b. Create the *existing* HTTP auth middleware
	httpAuthMiddleware, err := middleware.NewJWKSAuthMiddleware(jwksURL, logger)
	if err != nil {
		logger.Error("Failed to initialize HTTP authentication middleware", "err", err) // CHANGED
		os.Exit(1)
	}

	// 6c. Create the *new* WebSocket auth middleware
	wsAuthMiddleware, err := middleware.NewJWKSWebsocketAuthMiddleware(jwksURL, logger)
	if err != nil {
		logger.Error("Failed to initialize WebSocket authentication middleware", "err", err) // CHANGED
		os.Exit(1)
	}

	// 7. Create the two main services
	apiService, err := routingservice.New(
		cfg,
		deps,
		httpAuthMiddleware,
		logger.With("component", "ApiService"), // CHANGED
	)
	if err != nil {
		logger.Error("Failed to create API service", "err", err) // CHANGED
		os.Exit(1)
	}

	connManager, err := realtime.NewConnectionManager(
		":"+cfg.WebSocketPort, // Prepend ':'
		wsAuthMiddleware,
		deps.PresenceCache,
		deps.MessageQueue,
		logger.With("component", "ConnManager"), // CHANGED
	)
	if err != nil {
		logger.Error("Failed to create Connection Manager", "err", err) // CHANGED
		os.Exit(1)
	}

	// 8. Run the application
	app.Run(ctx, logger, apiService, connManager) // CHANGED
}

// newDependencies builds the service dependency container.
func newDependencies(ctx context.Context, cfg *config.AppConfig, logger *slog.Logger) (*routing.ServiceDependencies, error) { // CHANGED
	// Always builds production dependencies.
	// Emulators are handled via environment variables, not a config flag.
	return newProdDependencies(ctx, cfg, logger)
}

// newProdDependencies creates real, production-ready dependencies.
func newProdDependencies(ctx context.Context, cfg *config.AppConfig, logger *slog.Logger) (*routing.ServiceDependencies, error) { // CHANGED
	// Connect to GCP
	logger.Debug("Connecting to Firestore", "project_id", cfg.ProjectID) // ADDED
	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to firestore: %w", err)
	}
	logger.Debug("Connecting to PubSub", "project_id", cfg.ProjectID) // ADDED
	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to pubsub: %w", err)
	}

	err = ensureTopics(ctx, cfg, psClient, logger) // CHANGED
	if err != nil {
		return nil, err
	}

	// --- Create new Queue components ---
	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, cfg.ColdQueueCollection, logger) // CHANGED
	if err != nil {
		return nil, fmt.Errorf("failed to create cold queue: %w", err)
	}

	hotQueue, err := newHotQueue(ctx, cfg, fsClient, logger) // CHANGED
	if err != nil {
		return nil, fmt.Errorf("failed to create hot queue: %w", err)
	}

	messageQueue, err := queue.NewCompositeMessageQueue(hotQueue, coldQueue, logger) // CHANGED
	if err != nil {
		return nil, fmt.Errorf("failed to create composite queue: %w", err)
	}
	// --- End Queue components ---

	// Create other concrete dependencies
	logger.Debug("Creating ingestion producer", "topic", cfg.IngressTopicID)   // ADDED
	ingestProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID)) // CHANGED (topic.String() fix)

	presenceCache, err := newPresenceCache(ctx, cfg, fsClient, logger) // CHANGED
	if err != nil {
		return nil, err
	}
	tokenFetcher, err := newFirestoreTokenFetcher(ctx, cfg, fsClient, logger) // CHANGED
	if err != nil {
		return nil, err
	}
	ingestConsumer, err := newIngestionConsumer(ctx, cfg, psClient, logger) // CHANGED
	if err != nil {
		return nil, err
	}
	pushNotifier, err := newPushNotifier(cfg, psClient, logger) // CHANGED
	if err != nil {
		return nil, err
	}

	logger.Debug("All production dependencies initialized") // ADDED

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
func newHotQueue(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (queue.HotQueue, error) { // CHANGED
	cacheType := cfg.HotQueue.Type
	logger.Info("Initializing hot queue...", "type", cacheType) // CHANGED

	switch cacheType {
	case "firestore":
		// Use the prototype Firestore-backed hot queue
		mainCol := cfg.HotQueue.Firestore.MainCollectionName
		pendingCol := cfg.HotQueue.Firestore.PendingCollectionName
		if mainCol == "" || pendingCol == "" {
			logger.Error("hot_queue type is firestore but collection names are not configured") // ADDED
			return nil, fmt.Errorf("hot_queue type is firestore but collection names are not configured")
		}
		logger.Debug("Using Firestore hot queue", "main_collection", mainCol, "pending_collection", pendingCol) // ADDED
		return fsqueue.NewFirestoreHotQueue(
			fsClient,
			mainCol,
			pendingCol,
			logger, // CHANGED
		)

	case "redis":
		// Use the production Redis-backed hot queue
		redisAddr := cfg.HotQueue.Redis.Addr
		if redisAddr == "" {
			logger.Error("hot_queue type is redis but no address is configured (check REDIS_ADDR env var)") // ADDED
			return nil, fmt.Errorf("hot_queue type is redis but no address is configured (check REDIS_ADDR env var)")
		}
		logger.Debug("Connecting to Redis hot queue", "addr", redisAddr) // ADDED
		rdb := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		// Test the connection
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Error("Failed to connect to redis hot queue", "addr", redisAddr, "err", err) // ADDED
			return nil, fmt.Errorf("failed to connect to redis hot queue at %s: %w", redisAddr, err)
		}
		logger.Info("Connected to Redis hot queue", "addr", redisAddr) // CHANGED
		return fsqueue.NewRedisHotQueue(rdb, logger)                   // CHANGED

	default:
		return nil, fmt.Errorf("invalid hot_queue type: %s (must be 'firestore' or 'redis')", cacheType)
	}
}

// --- Production Dependency Constructors (unchanged) ---

func ensureTopics(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) error { // CHANGED
	ingressTopic := &pubsubpb.Topic{
		Name: cfg.IngressTopicID,
	}
	logger.Debug("Ensuring topic exists", "topic", cfg.IngressTopicID) // ADDED
	_, err := psClient.TopicAdminClient.CreateTopic(ctx, ingressTopic)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Topic already exists, skipping creation", "topic", cfg.IngressTopicID) // ADDED
		} else {
			logger.Error("Failed to create topic", "topic", cfg.IngressTopicID, "err", err) // CHANGED
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicID)
		}
	}

	ingressTopicDLQ := &pubsubpb.Topic{
		Name: cfg.IngressTopicDLQID,
	}
	logger.Debug("Ensuring topic exists", "topic", cfg.IngressTopicDLQID) // ADDED
	_, err = psClient.TopicAdminClient.CreateTopic(ctx, ingressTopicDLQ)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Topic already exists, skipping creation", "topic", cfg.IngressTopicDLQID) // ADDED
		} else {
			logger.Error("Failed to create topic", "topic", cfg.IngressTopicDLQID, "err", err) // CHANGED
			return fmt.Errorf("could not create topic: %s", cfg.IngressTopicDLQID)
		}
	}
	return nil
}

func newIngestionConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (messagepipeline.MessageConsumer, error) { // CHANGED
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
	logger.Debug("Ensuring subscription exists", "sub", subConfig.Name, "topic", subConfig.Topic) // ADDED
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			logger.Debug("Subscription already exists, skipping creation", "sub", subConfig.Name) // ADDED
		} else {
			logger.Error("Failed to create subscription", "sub", subConfig.Name, "err", err) // CHANGED
			return nil, fmt.Errorf("could not create sub: %s", cfg.IngressSubscriptionID)
		}
	}

	// This lib expects a zerolog.Logger. We pass a Nop logger.
	// We'll use the "Quiet" option (Option 1).
	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(subConfig.Name), psClient, zerolog.Nop(), // CHANGED
	)
}

func newPresenceCache(_ context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) { // CHANGED
	cacheType := cfg.PresenceCache.Type
	logger.Info("Initializing presence cache...", "type", cacheType) // CHANGED
	switch cacheType {
	case "firestore":
		logger.Debug("Using Firestore presence cache", "collection", cfg.PresenceCache.Firestore.MainCollectionName) // ADDED
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.MainCollectionName,
		)
	default:
		return nil, fmt.Errorf("invalid presence_cache type: %s", cacheType)
	}
}

func newFirestoreTokenFetcher(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (cache.Fetcher[urn.URN, []routing.DeviceToken], error) { // CHANGED
	logger.Debug("Initializing Firestore token fetcher", "collection", "device-tokens") // ADDED

	// This lib also expects a zerolog.Logger. We'll pass Nop.
	stringDocFetcher, err := cache.NewFirestore[string, persistence.DeviceTokenDoc](
		ctx,
		&cache.FirestoreConfig{ProjectID: cfg.ProjectID, CollectionName: "device-tokens"},
		fsClient,
		zerolog.Nop(), // CHANGED
	)
	if err != nil {
		return nil, err
	}
	stringTokenFetcher := &persistence.FirestoreTokenAdapter{DocFetcher: stringDocFetcher}
	return persistence.NewURNTokenFetcherAdapter(stringTokenFetcher), nil
}

// CORRECTED:
func newPushNotifier(cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (routing.PushNotifier, error) { // CHANGED
	logger.Debug("Initializing push notifier producer", "topic", cfg.PushNotificationsTopicID) // ADDED

	// This lib also expects a zerolog.Logger. We'll pass Nop.
	pushProducer, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, zerolog.Nop(), // CHANGED
	)
	if err != nil {
		return nil, err
	}
	return push.NewPubSubNotifier(pushProducer, logger) // CHANGED
}

type PS string

const (
	Sub PS = "subscriptions"
	Pub PS = "topics"
)

func convertPubsub(project, id string, ps PS) string {
	return fmt.Sprintf("projects/%s/%s/%s", project, ps, id)
}
