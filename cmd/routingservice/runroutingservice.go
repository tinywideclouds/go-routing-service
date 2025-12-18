/*
File: cmd/runroutingservice/runroutingservice.go
Description: Main entrypoint. Wires up the Ephemeral Poke Subscription for the "Fan-Out" architecture.
*/
package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/redis/go-redis/v9"
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	urn "github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-routing-service/internal/app"
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
)

//go:embed config.yaml
var configFile []byte

func main() {
	apiFlag := flag.Bool("api", false, "Start the API Service")
	wsFlag := flag.Bool("ws", false, "Start the WebSocket Service")
	flag.Parse()

	runAPI := *apiFlag
	runWS := *wsFlag

	if !runAPI && !runWS {
		runAPI = true
		runWS = true
	}

	// --- Setup Logger ---
	var logLevel slog.Level
	switch os.Getenv("LOG_LEVEL") {
	case "debug", "DEBUG":
		logLevel = slog.LevelDebug
	default:
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})).With("service", "go-routing-service")
	slog.SetDefault(logger)

	// --- Load Config ---
	var yamlCfg config.YamlConfig
	if err := yaml.Unmarshal(configFile, &yamlCfg); err != nil {
		logger.Error("Failed to unmarshal config", "err", err)
		os.Exit(1)
	}
	baseCfg, err := config.NewConfigFromYaml(&yamlCfg, logger)
	if err != nil {
		logger.Error("Failed to build base config", "err", err)
		os.Exit(1)
	}
	cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)
	if err != nil {
		logger.Error("Config validation failed", "err", err)
		os.Exit(1)
	}

	// Port Override for Cloud Run
	if port := os.Getenv("PORT"); port != "" {
		if runAPI {
			cfg.APIPort = port
		} else if runWS {
			cfg.WebSocketPort = port
		}
	}

	// Ensure GCP Resource Names are fully qualified
	cfg.IngressTopicID = convertPubsub(cfg.ProjectID, cfg.IngressTopicID, Pub)
	cfg.IngressSubscriptionID = convertPubsub(cfg.ProjectID, cfg.IngressSubscriptionID, Sub)
	cfg.IngressTopicDLQID = convertPubsub(cfg.ProjectID, cfg.IngressTopicDLQID, Pub)
	cfg.PushNotificationsTopicID = convertPubsub(cfg.ProjectID, cfg.PushNotificationsTopicID, Pub)

	ctx := context.Background()

	// --- Create Core Dependencies ---
	// We return psClient to allow main() to manage the ephemeral subscription.
	deps, psClient, err := newDependencies(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize dependencies", "err", err)
		os.Exit(1)
	}

	// --- Auth Middleware ---
	jwksURL, err := middleware.DiscoverAndValidateJWTConfig(cfg.IdentityServiceURL, middleware.RSA256, logger)
	if err != nil {
		logger.Error("OIDC Discovery failed", "err", err)
		os.Exit(1)
	}

	var httpAuth func(http.Handler) http.Handler
	if runAPI {
		if httpAuth, err = middleware.NewJWKSAuthMiddleware(jwksURL, logger); err != nil {
			logger.Error("HTTP Auth init failed", "err", err)
			os.Exit(1)
		}
	}

	var wsAuth func(http.Handler) http.Handler
	if runWS {
		if wsAuth, err = middleware.NewJWKSWebsocketAuthMiddleware(jwksURL, logger); err != nil {
			logger.Error("WS Auth init failed", "err", err)
			os.Exit(1)
		}
	}

	// --- Services ---
	var apiService *routingservice.Wrapper
	if runAPI {
		apiService, err = routingservice.New(cfg, deps, httpAuth, logger.With("component", "ApiService"))
		if err != nil {
			logger.Error("API Service init failed", "err", err)
			os.Exit(1)
		}
	}

	var connManager *realtime.ConnectionManager
	if runWS {
		// --- POKE LISTENER SETUP (The Fan-Out) ---
		// 1. Generate unique subscription ID for this instance
		instanceID := uuid.NewString()
		pokeSubID := fmt.Sprintf("poke-sub-%s", instanceID)
		pokeSubName := convertPubsub(cfg.ProjectID, pokeSubID, Sub)

		logger.Info("Creating ephemeral poke subscription", "sub", pokeSubName)

		// 2. Create the subscription on Pub/Sub
		_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:  pokeSubName,
			Topic: cfg.PushNotificationsTopicID,
			// Expiration Policy: Never expire while active.
			ExpirationPolicy: &pubsubpb.ExpirationPolicy{Ttl: nil},
		})
		if err != nil {
			logger.Error("Failed to create poke subscription", "err", err)
			os.Exit(1)
		}

		// 3. Ensure cleanup on shutdown
		defer func() {
			logger.Info("Deleting ephemeral poke subscription", "sub", pokeSubName)
			if err := psClient.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{Subscription: pokeSubName}); err != nil {
				logger.Warn("Failed to delete poke subscription", "err", err)
			}
		}()

		// 4. Create the Consumer attached to this ephemeral subscription
		pokeConsumer, err := messagepipeline.NewGooglePubsubConsumer(
			messagepipeline.NewGooglePubsubConsumerDefaults(pokeSubName),
			psClient,
			logger,
		)
		if err != nil {
			logger.Error("Poke Consumer init failed", "err", err)
			os.Exit(1)
		}

		connManager, err = realtime.NewConnectionManager(
			":"+cfg.WebSocketPort,
			wsAuth,
			deps.PresenceCache,
			deps.MessageQueue,
			pokeConsumer, // Injected here
			logger.With("component", "ConnManager"),
		)
		if err != nil {
			logger.Error("Connection Manager init failed", "err", err)
			os.Exit(1)
		}
	}

	app.Run(ctx, logger, apiService, connManager)
}

// newDependencies builds the service dependency container.
func newDependencies(ctx context.Context, cfg *config.AppConfig, logger *slog.Logger) (*routing.ServiceDependencies, *pubsub.Client, error) {
	logger.Debug("Connecting to Firestore", "project_id", cfg.ProjectID)
	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to firestore: %w", err)
	}

	logger.Debug("Connecting to PubSub", "project_id", cfg.ProjectID)
	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to pubsub: %w", err)
	}

	if err := ensureTopics(ctx, cfg, psClient, logger); err != nil {
		return nil, nil, err
	}

	coldQueue, err := fsqueue.NewFirestoreColdQueue(fsClient, cfg.ColdQueueCollection, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create cold queue: %w", err)
	}

	hotQueue, err := newHotQueue(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create hot queue: %w", err)
	}

	messageQueue, err := queue.NewCompositeMessageQueue(hotQueue, coldQueue, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create composite queue: %w", err)
	}

	ingestProducer := psub.NewProducer(psClient.Publisher(cfg.IngressTopicID))

	presenceCache, err := newPresenceCache(ctx, cfg, fsClient, logger)
	if err != nil {
		return nil, nil, err
	}

	ingestConsumer, err := newIngestionConsumer(ctx, cfg, psClient, logger)
	if err != nil {
		return nil, nil, err
	}

	pushNotifier, err := newPushNotifier(cfg, psClient, logger)
	if err != nil {
		return nil, nil, err
	}

	return &routing.ServiceDependencies{
		IngestionProducer: ingestProducer,
		IngestionConsumer: ingestConsumer,
		MessageQueue:      messageQueue,
		PresenceCache:     presenceCache,
		PushNotifier:      pushNotifier,
	}, psClient, nil
}

// ... Helpers ...

func newHotQueue(ctx context.Context, cfg *config.AppConfig, fsClient *firestore.Client, logger *slog.Logger) (queue.HotQueue, error) {
	cacheType := cfg.HotQueue.Type
	switch cacheType {
	case "firestore":
		return fsqueue.NewFirestoreHotQueue(
			fsClient,
			cfg.HotQueue.Firestore.MainCollectionName,
			cfg.HotQueue.Firestore.PendingCollectionName,
			logger,
		)
	case "redis":
		rdb := redis.NewClient(&redis.Options{Addr: cfg.HotQueue.Redis.Addr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		return fsqueue.NewRedisHotQueue(rdb, logger)
	default:
		return nil, fmt.Errorf("invalid hot_queue type")
	}
}

func ensureTopics(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, _ *slog.Logger) error {
	topics := []string{cfg.IngressTopicID, cfg.IngressTopicDLQID, cfg.PushNotificationsTopicID}
	for _, t := range topics {
		_, err := psClient.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: t})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("failed to create topic %s: %w", t, err)
		}
	}
	return nil
}

func newIngestionConsumer(ctx context.Context, cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (messagepipeline.MessageConsumer, error) {
	subName := cfg.IngressSubscriptionID
	_, err := psClient.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: cfg.IngressTopicID,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, err
	}
	return messagepipeline.NewGooglePubsubConsumer(
		messagepipeline.NewGooglePubsubConsumerDefaults(subName), psClient, logger,
	)
}

func newPresenceCache(_ context.Context, cfg *config.AppConfig, fsClient *firestore.Client, _ *slog.Logger) (cache.PresenceCache[urn.URN, routing.ConnectionInfo], error) {
	if cfg.PresenceCache.Type == "firestore" {
		return cache.NewFirestorePresenceCache[urn.URN, routing.ConnectionInfo](
			fsClient,
			cfg.PresenceCache.Firestore.MainCollectionName,
		)
	}
	// Add other cache types if needed
	return nil, fmt.Errorf("invalid presence type")
}

func newPushNotifier(cfg *config.AppConfig, psClient *pubsub.Client, logger *slog.Logger) (routing.PushNotifier, error) {
	prod, err := messagepipeline.NewGooglePubsubProducer(
		messagepipeline.NewGooglePubsubProducerDefaults(cfg.PushNotificationsTopicID), psClient, logger,
	)
	if err != nil {
		return nil, err
	}
	return push.NewPubSubNotifier(prod, logger)
}

type PS string

const (
	Sub PS = "subscriptions"
	Pub PS = "topics"
)

func convertPubsub(project, id string, ps PS) string {
	if strings.HasPrefix(id, "projects/") {
		return id
	}
	return fmt.Sprintf("projects/%s/%s/%s", project, ps, id)
}
