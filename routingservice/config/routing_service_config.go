// --- File: routingservice/config/routing_service_config.go ---
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
)

// AppConfig is the canonical, validated configuration object used throughout the application.
// It is created by NewConfigFromYaml (Stage 1) and finalized by
// UpdateConfigWithEnvOverrides (Stage 2).
type AppConfig struct {
	ProjectID                string
	RunMode                  string
	APIPort                  string
	WebSocketPort            string
	IdentityServiceURL       string
	CorsConfig               middleware.CorsConfig
	PresenceCache            YamlPresenceCacheConfig
	HotQueue                 YamlHotQueueConfig
	ColdQueueCollection      string
	IngressTopicID           string
	IngressSubscriptionID    string
	IngressTopicDLQID        string
	PushNotificationsTopicID string
	NumPipelineWorkers       int
}

// UpdateConfigWithEnvOverrides takes the base configuration (created from YAML)
// and completes it by applying environment variables and final validation.
// This function completes "Stage 2" of configuration loading.
func UpdateConfigWithEnvOverrides(cfg *AppConfig, logger *slog.Logger) (*AppConfig, error) {
	logger.Debug("Applying environment variable overrides...")

	// 1. Apply Environment Overrides
	if projectID := os.Getenv("GCP_PROJECT_ID"); projectID != "" {
		logger.Debug("Overriding config value", "key", "GCP_PROJECT_ID", "source", "env")
		cfg.ProjectID = projectID
	}
	if idURL := os.Getenv("IDENTITY_SERVICE_URL"); idURL != "" {
		logger.Debug("Overriding config value", "key", "IDENTITY_SERVICE_URL", "source", "env")
		cfg.IdentityServiceURL = idURL
	}
	if port := os.Getenv("API_PORT"); port != "" {
		logger.Debug("Overriding config value", "key", "API_PORT", "source", "env")
		cfg.APIPort = port
	}

	if port := os.Getenv("WEBSOCKET_PORT"); port != "" {
		logger.Debug("Overriding config value", "key", "WEBSOCKET_PORT", "source", "env")
		cfg.WebSocketPort = port
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		logger.Debug("Overriding config value", "key", "REDIS_ADDR", "source", "env")
		cfg.HotQueue.Redis.Addr = redisAddr
	}

	if corsOrigins := os.Getenv("CORS_ALLOWED_ORIGINS"); corsOrigins != "" {
		logger.Debug("Overriding config value", "key", "CORS_ALLOWED_ORIGINS", "source", "env")
		// Split by comma and trim spaces
		rawOrigins := strings.Split(corsOrigins, ",")
		var cleanOrigins []string
		for _, o := range rawOrigins {
			if trimmed := strings.TrimSpace(o); trimmed != "" {
				cleanOrigins = append(cleanOrigins, trimmed)
			}
		}
		cfg.CorsConfig.AllowedOrigins = cleanOrigins
	}

	// 2. Final Validation
	// REMOVED: Port validation is now handled in main.go based on active flags.

	if cfg.ProjectID == "" {
		logger.Error("Final config validation failed", "error", "GCP_PROJECT_ID is not set")
		return nil, fmt.Errorf("GCP_PROJECT_ID is not set in config or env var")
	}
	if cfg.IdentityServiceURL == "" {
		logger.Error("Final config validation failed", "error", "IDENTITY_SERVICE_URL is not set")
		return nil, fmt.Errorf("IDENTITY_SERVICE_URL is not set in config or env var")
	}

	logger.Debug("Configuration finalized and validated successfully")
	return cfg, nil
}
