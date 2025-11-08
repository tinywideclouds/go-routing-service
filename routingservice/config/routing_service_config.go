package config

import (
	"fmt"
	"log/slog" // IMPORTED
	"os"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
)

// --- Final Application Config Struct ---

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

// --- Stage 2 Function ---

// UpdateConfigWithEnvOverrides takes the base configuration (created from YAML)
// and completes it by applying environment variables and final validation.
// Stage 2 complete: The final runtime Config is created.
func UpdateConfigWithEnvOverrides(cfg *AppConfig, logger *slog.Logger) (*AppConfig, error) { // CHANGED
	logger.Debug("Applying environment variable overrides...") // ADDED

	// 1. Apply Environment Overrides
	if projectID := os.Getenv("GCP_PROJECT_ID"); projectID != "" {
		logger.Debug("Overriding config value", "key", "GCP_PROJECT_ID", "source", "env") // ADDED
		cfg.ProjectID = projectID
	}
	if idURL := os.Getenv("IDENTITY_SERVICE_URL"); idURL != "" {
		logger.Debug("Overriding config value", "key", "IDENTITY_SERVICE_URL", "source", "env") // ADDED
		cfg.IdentityServiceURL = idURL
	}
	if port := os.Getenv("API_PORT"); port != "" {
		logger.Debug("Overriding config value", "key", "API_PORT", "source", "env") // ADDED
		cfg.APIPort = port
	}
	if port := os.Getenv("WEBSOCKET_PORT"); port != "" {
		logger.Debug("Overriding config value", "key", "WEBSOCKET_PORT", "source", "env") // ADDED
		cfg.WebSocketPort = port
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		logger.Debug("Overriding config value", "key", "REDIS_ADDR", "source", "env") // ADDED
		// Note: This logic assumes HotQueue.Redis is non-nil.
		// If it can be nil, a check would be needed.
		// Based on your YAML, it's a struct, not a pointer, so this is safe.
		cfg.HotQueue.Redis.Addr = redisAddr
	}

	// 2. Final Validation
	if cfg.ProjectID == "" {
		logger.Error("Final config validation failed", "error", "GCP_PROJECT_ID is not set") // ADDED
		return nil, fmt.Errorf("GCP_PROJECT_ID is not set in config or env var")
	}
	if cfg.IdentityServiceURL == "" {
		logger.Error("Final config validation failed", "error", "IDENTITY_SERVICE_URL is not set") // ADDED
		return nil, fmt.Errorf("IDENTITY_SERVICE_URL is not set in config or env var")
	}
	if cfg.APIPort == "" {
		logger.Error("Final config validation failed", "error", "API_PORT is not set") // ADDED
		return nil, fmt.Errorf("API_PORT is not set in config or env var")
	}
	if cfg.WebSocketPort == "" {
		logger.Error("Final config validation failed", "error", "WEBSOCKET_PORT is not set") // ADDED
		return nil, fmt.Errorf("WEBSOCKET_PORT is not set in config or env var")
	}

	logger.Debug("Configuration finalized and validated successfully") // ADDED
	return cfg, nil
}
