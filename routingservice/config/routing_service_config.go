package config

import (
	"fmt"
	"os"
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
	Cors                     YamlCorsConfig
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
func UpdateConfigWithEnvOverrides(cfg *AppConfig) (*AppConfig, error) {
	// 1. Apply Environment Overrides
	if projectID := os.Getenv("GCP_PROJECT_ID"); projectID != "" {
		cfg.ProjectID = projectID
	}
	if idURL := os.Getenv("IDENTITY_SERVICE_URL"); idURL != "" {
		cfg.IdentityServiceURL = idURL
	}
	if port := os.Getenv("API_PORT"); port != "" {
		cfg.APIPort = port
	}
	if port := os.Getenv("WEBSOCKET_PORT"); port != "" {
		cfg.WebSocketPort = port
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		// Note: This logic assumes HotQueue.Redis is non-nil.
		// If it can be nil, a check would be needed.
		// Based on your YAML, it's a struct, not a pointer, so this is safe.
		cfg.HotQueue.Redis.Addr = redisAddr
	}

	// 2. Final Validation
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID is not set in config or env var")
	}
	if cfg.IdentityServiceURL == "" {
		return nil, fmt.Errorf("IDENTITY_SERVICE_URL is not set in config or env var")
	}
	if cfg.APIPort == "" {
		return nil, fmt.Errorf("API_PORT is not set in config or env var")
	}
	if cfg.WebSocketPort == "" {
		return nil, fmt.Errorf("WEBSOCKET_PORT is not set in config or env var")
	}

	return cfg, nil
}
