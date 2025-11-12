// --- File: routingservice/config/routing_service_config_test.go ---
package config_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import the package we are testing
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

// newBaseConfig creates a mock "Stage 1" config,
// simulating what NewConfigFromYaml would produce.
func newBaseConfig() *config.AppConfig {

	return &config.AppConfig{
		ProjectID:          "base-project",
		RunMode:            "base-mode",
		APIPort:            "9090",
		WebSocketPort:      "9091",
		IdentityServiceURL: "http://base-id.com",
		HotQueue: config.YamlHotQueueConfig{
			Type: "redis",
			Redis: config.YamlRedisConfig{
				Addr: "base-redis:6379",
			},
		},
		NumPipelineWorkers: 1, // Non-overridden value
	}
}

func TestUpdateConfigWithEnvOverrides(t *testing.T) {
	logger := newTestLogger()
	t.Run("Success - All overrides applied", func(t *testing.T) {
		// Arrange
		baseCfg := newBaseConfig()

		// Set all environment variables to override
		t.Setenv("GCP_PROJECT_ID", "env-project")
		t.Setenv("IDENTITY_SERVICE_URL", "http://env-id.com")
		t.Setenv("API_PORT", "8000")
		t.Setenv("WEBSOCKET_PORT", "8001")
		t.Setenv("REDIS_ADDR", "env-redis:6379")

		// Act
		// This is the "Stage 2" function
		cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Check that overrides were applied
		assert.Equal(t, "env-project", cfg.ProjectID)
		assert.Equal(t, "http://env-id.com", cfg.IdentityServiceURL)
		assert.Equal(t, "8000", cfg.APIPort)
		assert.Equal(t, "8001", cfg.WebSocketPort)
		assert.Equal(t, "env-redis:6379", cfg.HotQueue.Redis.Addr)

		// Check that non-overridden fields remain
		assert.Equal(t, "base-mode", cfg.RunMode)
		assert.Equal(t, 1, cfg.NumPipelineWorkers)
		assert.Equal(t, "redis", cfg.HotQueue.Type) // Type wasn't overridden
	})

	t.Run("Success - Overrides from empty base", func(t *testing.T) {
		// Arrange
		// Start with an empty config, as if YAML was minimal
		emptyCfg := &config.AppConfig{}

		// Set all required env vars
		t.Setenv("GCP_PROJECT_ID", "env-project")
		t.Setenv("IDENTITY_SERVICE_URL", "http://env-id.com")
		t.Setenv("API_PORT", "8000")
		t.Setenv("WEBSOCKET_PORT", "8001")
		// Note: REDIS_ADDR is not strictly required by validation

		// Act
		cfg, err := config.UpdateConfigWithEnvOverrides(emptyCfg, logger)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "env-project", cfg.ProjectID)
		assert.Equal(t, "http://env-id.com", cfg.IdentityServiceURL)
		assert.Equal(t, "8000", cfg.APIPort)
		assert.Equal(t, "8001", cfg.WebSocketPort)
	})

	t.Run("Failure - Missing required GCP_PROJECT_ID", func(t *testing.T) {
		// Arrange
		baseCfg := newBaseConfig()
		baseCfg.ProjectID = "" // Simulate it being empty from YAML
		os.Unsetenv("GCP_PROJECT_ID")

		// Act
		cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "GCP_PROJECT_ID is not set")
	})

	t.Run("Failure - Missing required IDENTITY_SERVICE_URL", func(t *testing.T) {
		// Arrange
		baseCfg := newBaseConfig()
		baseCfg.IdentityServiceURL = "" // Simulate it being empty from YAML
		os.Unsetenv("IDENTITY_SERVICE_URL")

		// Act
		cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "IDENTITY_SERVICE_URL is not set")
	})

	t.Run("Failure - Missing required API_PORT", func(t *testing.T) {
		// Arrange
		baseCfg := newBaseConfig()
		baseCfg.APIPort = "" // Simulate it being empty from YAML
		os.Unsetenv("API_PORT")

		// Act
		cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, cfg)

		assert.Contains(t, err.Error(), "API_PORT is not set")
	})

	t.Run("Failure - Missing required WEBSOCKET_PORT", func(t *testing.T) {
		// Arrange
		baseCfg := newBaseConfig()
		baseCfg.WebSocketPort = "" // Simulate it being empty from YAML
		err := os.Unsetenv("WEBSOCKET_PORT")
		assert.NoError(t, err)

		// Act
		cfg, err := config.UpdateConfigWithEnvOverrides(baseCfg, logger)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "WEBSOCKET_PORT is not set")
	})
}
