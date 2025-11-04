// --- File: routingservice/config/yaml_config_test.go ---
package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import the package we are testing
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

func TestNewConfigFromYaml(t *testing.T) {
	t.Run("Success - maps all fields correctly from YAML struct", func(t *testing.T) {
		// Arrange
		// This simulates the raw struct after unmarshaling the YAML file
		yamlCfg := &config.YamlConfig{
			ProjectID:                "yaml-project",
			RunMode:                  "yaml-mode",
			APIPort:                  "8080",
			WebSocketPort:            "8081",
			IdentityServiceURL:       "http://yaml-id.com",
			ColdQueueCollection:      "yaml-cold-queue",
			IngressTopicID:           "yaml-ingress-topic",
			IngressSubscriptionID:    "yaml-ingress-sub",
			IngressTopicDLQID:        "yaml-ingress-dlq",
			PushNotificationsTopicID: "yaml-push-topic",
			NumPipelineWorkers:       5,
			Cors: config.YamlCorsConfig{
				AllowedOrigins: []string{"http://yaml-origin.com"},
				Role:           "yaml-role",
			},
			PresenceCache: config.YamlPresenceCacheConfig{
				Type: "firestore",
				Firestore: config.YamlFirestoreConfig{
					MainCollectionName: "yaml-presence",
				},
			},
			HotQueue: config.YamlHotQueueConfig{
				Type: "redis",
				Redis: config.YamlRedisConfig{
					Addr: "yaml-redis:6379",
				},
			},
		}

		// Act
		// This is the "Stage 1" function
		cfg, err := config.NewConfigFromYaml(yamlCfg)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// Check that all fields were mapped 1:1
		assert.Equal(t, "yaml-project", cfg.ProjectID)
		assert.Equal(t, "yaml-mode", cfg.RunMode)
		assert.Equal(t, "8080", cfg.APIPort)
		assert.Equal(t, "8081", cfg.WebSocketPort)
		assert.Equal(t, "http://yaml-id.com", cfg.IdentityServiceURL)
		assert.Equal(t, "yaml-cold-queue", cfg.ColdQueueCollection)
		assert.Equal(t, "yaml-ingress-topic", cfg.IngressTopicID)
		assert.Equal(t, "yaml-ingress-sub", cfg.IngressSubscriptionID)
		assert.Equal(t, "yaml-ingress-dlq", cfg.IngressTopicDLQID)
		assert.Equal(t, "yaml-push-topic", cfg.PushNotificationsTopicID)
		assert.Equal(t, 5, cfg.NumPipelineWorkers)
		assert.Equal(t, []string{"http://yaml-origin.com"}, cfg.Cors.AllowedOrigins)
		assert.Equal(t, "yaml-role", cfg.Cors.Role)
		assert.Equal(t, "firestore", cfg.PresenceCache.Type)
		assert.Equal(t, "yaml-presence", cfg.PresenceCache.Firestore.MainCollectionName)
		assert.Equal(t, "redis", cfg.HotQueue.Type)
		assert.Equal(t, "yaml-redis:6379", cfg.HotQueue.Redis.Addr)
	})
}
