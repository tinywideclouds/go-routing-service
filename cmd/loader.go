package cmd

import (
	_ "embed" // Required for go:embed
	"fmt"

	"github.com/tinywideclouds/go-routing-service/routingservice/config"
	"gopkg.in/yaml.v3"
)

//go:embed prod/config.yaml
var configFile []byte

// Load parses the embedded configuration file for the service.
func Load() (*config.AppConfig, error) {
	var yamlCfg config.YamlConfig
	if err := yaml.Unmarshal(configFile, &yamlCfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal embedded yaml config: %w", err)
	}

	// This mapping is now 1:1, as AppConfig matches YamlConfig
	appCfg := &config.AppConfig{
		ProjectID:                yamlCfg.ProjectID,
		RunMode:                  yamlCfg.RunMode,
		APIPort:                  yamlCfg.APIPort,
		WebSocketPort:            yamlCfg.WebSocketPort,
		IdentityServiceURL:       yamlCfg.IdentityServiceURL,
		Cors:                     yamlCfg.Cors,
		PresenceCache:            yamlCfg.PresenceCache,
		HotQueue:                 yamlCfg.HotQueue,
		ColdQueueCollection:      yamlCfg.ColdQueueCollection,
		IngressTopicID:           yamlCfg.IngressTopicID,
		IngressSubscriptionID:    yamlCfg.IngressSubscriptionID,
		IngressTopicDLQID:        yamlCfg.IngressTopicDLQID,
		PushNotificationsTopicID: yamlCfg.PushNotificationsTopicID,
		NumPipelineWorkers:       yamlCfg.NumPipelineWorkers,
	}

	return appCfg, nil
}
