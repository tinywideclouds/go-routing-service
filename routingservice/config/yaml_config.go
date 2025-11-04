package config

// --- YAML-Specific Structs ---

type YamlRedisConfig struct {
	Addr string `yaml:"addr"`
}

type YamlFirestoreConfig struct {
	MainCollectionName    string `yaml:"main_collection_name"`
	PendingCollectionName string `yaml:"pending_collection_name"`
}

type YamlHotQueueConfig struct {
	Type      string              `yaml:"type"` // "firestore" or "redis"
	Redis     YamlRedisConfig     `yaml:"redis"`
	Firestore YamlFirestoreConfig `yaml:"firestore"`
}

type YamlPresenceCacheConfig struct {
	Type      string              `yaml:"type"`
	Redis     YamlRedisConfig     `yaml:"redis"`
	Firestore YamlFirestoreConfig `yaml:"firestore"`
}

type YamlCorsConfig struct {
	AllowedOrigins []string `yaml:"allowed_origins"`
	Role           string   `yaml:"role"`
}

// YamlConfig defines the structure for unmarshaling the embedded config.yaml file.
type YamlConfig struct {
	ProjectID                string                  `yaml:"project_id"`
	RunMode                  string                  `yaml:"run_mode"`
	APIPort                  string                  `yaml:"api_port"`
	WebSocketPort            string                  `yaml:"websocket_port"`
	IdentityServiceURL       string                  `yaml:"identity_service_url"`
	Cors                     YamlCorsConfig          `yaml:"cors"`
	PresenceCache            YamlPresenceCacheConfig `yaml:"presence_cache"`
	HotQueue                 YamlHotQueueConfig      `yaml:"hot_queue"`
	ColdQueueCollection      string                  `yaml:"cold_queue_collection"`
	IngressTopicID           string                  `yaml:"ingress_topic_id"`
	IngressSubscriptionID    string                  `yaml:"ingress_subscription_id"`
	IngressTopicDLQID        string                  `yaml:"ingress_topic_dlq_id"`
	PushNotificationsTopicID string                  `yaml:"push_notifications_topic_id"`
	NumPipelineWorkers       int                     `yaml:"num_pipeline_workers"`
}

// --- Stage 1 Function ---

// NewConfigFromYaml converts the raw unmarshaled data (YamlConfig) into a clean, base AppConfig struct.
// Stage 1 complete: The AppConfig struct now exists, but without environment overrides.
func NewConfigFromYaml(yamlCfg *YamlConfig) (*AppConfig, error) {
	// This mapping is 1:1, as AppConfig matches YamlConfig
	appCfg := &AppConfig{
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
