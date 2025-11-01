package config

type YamlRedisConfig struct {
	Addr string `yaml:"addr"`
}

// YamlFirestoreConfig now supports the two-collection model
type YamlFirestoreConfig struct {
	MainCollectionName    string `yaml:"main_collection_name"`
	PendingCollectionName string `yaml:"pending_collection_name"`
}

// YamlHotQueueConfig now holds both types
type YamlHotQueueConfig struct {
	Type      string              `yaml:"type"` // "firestore" or "redis"
	Redis     YamlRedisConfig     `yaml:"redis"`
	Firestore YamlFirestoreConfig `yaml:"firestore"`
}

type YamlPresenceCacheConfig struct {
	Type      string              `yaml:"type"`
	Redis     YamlRedisConfig     `yaml:"redis"`
	Firestore YamlFirestoreConfig `yaml:"firestore"` // Note: This Firestore config is simpler
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

// --- Application Config Struct ---

// AppConfig is the canonical, validated configuration object used throughout the application.
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
