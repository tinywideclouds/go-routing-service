package config

type YamlRedisConfig struct {
	Addr string `yaml:"addr"`
}

type YamlFirestoreConfig struct {
	CollectionName string `yaml:"collection_name"`
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
	ProjectID                         string                  `yaml:"project_id"`
	RunMode                           string                  `yaml:"run_mode"`
	APIPort                           string                  `yaml:"api_port"`
	WebSocketPort                     string                  `yaml:"websocket_port"`
	IdentityServiceURL                string                  `yaml:"identity_service_url"`
	Cors                              YamlCorsConfig          `yaml:"cors"`
	PresenceCache                     YamlPresenceCacheConfig `yaml:"presence_cache"`
	IngressTopicID                    string                  `yaml:"ingress_topic_id"`
	IngressSubscriptionID             string                  `yaml:"ingress_subscription_id"`
	IngressTopicDLQID                 string                  `yaml:"ingress_topic_dlq_id"`
	DeliveryTopicID                   string                  `yaml:"delivery_topic_id"`
	PushNotificationsTopicID          string                  `yaml:"push_notifications_topic_id"`
	DeliveryBusSubscriptionExpiration string                  `yaml:"delivery_bus_sub_expiration"`
	NumPipelineWorkers                int                     `yaml:"num_pipeline_workers"` // ADDED
}

// --- Application Config Struct ---

// AppConfig is the canonical, validated configuration object used throughout the application.
type AppConfig struct {
	ProjectID                         string
	RunMode                           string
	APIPort                           string
	WebSocketPort                     string
	IdentityServiceURL                string
	Cors                              YamlCorsConfig
	PresenceCache                     YamlPresenceCacheConfig
	IngressTopicID                    string
	IngressSubscriptionID             string
	IngressTopicDLQID                 string
	DeliveryTopicID                   string
	PushNotificationsTopicID          string
	DeliveryBusSubscriptionExpiration string
	NumPipelineWorkers                int // ADDED
}
