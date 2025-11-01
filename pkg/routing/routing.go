/*
File: pkg/routing/routing.go
Description: NEW FILE. Consolidates core domain types and the
ServiceDependencies struct (moved from config_routing.go).
*/
package routing

import (
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
)

// ConnectionInfo holds details about a user's real-time connection.
type ConnectionInfo struct {
	ServerInstanceID string `json:"serverInstanceId"`
	ConnectedAt      int64  `json:"connectedAt"`
}

// DeviceToken represents a push notification token for a user's device.
type DeviceToken struct {
	Token    string `json:"token"`
	Platform string `json:"platform"` // e.g., "ios", "android"
}

// ServiceDependencies holds all the external services the routing service needs to operate.
// (Formerly 'Dependencies' in config_routing.go)
type ServiceDependencies struct {
	// --- Producers ---
	IngestionProducer IngestionProducer

	// --- Consumers ---
	IngestionConsumer messagepipeline.MessageConsumer

	// --- Storage & Caches ---
	MessageQueue       queue.MessageQueue
	PresenceCache      cache.PresenceCache[urn.URN, ConnectionInfo]
	DeviceTokenFetcher cache.Fetcher[urn.URN, []DeviceToken]

	// --- Notifiers ---
	PushNotifier PushNotifier
}
