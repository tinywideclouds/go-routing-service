// --- File: pkg/routing/routing.go ---
// Package routing consolidates core domain types and service dependency
// definitions for the routing service.
package routing

import (
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"

	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
)

// ConnectionInfo holds details about a user's real-time connection.
// This is stored in the presence cache.
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
// This struct is used for dependency injection.
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
