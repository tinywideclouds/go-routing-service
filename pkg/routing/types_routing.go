// Package routing contains the public domain models, interfaces, and configuration
// for the routing service. It defines the contract for interacting with the service.
package routing

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
