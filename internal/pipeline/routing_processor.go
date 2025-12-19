package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

const priorityHigh = 5

func NewRoutingProcessor(deps *routing.ServiceDependencies, cfg *config.AppConfig, logger *slog.Logger) messagepipeline.StreamProcessor[secure.SecureEnvelope] {
	return func(ctx context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
		recipientURN := envelope.RecipientID

		// Generate the Definitive Message ID here.
		// This ID will persist through Hot Queue, Cold Queue, and Client Ack.
		messageID := uuid.NewString()

		procLogger := logger.With(
			"recipient_id", recipientURN.String(),
			"msg_id", msg.ID,
			"routing_id", messageID, // Log the new internal ID
			"priority", envelope.Priority,
		)

		// 1. Check Presence (Multi-Session Aware)
		// We verify if ANY session exists for the user.
		sessions, err := deps.PresenceCache.FetchSessions(ctx, recipientURN)
		isOnline := (err == nil && len(sessions) > 0)

		// --- EXPRESS LANE (High Priority) ---
		if envelope.Priority >= priorityHigh {
			procLogger.Info("Processing HIGH PRIORITY message (Express Lane)")

			if err := deps.MessageQueue.EnqueueHot(ctx, messageID, envelope); err != nil {
				procLogger.Error("Failed to enqueue High Priority message", "err", err)
				return fmt.Errorf("failed to enqueue High Priority message: %w", err)
			}

			// In Express Lane, we notify regardless, but tailor the notification type.
			if isOnline {
				procLogger.Debug("Express Lane: User is online, sending poke")
				_ = deps.PushNotifier.PokeOnline(ctx, recipientURN)
			} else {
				procLogger.Debug("Express Lane: User is offline, sending push")
				_ = deps.PushNotifier.NotifyOffline(ctx, envelope)
			}
			return nil
		}

		// --- STANDARD LANE ---
		if isOnline {
			procLogger.Info("User is online. Routing message to HOT queue.")
			if err := deps.MessageQueue.EnqueueHot(ctx, messageID, envelope); err != nil {
				return fmt.Errorf("failed to enqueue message: %w", err)
			}
			// This poke triggers the "Fan-Out" in PubSub, which reaches the ConnectionManager.
			_ = deps.PushNotifier.PokeOnline(ctx, recipientURN)
			return nil
		}

		// Offline Drops
		if envelope.IsEphemeral {
			procLogger.Info("User is offline and message is ephemeral. Dropping message.")
			return nil
		}

		// Offline Cold Path
		procLogger.Info("User is offline. Routing message to COLD queue.")

		if err := deps.PushNotifier.NotifyOffline(ctx, envelope); err != nil {
			procLogger.Warn("Failed to send offline notification", "err", err)
		}

		if err := deps.MessageQueue.EnqueueCold(ctx, messageID, envelope); err != nil {
			procLogger.Error("Failed to store message in cold queue", "err", err)
			return fmt.Errorf("failed to store message in cold queue: %w", err)
		}

		return nil
	}
}
