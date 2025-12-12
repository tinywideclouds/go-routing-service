// --- File: internal/pipeline/routing_processor.go ---
package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

const priorityHigh = 5

func NewRoutingProcessor(deps *routing.ServiceDependencies, cfg *config.AppConfig, logger *slog.Logger) messagepipeline.StreamProcessor[secure.SecureEnvelope] {
	return func(ctx context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
		recipientURN := envelope.RecipientID
		procLogger := logger.With(
			"recipient_id", recipientURN.String(),
			"msg_id", msg.ID,
			"priority", envelope.Priority,
		)

		// 1. Check Presence
		_, err := deps.PresenceCache.Fetch(ctx, recipientURN)
		isOnline := (err == nil)

		// --- EXPRESS LANE (High Priority) ---
		if envelope.Priority >= priorityHigh {
			procLogger.Info("Processing HIGH PRIORITY message (Express Lane)")

			if err := deps.MessageQueue.EnqueueHot(ctx, envelope); err != nil {
				procLogger.Error("Failed to enqueue High Priority message", "err", err)
				return fmt.Errorf("failed to enqueue High Priority message: %w", err)
			}

			if isOnline {
				procLogger.Debug("Express Lane: User is online, sending poke")
				_ = deps.PushNotifier.PokeOnline(ctx, recipientURN)
			} else {
				procLogger.Debug("Express Lane: User is offline, sending push")
				// REFACTORED: Direct call, no token fetching
				_ = deps.PushNotifier.NotifyOffline(ctx, envelope)
			}
			return nil
		}

		// --- STANDARD LANE ---
		if isOnline {
			procLogger.Info("User is online. Routing message to HOT queue.")
			if err := deps.MessageQueue.EnqueueHot(ctx, envelope); err != nil {
				return fmt.Errorf("failed to enqueue message: %w", err)
			}
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

		// REFACTORED: Direct call
		if err := deps.PushNotifier.NotifyOffline(ctx, envelope); err != nil {
			procLogger.Warn("Failed to send offline notification", "err", err)
			// Non-critical error, continue to enqueue
		}

		if err := deps.MessageQueue.EnqueueCold(ctx, envelope); err != nil {
			procLogger.Error("Failed to store message in cold queue", "err", err)
			return fmt.Errorf("failed to store message in cold queue: %w", err)
		}

		return nil
	}
}
