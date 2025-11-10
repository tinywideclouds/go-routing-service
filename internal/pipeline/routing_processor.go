/*
File: internal/pipeline/routing_processor.go
Description: REFACTORED to use the new 'queue.MessageQueue' and
the explicit PushNotifier.PokeOnline/NotifyOffline methods.
*/
package pipeline

import (
	"context"
	"fmt"
	"log/slog" // IMPORTED

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	// "github.com/rs/zerolog" // REMOVED
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

// NewRoutingProcessor creates the main message handler (StreamProcessor) for the routing pipeline.
// It determines user presence and routes messages to the correct hot/cold queue.
func NewRoutingProcessor(deps *routing.ServiceDependencies, cfg *config.AppConfig, logger *slog.Logger) messagepipeline.StreamProcessor[secure.SecureEnvelope] { // CHANGED
	return func(ctx context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
		recipientURN := envelope.RecipientID
		procLogger := logger.With("recipient_id", recipientURN.String(), "msg_id", msg.ID) // CHANGED

		// 1. Check if the user is online via the presence cache.
		if _, err := deps.PresenceCache.Fetch(ctx, recipientURN); err == nil {
			// --- HOT PATH ---
			procLogger.Info("User is online. Routing message to HOT queue.") // CHANGED
			if err := deps.MessageQueue.EnqueueHot(ctx, envelope); err != nil {
				// If EnqueueHot fails, it automatically falls back to cold.
				// If *that* fails, the error is returned.
				procLogger.Error("Failed to enqueue message (hot and cold fallback)", "err", err) // ADDED
				return fmt.Errorf("failed to enqueue message (hot and cold fallback): %w", err)
			}

			// --- START OF REFACTOR ---
			// After successful enqueue, send a "poke" notification.
			procLogger.Debug("Sending 'poke' notification")
			if err := deps.PushNotifier.PokeOnline(ctx, recipientURN); err != nil {
				// Non-critical, just log it. The message is already queued.
				procLogger.Warn("Failed to send online 'poke' notification", "err", err)
			}
			// --- END OF REFACTOR ---
			return nil
		}

		// --- COLD PATH ---
		// 2. User is offline. Fetch their device tokens for push notifications.
		procLogger.Info("User is offline. Checking for push notification tokens.") // CHANGED
		tokens, err := deps.DeviceTokenFetcher.Fetch(ctx, recipientURN)
		if err != nil {
			procLogger.Warn("Failed to fetch device tokens. Message will be stored but no push will be sent.", "err", err) // CHANGED
			// Non-critical. We must still store the message.
		}

		// 3. Separate tokens for mobile.
		var mobileTokens []routing.DeviceToken
		for _, token := range tokens {
			if token.Platform == "ios" || token.Platform == "android" {
				mobileTokens = append(mobileTokens, token)
			}
		}

		// 4. Send mobile notifications.
		if len(mobileTokens) > 0 {
			procLogger.Info("Routing notification to push notification service", "count", len(mobileTokens)) // CHANGED

			// --- START OF REFACTOR ---
			// We send the *full* envelope here for a rich push.
			if err := deps.PushNotifier.NotifyOffline(ctx, mobileTokens, envelope); err != nil {
				procLogger.Error("Push notifier failed. Message will be stored, but this error is logged.", "err", err)
				// Non-critical. We still must store.
			}
			// --- END OF REFACTOR ---

		} else {
			procLogger.Debug("User is offline but has no mobile tokens. Storing in cold queue only.") // ADDED
		}

		// 5. Finally, store the message in the COLD queue.
		procLogger.Info("Storing message in COLD queue for later retrieval.") // CHANGED
		if err := deps.MessageQueue.EnqueueCold(ctx, envelope); err != nil {
			// This is a critical error. Return it to trigger a NACK.
			procLogger.Error("Failed to store message in cold queue", "err", err) // ADDED
			return fmt.Errorf("failed to store message in cold queue: %w", err)
		}
		return nil
	}
}
