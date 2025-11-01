/*
File: internal/pipeline/routing_processor.go
Description: REFACTORED to use the new 'queue.MessageQueue' and implement
the 'Hot/Cold' queue logic based on user presence.
*/
package pipeline

import (
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/routingservice/config"
)

// NewRoutingProcessor creates the main message handler (StreamProcessor) for the routing pipeline.
// It determines user presence and routes messages to the correct hot/cold queue.
func NewRoutingProcessor(deps *routing.ServiceDependencies, cfg *config.AppConfig, logger zerolog.Logger) messagepipeline.StreamProcessor[secure.SecureEnvelope] {
	return func(ctx context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
		recipientURN := envelope.RecipientID
		procLogger := logger.With().Str("recipient_id", recipientURN.String()).Logger()

		// 1. Check if the user is online via the presence cache.
		if _, err := deps.PresenceCache.Fetch(ctx, recipientURN); err == nil {
			// --- REFACTORED: HOT PATH ---
			procLogger.Info().Msg("User is online. Routing message to HOT queue.")
			if err := deps.MessageQueue.EnqueueHot(ctx, envelope); err != nil {
				// If EnqueueHot fails, it automatically falls back to cold.
				// If *that* fails, the error is returned.
				return fmt.Errorf("failed to enqueue message (hot and cold fallback): %w", err)
			}

			// After successful enqueue, send a "poke" notification.
			// We send a 'nil' envelope to signal this is a "poke", not a push.
			if err := deps.PushNotifier.Notify(ctx, nil, nil); err != nil {
				// Non-critical, just log it. The message is already queued.
				procLogger.Warn().Err(err).Msg("Failed to send online 'poke' notification.")
			}
			return nil
			// --- END REFACTOR ---
		}

		// --- REFACTORED: COLD PATH ---
		// 2. User is offline. Fetch their device tokens for push notifications.
		procLogger.Info().Msg("User is offline. Checking for push notification tokens.")
		tokens, err := deps.DeviceTokenFetcher.Fetch(ctx, recipientURN)
		if err != nil {
			procLogger.Warn().Err(err).Msg("Failed to fetch device tokens. Message will be stored but no push will be sent.")
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
			procLogger.Info().Int("count", len(mobileTokens)).Msg("Routing notification to push notification service.")
			// We send the *full* envelope here for a rich push.
			if err := deps.PushNotifier.Notify(ctx, mobileTokens, envelope); err != nil {
				procLogger.Error().Err(err).Msg("Push notifier failed. Message will be stored, but this error is logged.")
				// Non-critical. We still must store.
			}
		}

		// 5. Finally, store the message in the COLD queue.
		procLogger.Info().Msg("Storing message in COLD queue for later retrieval.")
		if err := deps.MessageQueue.EnqueueCold(ctx, envelope); err != nil {
			// This is a critical error. Return it to trigger a NACK.
			return fmt.Errorf("failed to store message in cold queue: %w", err)
		}
		return nil
		// --- END REFACTOR ---
	}
}

// DELETED: storeMessage helper function
