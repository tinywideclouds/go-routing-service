/*
File: internal/pipeline/routing_processor.go
Description: REFACTORED to use the new 'secure.SecureEnvelope'
type. Also removes logging of 'MessageID' as it's no longer visible.
*/
package pipeline

import (
	"context"
	"fmt"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/illmade-knight/go-routing-service/routingservice/config"
	"github.com/rs/zerolog"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// NewRoutingProcessor creates the main message handler (StreamProcessor) for the routing pipeline.
// It determines user presence and routes messages to the correct delivery channel.
// REFACTORED: Updated generic type
func NewRoutingProcessor(deps *routing.Dependencies, cfg *config.AppConfig, logger zerolog.Logger) messagepipeline.StreamProcessor[secure.SecureEnvelope] {
	// REFACTORED: Updated function signature
	return func(ctx context.Context, msg messagepipeline.Message, envelope *secure.SecureEnvelope) error {
		recipientURN := envelope.RecipientID
		// REFACTORED: Removed 'message_id' from log, as it's no longer available.
		procLogger := logger.With().Str("recipient_id", recipientURN.String()).Logger()

		// 1. Check if the user is online via the presence cache.
		if _, err := deps.PresenceCache.Fetch(ctx, recipientURN); err == nil {
			procLogger.Info().Msg("User is online. Routing message to real-time delivery bus.")
			// REFACTORED: Pass new envelope type
			err := deps.DeliveryProducer.Publish(ctx, cfg.DeliveryTopicID, envelope)
			if err != nil {
				// If this fails, we must NACK so we can retry.
				// This could cause a duplicate (if the user goes offline),
				// but that's better than dropping the message.
				return fmt.Errorf("failed to publish to delivery bus: %w", err)
			}

			// If we dispatch for online delivery, we ALSO store for offline.
			// This handles the "multi-device" use case.
			return storeMessage(ctx, deps.MessageStore, envelope, procLogger)
		}

		// 2. User is offline. Fetch their device tokens for push notifications.
		procLogger.Info().Msg("User is offline. Checking for push notification tokens.")
		tokens, err := deps.DeviceTokenFetcher.Fetch(ctx, recipientURN)
		if err != nil {
			procLogger.Warn().Err(err).Msg("Failed to fetch device tokens. Message will be stored but no push will be sent.")
			// We don't return the error, as this is non-critical.
			// We must still store the message.
		}

		// 3. Separate tokens by platform (web vs. mobile)
		// We only send push notifications for mobile.
		var mobileTokens []routing.DeviceToken
		for _, token := range tokens {
			if token.Platform == "ios" || token.Platform == "android" {
				mobileTokens = append(mobileTokens, token)
			}
		}

		// 4. Handle mobile notifications by publishing to the external push notification service.
		if len(mobileTokens) > 0 {
			procLogger.Info().Int("count", len(mobileTokens)).Msg("Routing notification to push notification service.")
			// REFACTORED: Pass new envelope type
			err := deps.PushNotifier.Notify(ctx, mobileTokens, envelope)
			if err != nil {
				procLogger.Error().Err(err).Msg("Push notifier failed. Message will be stored, but this error is logged.")
				// Again, non-critical. We still must store.
			}
		}

		// 5. Finally, store the message for later retrieval via the API.
		return storeMessage(ctx, deps.MessageStore, envelope, procLogger)
	}
}

// storeMessage is a helper to encapsulate the logic for storing a message offline.
// REFACTORED: Updated signature
func storeMessage(ctx context.Context, store routing.MessageStore, envelope *secure.SecureEnvelope, logger zerolog.Logger) error {
	logger.Info().Msg("Storing message for later retrieval.")
	// REFACTORED: Pass new envelope type
	err := store.StoreMessages(ctx, envelope.RecipientID, []*secure.SecureEnvelope{envelope})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to store message in Firestore. Message will be NACK'd for retry.")
		return err // This is a critical error. Return it to trigger a NACK.
	}
	return nil
}
