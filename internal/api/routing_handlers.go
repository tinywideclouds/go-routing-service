/*
File: internal/api/routing_handlers.go
Description: REFACTORED to use the new 'queue.MessageQueue' interface
instead of the old 'routing.MessageStore'.
*/
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-microservice-base/pkg/response"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"

	// REFACTORED: Use new platform packages
	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingTypes "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

const (
	defaultBatchLimit = 50
	maxBatchLimit     = 500
)

// API holds the dependencies for the stateless HTTP handlers.
type API struct {
	producer routing.IngestionProducer
	queue    queue.MessageQueue // REFACTORED: Use new interface
	logger   zerolog.Logger
	wg       sync.WaitGroup
}

// NewAPI creates a new, stateless API handler.
func NewAPI(producer routing.IngestionProducer, queue queue.MessageQueue, logger zerolog.Logger) *API {
	return &API{
		producer: producer,
		queue:    queue, // REFACTORED: Use new interface
		logger:   logger,
	}
}

// Wait will block until all background tasks are complete.
func (a *API) Wait() {
	a.wg.Wait()
}

// SendHandler ingests a message and publishes it.
func (a *API) SendHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		response.WriteJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var envelope secure.SecureEnvelope
	if err := envelope.UnmarshalJSON(body); err != nil {
		a.logger.Warn().Err(err).Str("user", authedUserID).Msg("Failed to unmarshal secure envelope")
		response.WriteJSONError(w, http.StatusBadRequest, "invalid message envelope format")
		return
	}

	if err := a.producer.Publish(r.Context(), &envelope); err != nil {
		a.logger.Error().Err(err).Str("user", authedUserID).Msg("Failed to publish message to ingestion topic")
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to send message")
		return
	}

	response.WriteJSON(w, http.StatusAccepted, nil)
}

// GetMessageBatchHandler implements the "pull" part of the queue.
func (a *API) GetMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}
	userURN, err := urn.New(urn.SecureMessaging, urn.EntityTypeUser, authedUserID)
	if err != nil {
		a.logger.Error().Err(err).Str("user", authedUserID).Msg("Failed to create URN from authed user ID")
		response.WriteJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// Parse 'limit' query parameter
	limit := defaultBatchLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if val, err := strconv.Atoi(limitStr); err == nil {
			if val > maxBatchLimit {
				limit = maxBatchLimit
			} else if val > 0 {
				limit = val
			}
		} else {
			response.WriteJSONError(w, http.StatusBadRequest, "invalid 'limit' parameter, must be an integer")
			return
		}
	}

	log := a.logger.With().Str("user", userURN.String()).Int("limit", limit).Logger()

	// 1. Call the queue to get the batch of wrapper messages
	// REFACTORED: Call queue.RetrieveBatch
	queuedMessages, err := a.queue.RetrieveBatch(r.Context(), userURN, limit)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve message batch from queue")
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to retrieve messages")
		return
	}

	// 2. Wrap them in the native list struct
	messageList := &routingTypes.QueuedMessageList{
		Messages: queuedMessages,
	}

	// 3. Send the JSON response
	log.Info().Int("count", len(queuedMessages)).Msg("Successfully retrieved message batch")
	response.WriteJSON(w, http.StatusOK, messageList)
}

// AcknowledgeMessagesHandler implements the "ack" part of the queue.
func (a *API) AcknowledgeMessagesHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}
	userURN, err := urn.New(urn.SecureMessaging, urn.EntityTypeUser, authedUserID)
	if err != nil {
		a.logger.Error().Err(err).Str("user", authedUserID).Msg("Failed to create URN from authed user ID")
		response.WriteJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// 1. Parse the simple JSON body: {"messageIds": ["id1", "id2"]}
	var ackBody struct {
		MessageIDs []string `json:"messageIds"`
	}

	if err := json.NewDecoder(r.Body).Decode(&ackBody); err != nil {
		response.WriteJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(ackBody.MessageIDs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	log := a.logger.With().Str("user", userURN.String()).Int("count", len(ackBody.MessageIDs)).Logger()

	// 2. Call the queue to delete the messages
	// This runs in a background goroutine.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// REFACTORED: Call queue.Acknowledge
		if err := a.queue.Acknowledge(ctx, userURN, ackBody.MessageIDs); err != nil {
			log.Error().Err(err).Msg("Failed to acknowledge/delete messages in background")
		} else {
			log.Info().Msg("Successfully acknowledged messages in background")
		}
	}()

	// 3. Respond 204 No Content immediately.
	w.WriteHeader(http.StatusNoContent)
}
