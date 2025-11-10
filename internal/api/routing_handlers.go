/*
File: internal/api/routing_handlers.go
Description: REFACTORED to use the new 'queue.MessageQueue' interface
and FIX bug in URN parsing.
*/
package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog" // IMPORTED
	"net/http"
	"strconv"
	"sync"
	"time"

	// "github.com/rs/zerolog" // REMOVED
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
	logger   *slog.Logger       // CHANGED
	wg       sync.WaitGroup
}

// NewAPI creates a new, stateless API handler.
func NewAPI(producer routing.IngestionProducer, queue queue.MessageQueue, logger *slog.Logger) *API { // CHANGED
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
		a.logger.Warn("SendHandler: No user ID in context") // ADDED
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}
	log := a.logger.With("user", authedUserID) // ADDED

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Warn("Failed to read request body", "err", err) // ADDED
		response.WriteJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var envelope secure.SecureEnvelope
	if err := envelope.UnmarshalJSON(body); err != nil {
		log.Warn("Failed to unmarshal secure envelope", "err", err) // CHANGED
		response.WriteJSONError(w, http.StatusBadRequest, "invalid message envelope format")
		return
	}

	log = log.With("recipient", envelope.RecipientID.String())
	log.Debug("Publishing message to ingestion topic")

	if err := a.producer.Publish(r.Context(), &envelope); err != nil {
		log.Error("Failed to publish message to ingestion topic", "err", err) // CHANGED
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to send message")
		return
	}

	log.Debug("Message accepted for ingestion") // ADDED
	response.WriteJSON(w, http.StatusAccepted, nil)
}

// GetMessageBatchHandler implements the "pull" part of the queue.
func (a *API) GetMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		a.logger.Warn("GetMessageBatchHandler: No user ID in context") // ADDED
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}

	// --- START OF FIX ---
	// The authedUserID is a full URN string, so we must PARSE it.
	userURN, err := urn.Parse(authedUserID)
	if err != nil {
		a.logger.Error("Failed to PARSE URN from authed user ID", "err", err, "user", authedUserID)
		response.WriteJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	// --- END OF FIX ---

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
			a.logger.Warn("Invalid 'limit' parameter", "limit", limitStr) // ADDED
			response.WriteJSONError(w, http.StatusBadRequest, "invalid 'limit' parameter, must be an integer")
			return
		}
	}

	log := a.logger.With("user", userURN.String(), "limit", limit) // CHANGED

	log.Debug("Retrieving message batch...") // ADDED
	queuedMessages, err := a.queue.RetrieveBatch(r.Context(), userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve message batch from queue", "err", err) // CHANGED
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to retrieve messages")
		return
	}

	messageList := &routingTypes.QueuedMessageList{
		Messages: queuedMessages,
	}

	log.Info("Successfully retrieved message batch", "count", len(queuedMessages)) // CHANGED
	response.WriteJSON(w, http.StatusOK, messageList)
}

// AcknowledgeMessagesHandler implements the "ack" part of the queue.
func (a *API) AcknowledgeMessagesHandler(w http.ResponseWriter, r *http.Request) {
	authedUserID, ok := middleware.GetUserIDFromContext(r.Context())
	if !ok {
		a.logger.Warn("AcknowledgeMessagesHandler: No user ID in context") // ADDED
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}

	// --- START OF FIX ---
	// The authedUserID is a full URN string, so we must PARSE it.
	userURN, err := urn.Parse(authedUserID)
	if err != nil {
		a.logger.Error("Failed to PARSE URN from authed user ID", "err", err, "user", authedUserID)
		response.WriteJSONError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	// --- END OF FIX ---

	var ackBody struct {
		MessageIDs []string `json:"messageIds"`
	}

	if err := json.NewDecoder(r.Body).Decode(&ackBody); err != nil {
		a.logger.Warn("Failed to decode ack body", "err", err, "user", authedUserID) // ADDED
		response.WriteJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(ackBody.MessageIDs) == 0 {
		a.logger.Debug("Ack request had no message IDs", "user", authedUserID) // ADDED
		w.WriteHeader(http.StatusNoContent)
		return
	}

	log := a.logger.With("user", userURN.String(), "count", len(ackBody.MessageIDs)) // CHANGED

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		log.Info("Acknowledging messages in background...") // ADDED

		if err := a.queue.Acknowledge(ctx, userURN, ackBody.MessageIDs); err != nil {
			log.Error("Failed to acknowledge/delete messages in background", "err", err) // CHANGED
		} else {
			log.Info("Successfully acknowledged messages in background") // CHANGED
		}
	}()

	log.Debug("Ack request received, responding 204") // ADDED
	w.WriteHeader(http.StatusNoContent)
}
