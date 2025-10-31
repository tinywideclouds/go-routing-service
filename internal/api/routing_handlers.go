/*
File: internal/api/routing_handlers.go
Description: REFACTORED to implement the new API.
- "Dumb Content": SendHandler no longer checks SenderID.
- DELETED: GetDigestHandler and GetHistoryHandler.
- ADDED: GetMessageBatchHandler (pull) and AcknowledgeMessagesHandler (ack).
*/
package api

import (
	"context"
	"encoding/json" // NEW: Using standard json for the simple ACK body
	"io"
	"net/http"
	"strconv" // NEW: For parsing limit query param
	"sync"
	"time"

	"github.com/illmade-knight/go-microservice-base/pkg/response"
	"github.com/illmade-knight/go-routing-service/pkg/routing"
	"github.com/rs/zerolog"

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
	store    routing.MessageStore
	logger   zerolog.Logger
	wg       sync.WaitGroup
}

// NewAPI creates a new, stateless API handler.
func NewAPI(producer routing.IngestionProducer, store routing.MessageStore, logger zerolog.Logger) *API {
	return &API{
		producer: producer,
		store:    store,
		logger:   logger,
	}
}

// Wait will block until all background tasks are complete.
func (a *API) Wait() {
	a.wg.Wait()
}

// SendHandler ingests a message, enforces the sender's identity, and publishes it.
// REFACTORED: This now implements the "Dumb Content" (Sealed Sender) model.
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

	// REFACTORED: Unmarshal into the new "dumb" envelope.
	var envelope secure.SecureEnvelope
	if err := envelope.UnmarshalJSON(body); err != nil {
		a.logger.Warn().Err(err).Str("user", authedUserID).Msg("Failed to unmarshal secure envelope")
		response.WriteJSONError(w, http.StatusBadRequest, "invalid message envelope format")
		return
	}

	// --- "DUMB CONTENT" REFACTOR ---
	// DELETED: The sender spoofing check is GONE.
	//
	// if authedUserID != envelope.SenderID.EntityID() { ... }
	//
	// The new 'secure.SecureEnvelope' has no SenderID.
	// We are now a "dumb" router.
	// ---

	// Publish the message to the ingestion pipeline.
	// REFACTORED: Pass the new envelope type.
	if err := a.producer.Publish(r.Context(), &envelope); err != nil {
		a.logger.Error().Err(err).Str("user", authedUserID).Msg("Failed to publish message to ingestion topic")
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to send message")
		return
	}

	response.WriteJSON(w, http.StatusAccepted, nil)
}

// --- DELETED HANDLERS ---
//
// func (a *API) GetDigestHandler(w http.ResponseWriter, r *http.Request)
// func (a *API) GetHistoryHandler(w http.ResponseWriter, r *http.Request)
//
// --- DELETED ---

// NEW: GetMessageBatchHandler implements the "pull" part of the "Reliable Dumb Queue".
// It retrieves the next available batch of messages for the authenticated user.
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

	// 1. Call the store to get the batch of wrapper messages
	queuedMessages, err := a.store.RetrieveMessageBatch(r.Context(), userURN, limit)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve message batch from store")
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to retrieve messages")
		return
	}

	// 2. Wrap them in the native list struct
	// This list struct will be marshaled to JSON using its custom
	// MarshalJSON method, which uses our 'queue.pb.go' facade.
	messageList := &routingTypes.QueuedMessageList{
		Messages: queuedMessages,
	}

	// 3. Send the JSON response
	log.Info().Int("count", len(queuedMessages)).Msg("Successfully retrieved message batch")
	response.WriteJSON(w, http.StatusOK, messageList)
}

// NEW: AcknowledgeMessagesHandler implements the "ack" part of the "Reliable Dumb Queue".
// It receives a list of internal message IDs and deletes them from the queue.
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
		// Technically not an error, just nothing to do.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	log := a.logger.With().Str("user", userURN.String()).Int("count", len(ackBody.MessageIDs)).Logger()

	// 2. Call the store to delete the messages
	// This is a fire-and-forget from the client's perspective.
	// We'll run the actual delete in a background goroutine to
	// return the response to the client immediately.
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// Create a new background context
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := a.store.AcknowledgeMessages(ctx, userURN, ackBody.MessageIDs); err != nil {
			// Log the error, but the client has already moved on.
			log.Error().Err(err).Msg("Failed to acknowledge/delete messages in background")
		} else {
			log.Info().Msg("Successfully acknowledged messages in background")
		}
	}()

	// 3. Respond 204 No Content immediately.
	w.WriteHeader(http.StatusNoContent)
}
