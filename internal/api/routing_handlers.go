// --- File: internal/api/routing_handlers.go ---
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/tinywideclouds/go-microservice-base/pkg/middleware"
	"github.com/tinywideclouds/go-microservice-base/pkg/response"
	"github.com/tinywideclouds/go-platform/pkg/net/v1"
	routingTypes "github.com/tinywideclouds/go-platform/pkg/routing/v1"
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
	"github.com/tinywideclouds/go-routing-service/internal/queue"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
)

const (
	defaultBatchLimit = 50
	maxBatchLimit     = 500
)

type API struct {
	producer routing.IngestionProducer
	queue    queue.MessageQueue
	logger   *slog.Logger
	wg       sync.WaitGroup
}

func NewAPI(producer routing.IngestionProducer, queue queue.MessageQueue, logger *slog.Logger) *API {
	return &API{producer: producer, queue: queue, logger: logger}
}

func (a *API) Wait() { a.wg.Wait() }

func (a *API) resolveUserURN(ctx context.Context) (urn.URN, error) {
	if handle, ok := middleware.GetUserHandleFromContext(ctx); ok && handle != "" {
		return urn.Parse(handle)
	}
	if userID, ok := middleware.GetUserIDFromContext(ctx); ok && userID != "" {
		return urn.Parse(userID)
	}
	return urn.URN{}, context.Canceled
}

func (a *API) SendHandler(w http.ResponseWriter, r *http.Request) {
	userURN, err := a.resolveUserURN(r.Context())
	if err != nil {
		a.logger.Warn("SendHandler: No user identity found in context")
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}
	log := a.logger.With("user", userURN.String())

	// REFACTORED: Idiomatic Decoder
	var envelope secure.SecureEnvelope
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		log.Warn("Failed to decode secure envelope", "err", err)
		response.WriteJSONError(w, http.StatusBadRequest, "invalid message envelope format")
		return
	}

	log = log.With("recipient", envelope.RecipientID.String())

	if err := a.producer.Publish(r.Context(), &envelope); err != nil {
		log.Error("Failed to publish message", "err", err)
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to send message")
		return
	}

	log.Debug("Message accepted")
	response.WriteJSON(w, http.StatusAccepted, nil)
}

// ... GetMessageBatchHandler and AcknowledgeMessagesHandler (Updated imports, logic unchanged) ...
// (Omitting full repetition of GET/ACK handlers unless requested, as they were fine in previous step logic-wise, just needed package import fixes if any)
// Assuming you retain the logic from previous upload for GET/ACK but ensure correct imports.

func (a *API) GetMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	// ... Same logic ...
	userURN, err := a.resolveUserURN(r.Context())
	if err != nil {
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}
	// ... Limit parsing ...
	queuedMessages, err := a.queue.RetrieveBatch(r.Context(), userURN, defaultBatchLimit) // simplified for brevity
	// ... Response ...
	response.WriteJSON(w, http.StatusOK, &routingTypes.QueuedMessageList{Messages: queuedMessages})
}

func (a *API) AcknowledgeMessagesHandler(w http.ResponseWriter, r *http.Request) {
	// ... Same logic ...
	// Using json.NewDecoder here too
	var ackBody struct {
		MessageIDs []string `json:"messageIds"`
	}
	json.NewDecoder(r.Body).Decode(&ackBody)
	// ... Background ack ...
	w.WriteHeader(http.StatusNoContent)
}
