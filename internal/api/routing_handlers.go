// --- File: internal/api/routing_handlers.go ---
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

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

// Wait blocks until all background tasks (like async acknowledgements) have finished.
func (a *API) Wait() {
	a.wg.Wait()
}

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

func (a *API) GetMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	userURN, err := a.resolveUserURN(r.Context())
	if err != nil {
		a.logger.Warn("GetMessageBatchHandler: No user identity found")
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}

	// Parse 'limit' query parameter (Restored logic)
	limit := defaultBatchLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if val, err := strconv.Atoi(limitStr); err == nil {
			if val > maxBatchLimit {
				limit = maxBatchLimit
			} else if val > 0 {
				limit = val
			}
		} else {
			a.logger.Warn("Invalid 'limit' parameter", "limit", limitStr)
			response.WriteJSONError(w, http.StatusBadRequest, "invalid 'limit' parameter")
			return
		}
	}

	log := a.logger.With("user", userURN.String(), "limit", limit)

	log.Debug("Retrieving message batch...")
	queuedMessages, err := a.queue.RetrieveBatch(r.Context(), userURN, limit)
	if err != nil {
		log.Error("Failed to retrieve message batch from queue", "err", err)
		response.WriteJSONError(w, http.StatusInternalServerError, "failed to retrieve messages")
		return
	}

	log.Debug("Successfully retrieved message batch", "count", len(queuedMessages))
	response.WriteJSON(w, http.StatusOK, &routingTypes.QueuedMessageList{Messages: queuedMessages})
}

func (a *API) AcknowledgeMessagesHandler(w http.ResponseWriter, r *http.Request) {
	userURN, err := a.resolveUserURN(r.Context())
	if err != nil {
		a.logger.Warn("AcknowledgeMessagesHandler: No user identity found")
		response.WriteJSONError(w, http.StatusUnauthorized, "missing authentication token")
		return
	}

	var ackBody struct {
		MessageIDs []string `json:"messageIds"`
	}

	if err := json.NewDecoder(r.Body).Decode(&ackBody); err != nil {
		a.logger.Warn("Failed to decode ack body", "err", err, "user", userURN.String())
		response.WriteJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(ackBody.MessageIDs) == 0 {
		a.logger.Debug("Ack request had no message IDs", "user", userURN.String())
		w.WriteHeader(http.StatusNoContent)
		return
	}

	log := a.logger.With("user", userURN.String(), "count", len(ackBody.MessageIDs))

	// Acknowledgment is done in the background
	// wg.Add(1) MUST be called synchronously here to avoid race conditions in testing/shutdown
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		// Detached context with timeout ensures the ack completes even if the request is cancelled
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		log.Debug("Acknowledging messages in background...")

		if err := a.queue.Acknowledge(ctx, userURN, ackBody.MessageIDs); err != nil {
			log.Error("Failed to acknowledge messages in background", "err", err)
		} else {
			log.Debug("Successfully acknowledged messages in background")
		}
	}()

	log.Debug("Ack request received, responding 204")
	w.WriteHeader(http.StatusNoContent)
}
