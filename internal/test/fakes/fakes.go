// Package fakes provides in-memory test doubles (fakes) and test-specific
// adapters for the service's dependencies. These are used in the cmd/local
// entrypoint and in integration tests.
package fakes

import (
	"context"
	"sync"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-secure-messaging/pkg/transport"
	"github.com/illmade-knight/go-secure-messaging/pkg/urn"
	"github.com/rs/zerolog"
	"github.com/tinywideclouds/go-routing-service/pkg/routing"
)

// --- Consumer ---

type InMemoryConsumer struct {
	outputChan chan messagepipeline.Message
	logger     zerolog.Logger
	stopOnce   sync.Once
	doneChan   chan struct{}
}

func NewInMemoryConsumer(bufferSize int, logger zerolog.Logger) *InMemoryConsumer {
	return &InMemoryConsumer{
		outputChan: make(chan messagepipeline.Message, bufferSize),
		logger:     logger.With().Str("component", "InMemoryConsumer").Logger(),
		doneChan:   make(chan struct{}),
	}
}
func (c *InMemoryConsumer) Publish(msg messagepipeline.Message) {
	select {
	case c.outputChan <- msg:
	case <-c.doneChan:
	}
}
func (c *InMemoryConsumer) Messages() <-chan messagepipeline.Message { return c.outputChan }
func (c *InMemoryConsumer) Start(_ context.Context) error            { return nil }
func (c *InMemoryConsumer) Stop(_ context.Context) error {
	c.stopOnce.Do(func() {
		close(c.doneChan)
		close(c.outputChan)
	})
	return nil
}
func (c *InMemoryConsumer) Done() <-chan struct{} { return c.doneChan }

// --- Producer ---

type Producer struct {
	logger        zerolog.Logger
	publishedChan chan messagepipeline.MessageData
}

func NewProducer(logger zerolog.Logger) *Producer {
	return &Producer{
		logger:        logger,
		publishedChan: make(chan messagepipeline.MessageData, 100),
	}
}
func (p *Producer) Published() <-chan messagepipeline.MessageData { return p.publishedChan }

// Publish to match the DeliveryEventProducer interface.
func (p *Producer) Publish(_ context.Context, data messagepipeline.MessageData) (string, error) {
	p.logger.Info().Str("id", data.ID).Msg("[FAKES-PRODUCER] Publish called.")
	p.publishedChan <- data
	return data.ID, nil
}
func (p *Producer) Stop(_ context.Context) error { close(p.publishedChan); return nil }

// --- Persistence & Notifications ---

type PushNotifier struct{ logger zerolog.Logger }

func NewPushNotifier(logger zerolog.Logger) *PushNotifier { return &PushNotifier{logger: logger} }
func (m *PushNotifier) Notify(_ context.Context, _ []routing.DeviceToken, _ *transport.SecureEnvelope) error {
	return nil
}

type TokenFetcher struct{}

func NewTokenFetcher() *TokenFetcher { return &TokenFetcher{} }
func (m *TokenFetcher) Fetch(_ context.Context, _ urn.URN) ([]routing.DeviceToken, error) {
	return nil, nil
}
func (m *TokenFetcher) Close() error { return nil }

type MessageStore struct{ logger zerolog.Logger }

func NewMessageStore(logger zerolog.Logger) *MessageStore { return &MessageStore{logger: logger} }
func (m *MessageStore) StoreMessages(_ context.Context, _ urn.URN, _ []*transport.SecureEnvelope) error {
	return nil
}
func (m *MessageStore) RetrieveMessages(_ context.Context, _ urn.URN) ([]*transport.SecureEnvelope, error) {
	return nil, nil
}
func (m *MessageStore) DeleteMessages(_ context.Context, _ urn.URN, _ []string) error { return nil }
