// --- File: internal/platform/pubsub/producer_pubsub.go ---
// Package pubsub contains concrete adapters for interacting with Google Cloud Pub/Sub.
package pubsub

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// pubsubTopicClient defines the interface for the underlying pubsub.Topic.
// This allows us to use a mock for testing.
type pubsubTopicClient interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
	String() string // For logging the topic name
}

// Producer implements the routing.IngestionProducer interface.
// It acts as an adapter, serializing a SecureEnvelope and publishing it
// to a Google Cloud Pub/Sub topic.
type Producer struct {
	topic pubsubTopicClient
}

// NewProducer is the constructor for the Pub/Sub producer.
// It takes a topic client that it will publish messages to.
func NewProducer(topic pubsubTopicClient) *Producer {
	return &Producer{
		topic: topic,
	}
}

// Publish serializes the SecureEnvelope into its Protobuf JSON representation
// and sends it to the message bus.
func (p *Producer) Publish(ctx context.Context, envelope *secure.SecureEnvelope) error {
	// Convert the native Go envelope to its Protobuf representation.
	protoEnvelope := secure.ToProto(envelope)

	// Serialize the Protobuf message using protojson.
	payloadBytes, err := protojson.Marshal(protoEnvelope)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to marshal envelope for publishing", "err", err, "recipient", envelope.RecipientID.String())
		return fmt.Errorf("failed to marshal envelope for publishing: %w", err)
	}
	slog.DebugContext(ctx, "Envelope marshaled for publishing", "size_bytes", len(payloadBytes), "recipient", envelope.RecipientID.String())

	// Create the pubsub.Message directly.
	message := &pubsub.Message{
		Data: payloadBytes,
	}

	slog.DebugContext(ctx, "Publishing message to Pub/Sub", "topic", p.topic.String(), "recipient", envelope.RecipientID.String())

	// Publish the message and wait for the result.
	result := p.topic.Publish(ctx, message)
	msgID, err := result.Get(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to publish message", "err", err, "topic", p.topic.String(), "recipient", envelope.RecipientID.String())
		return fmt.Errorf("failed to publish message: %w", err)
	}

	slog.DebugContext(ctx, "Message published successfully", "topic", p.topic.String(), "msg_id", msgID)

	return nil
}
