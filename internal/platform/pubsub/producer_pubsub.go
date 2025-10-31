/*
File: internal/platform/pubsub/producer_pubsub.go
Description: REFACTORED to use the new 'secure.SecureEnvelope'
facade and types.
*/
// Package pubsub contains concrete adapters for interacting with Google Cloud Pub/Sub.
package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/protobuf/encoding/protojson"

	// REFACTORED: Use new platform 'secure' package
	"github.com/tinywideclouds/go-platform/pkg/secure/v1"
)

// pubsubTopicClient defines the interface for the underlying pubsub.Topic.
// This allows us to use a mock for testing.
type pubsubTopicClient interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
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

// Publish serializes the SecureEnvelope and sends it to the message bus.
// It conforms to the routing.IngestionProducer interface.
// REFACTORED: Signature now accepts *secure.SecureEnvelope
func (p *Producer) Publish(ctx context.Context, envelope *secure.SecureEnvelope) error {
	// REFACTORED: Convert the native Go envelope to its Protobuf representation
	// using the new facade.
	protoEnvelope := secure.ToProto(envelope)

	// REFACTORED: Serialize the Protobuf message using protojson, which matches
	// the unmarshaler used by the EnvelopeTransformer.
	payloadBytes, err := protojson.Marshal(protoEnvelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope for publishing: %w", err)
	}

	// Create the pubsub.Message directly.
	message := &pubsub.Message{
		Data: payloadBytes,
	}

	// Publish the message and wait for the result.
	result := p.topic.Publish(ctx, message)
	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
