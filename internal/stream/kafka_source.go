package stream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// KafkaSource is the production EventSource: it consumes JSON-encoded domain.Event values
// from a Kafka topic (the "raw-events" topic in the architecture diagram).
//
// LEARNING NOTE — consumer groups and at-least-once processing
// ------------------------------------------------------------
// We read via a consumer GROUP (GroupID). Kafka distributes the topic's partitions across
// the members of a group, so you scale throughput by running more processor instances —
// each handles a subset of partitions, and Kafka rebalances automatically when instances
// come and go.
//
// We use MANUAL offset commits: we commit an event's offset only AFTER the feature values
// derived from it have been written to the sinks. If the process crashes between handling
// and committing, the event is re-delivered on restart. That gives *at-least-once*
// processing — an event might be processed twice, never zero times. Because our
// aggregations are recomputed from retained samples (not blind increments), a duplicate
// delivery of the same event is idempotent as long as producers don't emit true duplicates
// with distinct identities. Choosing at-least-once (vs at-most-once) is the safe default
// for feature pipelines: a briefly double-counted event is far less harmful than a
// silently dropped one.
type KafkaSource struct {
	reader *kafka.Reader
}

// KafkaConfig configures the consumer.
type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewKafkaSource builds a Kafka-backed event source.
func NewKafkaSource(cfg KafkaConfig) *KafkaSource {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		// MinBytes/MaxBytes tune the batch size the broker returns per fetch.
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &KafkaSource{reader: r}
}

// Consume loops fetching messages, decoding each into a domain.Event, invoking handle, and
// committing the offset only on success (manual commit = at-least-once).
func (s *KafkaSource) Consume(ctx context.Context, handle func(context.Context, domain.Event) error) error {
	for {
		// FetchMessage does NOT auto-commit; we commit explicitly below.
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			// Context cancellation is the normal shutdown path.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafka fetch: %w", err)
		}

		var e domain.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			// A poison message we cannot decode: skip it (commit) rather than blocking the
			// partition forever. A real system routes these to a dead-letter topic.
			if cerr := s.reader.CommitMessages(ctx, msg); cerr != nil {
				return fmt.Errorf("kafka commit (bad msg): %w", cerr)
			}
			continue
		}

		if err := handle(ctx, e); err != nil {
			// Do NOT commit — the event will be redelivered. Return so the caller decides.
			return err
		}

		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("kafka commit: %w", err)
		}
	}
}

// Close releases the underlying reader.
func (s *KafkaSource) Close() error { return s.reader.Close() }

// Publisher is a small helper to PRODUCE events onto the raw-events topic. It is used by
// the event generator (cmd/streamproc can also run in "generate" mode) and by integration
// tests, so it lives next to the consumer it feeds.
type Publisher struct {
	writer *kafka.Writer
}

// NewPublisher creates a producer for the given topic.
func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{}, // hash on key => same entity always to same partition
		},
	}
}

// Publish writes one event. We key the message by EntityID so that all events for an
// entity land on the same partition and are therefore processed in order by one consumer —
// essential for correct per-entity windowing.
func (p *Publisher) Publish(ctx context.Context, e domain.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.EntityID),
		Value: body,
	})
}

// Close flushes and closes the producer.
func (p *Publisher) Close() error { return p.writer.Close() }
