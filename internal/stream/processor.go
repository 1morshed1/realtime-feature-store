package stream

import (
	"context"
	"fmt"
	"log"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// EventSource is the PORT for consuming raw events. Kafka is one adapter; an in-memory
// slice is another. Consume blocks, delivering each event to handle, until the context is
// cancelled or the source is exhausted (in-memory) — Kafka runs until cancelled.
type EventSource interface {
	Consume(ctx context.Context, handle func(context.Context, domain.Event) error) error
}

// Sink is the PORT for writing computed feature values. Both the online store
// (online.Store.Set) and the offline store satisfy this shape, so the processor can fan a
// computed feature out to several destinations without knowing what they are.
type Sink interface {
	Set(ctx context.Context, entity, id string, values []domain.FeatureValue) error
}

// Processor wires a source, an aggregator, and one or more sinks into a running pipeline:
//
//	events --> Aggregator.Update --> feature values --> each Sink
//
// It is the top-level object of the streaming layer.
type Processor struct {
	source EventSource
	agg    *Aggregator
	sinks  []Sink
}

// NewProcessor builds a processor. Typically the sinks are [onlineStore, offlineStore]:
// the online store makes the feature servable now, the offline store records history for
// point-in-time training joins.
func NewProcessor(source EventSource, agg *Aggregator, sinks ...Sink) *Processor {
	return &Processor{source: source, agg: agg, sinks: sinks}
}

// Run consumes events until the context is cancelled or the source ends, updating
// aggregates and writing results to every sink.
func (p *Processor) Run(ctx context.Context) error {
	return p.source.Consume(ctx, p.handle)
}

// handle processes a single event: compute the affected features, then persist them.
func (p *Processor) handle(ctx context.Context, e domain.Event) error {
	values := p.agg.Update(e)
	if len(values) == 0 {
		return nil
	}
	for _, sink := range p.sinks {
		if err := sink.Set(ctx, e.Entity, e.EntityID, values); err != nil {
			// We surface sink failures to the source, which owns the delivery semantics:
			// with Kafka, not committing the offset means the event is retried, giving
			// at-least-once processing. A production system would add bounded retries and
			// dead-lettering; we log for visibility and propagate.
			log.Printf("stream: sink write failed for %s/%s: %v", e.Entity, e.EntityID, err)
			return fmt.Errorf("stream: sink write: %w", err)
		}
	}
	return nil
}
