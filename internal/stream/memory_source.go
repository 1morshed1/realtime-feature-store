package stream

import (
	"context"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// MemorySource is an EventSource backed by a fixed slice of events. It is used by the demo
// and tests: it delivers each event once, in order, then returns nil (exhausted). This
// lets us drive the whole pipeline deterministically with no Kafka.
type MemorySource struct {
	events []domain.Event
}

// NewMemorySource creates a source over the given events.
func NewMemorySource(events []domain.Event) *MemorySource {
	return &MemorySource{events: events}
}

// Consume delivers each event to handle, stopping early if the context is cancelled or a
// handler returns an error.
func (s *MemorySource) Consume(ctx context.Context, handle func(context.Context, domain.Event) error) error {
	for _, e := range s.events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := handle(ctx, e); err != nil {
			return err
		}
	}
	return nil
}
