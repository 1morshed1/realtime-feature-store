package stream

import (
	"context"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/online"
)

// TestProcessorEndToEnd drives the whole streaming pipeline with in-memory adapters:
// a slice of events -> aggregator -> online store, then reads the store back and asserts
// the computed features. No Kafka, no Redis.
func TestProcessorEndToEnd(t *testing.T) {
	events := []domain.Event{
		ev("u1", "transaction", 100, 0),
		ev("u1", "transaction", 200, 10),
		ev("u1", "transaction", 300, 20),
		ev("u2", "transaction", 50, 5),
	}
	source := NewMemorySource(events)

	agg := NewAggregator([]FeatureSpec{
		{FeatureName: "txn_count_1h", Entity: "user_id", EventType: "transaction", Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt},
		{FeatureName: "txn_avg_1h", Entity: "user_id", EventType: "transaction", Agg: AggAvg, Window: time.Hour, DType: domain.DataTypeFloat},
	})

	onlineStore := online.NewMemoryStore()
	proc := NewProcessor(source, agg, onlineStore)

	if err := proc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// u1: 3 transactions of 100,200,300 in the hour -> count 3, avg 200.
	fv, _ := onlineStore.Get(context.Background(), "user_id", "u1", nil)
	if v, ok := fv.Get("txn_count_1h"); !ok || v.Int != 3 {
		t.Fatalf("u1 count = %+v (ok=%v), want 3", v, ok)
	}
	if v, ok := fv.Get("txn_avg_1h"); !ok || v.Float != 200 {
		t.Fatalf("u1 avg = %+v (ok=%v), want 200", v, ok)
	}

	// u2: single transaction of 50 -> count 1, avg 50.
	fv2, _ := onlineStore.Get(context.Background(), "user_id", "u2", nil)
	if v, ok := fv2.Get("txn_count_1h"); !ok || v.Int != 1 {
		t.Fatalf("u2 count = %+v, want 1", v)
	}
}

// fanoutSink records every write, letting us verify the processor fans out to multiple
// sinks (online + offline).
type recordingSink struct{ writes int }

func (r *recordingSink) Set(_ context.Context, _, _ string, _ []domain.FeatureValue) error {
	r.writes++
	return nil
}

func TestProcessorFansOutToAllSinks(t *testing.T) {
	source := NewMemorySource([]domain.Event{ev("u1", "x", 1, 0)})
	agg := NewAggregator([]FeatureSpec{{FeatureName: "c", Entity: "user_id", Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt}})
	a, b := &recordingSink{}, &recordingSink{}
	proc := NewProcessor(source, agg, a, b)
	if err := proc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if a.writes != 1 || b.writes != 1 {
		t.Fatalf("fan-out failed: a=%d b=%d, want 1 each", a.writes, b.writes)
	}
}
