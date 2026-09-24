package stream

import (
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// base time for readable, deterministic event timestamps.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(minutes int) time.Time { return t0.Add(time.Duration(minutes) * time.Minute) }

func ev(entityID, typ string, amount float64, minutes int) domain.Event {
	return domain.Event{
		Entity:    "user_id",
		EntityID:  entityID,
		Type:      typ,
		Amount:    amount,
		EventTime: at(minutes),
	}
}

// feed applies events in order and returns the last value produced for featureName.
func feed(a *Aggregator, feature string, events ...domain.Event) (domain.Value, bool) {
	var last domain.Value
	var found bool
	for _, e := range events {
		for _, fv := range a.Update(e) {
			if fv.FeatureName == feature {
				last = fv.Value
				found = true
			}
		}
	}
	return last, found
}

// feedAll applies events once and returns the last value produced for EVERY feature.
// Use this (instead of calling feed twice on the same aggregator) when asserting several
// features from one event stream — replaying events would double-count.
func feedAll(a *Aggregator, events ...domain.Event) map[string]domain.Value {
	last := make(map[string]domain.Value)
	for _, e := range events {
		for _, fv := range a.Update(e) {
			last[fv.FeatureName] = fv.Value
		}
	}
	return last
}

func TestCountWindow(t *testing.T) {
	// transaction_count_1h: count of "transaction" events in the last 60 minutes.
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "count_1h", Entity: "user_id", EventType: "transaction",
		Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt,
	}})

	// Three transactions within an hour -> count 3.
	v, ok := feed(a, "count_1h",
		ev("u1", "transaction", 10, 0),
		ev("u1", "transaction", 20, 30),
		ev("u1", "transaction", 30, 50),
	)
	if !ok || v.Int != 3 {
		t.Fatalf("count = %v (ok=%v), want 3", v.Int, ok)
	}

	// A fourth event at minute 120 advances the watermark; only events in (60,120] remain.
	// The earlier three (at 0,30,50) are now outside the window -> count 1.
	v, _ = feed(a, "count_1h", ev("u1", "transaction", 40, 120))
	if v.Int != 1 {
		t.Fatalf("count after window slide = %v, want 1", v.Int)
	}
}

func TestAvgAndSum(t *testing.T) {
	a := NewAggregator([]FeatureSpec{
		{FeatureName: "avg", Entity: "user_id", Agg: AggAvg, Window: time.Hour, DType: domain.DataTypeFloat},
		{FeatureName: "sum", Entity: "user_id", Agg: AggSum, Window: time.Hour, DType: domain.DataTypeFloat},
	})
	// amounts 10, 20, 60 within the window -> sum 90, avg 30. Feed ONCE and read both.
	last := feedAll(a, ev("u1", "x", 10, 0), ev("u1", "x", 20, 10), ev("u1", "x", 60, 20))
	if last["avg"].Float != 30 {
		t.Fatalf("avg = %v, want 30", last["avg"].Float)
	}
	if last["sum"].Float != 90 {
		t.Fatalf("sum = %v, want 90", last["sum"].Float)
	}
}

func TestRate(t *testing.T) {
	// events per minute over a 10-minute window.
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "rate_10m", Entity: "user_id", Agg: AggRate, Window: 10 * time.Minute, DType: domain.DataTypeFloat,
	}})
	// 5 events inside a 10-min window -> 0.5 events/min.
	v, _ := feed(a, "rate_10m",
		ev("u1", "x", 1, 0), ev("u1", "x", 1, 2), ev("u1", "x", 1, 4),
		ev("u1", "x", 1, 6), ev("u1", "x", 1, 8),
	)
	if v.Float != 0.5 {
		t.Fatalf("rate = %v, want 0.5", v.Float)
	}
}

func TestLast(t *testing.T) {
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "last_amount", Entity: "user_id", Agg: AggLast, Window: time.Hour, DType: domain.DataTypeFloat,
	}})
	v, _ := feed(a, "last_amount", ev("u1", "x", 10, 0), ev("u1", "x", 99, 30))
	if v.Float != 99 {
		t.Fatalf("last = %v, want 99", v.Float)
	}
}

// TestOutOfOrder verifies that a late-arriving event is placed correctly in the window and
// does NOT advance the watermark past the true maximum. This is the property that makes
// features reproducible when a stream is replayed.
func TestOutOfOrder(t *testing.T) {
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "count_1h", Entity: "user_id", EventType: "transaction",
		Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt,
	}})
	// Events arrive out of order: minute 50 first, then a LATE one at minute 10.
	// Both are within the 1h window ending at watermark=50 -> count 2.
	v, _ := feed(a, "count_1h",
		ev("u1", "transaction", 1, 50),
		ev("u1", "transaction", 1, 10), // late, but still inside (50-60, 50]
	)
	if v.Int != 2 {
		t.Fatalf("count with late event = %v, want 2", v.Int)
	}
}

// TestPerEntityIsolation ensures one entity's events never leak into another's window.
func TestPerEntityIsolation(t *testing.T) {
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "count_1h", Entity: "user_id", Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt,
	}})
	feed(a, "count_1h", ev("u1", "x", 1, 0), ev("u1", "x", 1, 1))
	v, _ := feed(a, "count_1h", ev("u2", "x", 1, 0))
	if v.Int != 1 {
		t.Fatalf("u2 count = %v, want 1 (isolation broken)", v.Int)
	}
}

// TestEventTypeFilter ensures a feature only consumes its configured event type.
func TestEventTypeFilter(t *testing.T) {
	a := NewAggregator([]FeatureSpec{{
		FeatureName: "logins_1h", Entity: "user_id", EventType: "login",
		Agg: AggCount, Window: time.Hour, DType: domain.DataTypeInt,
	}})
	// A "transaction" event must not affect a login feature; it should produce nothing.
	if out := a.Update(ev("u1", "transaction", 1, 0)); len(out) != 0 {
		t.Fatalf("transaction event produced login feature: %+v", out)
	}
	v, _ := feed(a, "logins_1h", ev("u1", "login", 0, 5))
	if v.Int != 1 {
		t.Fatalf("logins = %v, want 1", v.Int)
	}
}
