// Package stream is the real-time feature computation layer. It consumes raw events from
// a source (Kafka) and turns them into features via event-time windowed aggregations,
// then writes the results to the online and offline stores.
//
// This file contains the aggregation engine — the mathematical core. It is deliberately
// free of Kafka/Redis so the windowing logic can be tested with plain Go values.
package stream

import (
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// AggType is the kind of windowed aggregation to compute.
type AggType string

const (
	AggAvg   AggType = "avg"   // mean of event amounts in the window
	AggSum   AggType = "sum"   // sum of event amounts in the window
	AggCount AggType = "count" // number of events in the window
	AggRate  AggType = "rate"  // events per minute over the window
	AggLast  AggType = "last"  // most recent event amount (by event time)
)

// FeatureSpec declares one streaming feature: which events feed it, what to compute, and
// over what window. The stream processor is configured with a set of these — in a fuller
// system they would be derived from the registry's feature definitions.
type FeatureSpec struct {
	FeatureName string          // e.g. "transaction_count_1h"
	Entity      string          // e.g. "user_id"
	EventType   string          // only events of this type feed the feature; "" = all types
	Agg         AggType         // the aggregation to apply
	Window      time.Duration   // event-time window size (ignored by AggLast)
	DType       domain.DataType // the output value's type
}

// sample is one retained data point: the amount and the event time it occurred.
type sample struct {
	t time.Time
	v float64
}

// window holds the retained samples for one (feature, entity) pair.
//
// LEARNING NOTE — why we keep raw samples instead of just a running sum
// ---------------------------------------------------------------------
// A running sum is O(1) but cannot handle two realities of streams:
//   - out-of-order events (a late event must be inserted into the past), and
//   - eviction (old samples leaving the window must be subtracted).
//
// Keeping the samples lets us recompute correctly in both cases. The cost is O(n) per
// event where n = samples in the window. For large windows/high throughput you'd switch
// to a bucketed/ring-buffer structure or a probabilistic sketch; we favor correctness and
// clarity here and call out the tradeoff explicitly.
type window struct {
	samples   []sample
	watermark time.Time // max event time seen so far — defines the window's right edge
}

// Aggregator maintains windowed state for every configured feature across all entities
// and produces feature values as events arrive.
//
// State layout: featureName -> entityID -> *window. In production this state is huge and
// lives in a fault-tolerant store (RocksDB via Kafka Streams/Flink). Here it is an
// in-memory map; the aggregation math is identical, only the state backend differs.
type Aggregator struct {
	specs    []FeatureSpec
	state    map[string]map[string]*window // featureName -> entityID -> window
	byEntity map[string][]FeatureSpec      // entity -> specs, for quick event dispatch
}

// NewAggregator builds an aggregator for the given feature specs.
func NewAggregator(specs []FeatureSpec) *Aggregator {
	a := &Aggregator{
		specs:    specs,
		state:    make(map[string]map[string]*window),
		byEntity: make(map[string][]FeatureSpec),
	}
	for _, s := range specs {
		a.state[s.FeatureName] = make(map[string]*window)
		a.byEntity[s.Entity] = append(a.byEntity[s.Entity], s)
	}
	return a
}

// Update applies one event and returns the feature values that changed as a result. The
// returned values all belong to event.Entity / event.EntityID and are ready to be written
// to the stores.
func (a *Aggregator) Update(event domain.Event) []domain.FeatureValue {
	specs := a.byEntity[event.Entity]
	if len(specs) == 0 {
		return nil
	}
	var out []domain.FeatureValue
	for _, spec := range specs {
		// EventType "" means the feature is fed by every event for this entity.
		if spec.EventType != "" && spec.EventType != event.Type {
			continue
		}
		w := a.windowFor(spec.FeatureName, event.EntityID)
		w.add(event.EventTime, event.Amount)
		if spec.Agg != AggLast {
			w.evict(spec.Window)
		}
		out = append(out, domain.FeatureValue{
			FeatureName: spec.FeatureName,
			Value:       compute(spec, w),
		})
	}
	return out
}

func (a *Aggregator) windowFor(feature, entityID string) *window {
	m := a.state[feature]
	w, ok := m[entityID]
	if !ok {
		w = &window{}
		m[entityID] = w
	}
	return w
}

// add inserts a sample, keeping samples ordered by event time so out-of-order arrivals
// land in the right place, and advances the watermark.
func (w *window) add(t time.Time, v float64) {
	if t.After(w.watermark) {
		w.watermark = t
	}
	// Fast path: in-order append (the common case).
	if len(w.samples) == 0 || !t.Before(w.samples[len(w.samples)-1].t) {
		w.samples = append(w.samples, sample{t, v})
		return
	}
	// Slow path: out-of-order — find the insertion point to keep the slice sorted.
	i := len(w.samples)
	for i > 0 && w.samples[i-1].t.After(t) {
		i--
	}
	w.samples = append(w.samples, sample{})
	copy(w.samples[i+1:], w.samples[i:])
	w.samples[i] = sample{t, v}
}

// evict drops samples that fall outside the window (t <= watermark - window). The window
// is the half-open interval (watermark-size, watermark].
func (w *window) evict(size time.Duration) {
	cutoff := w.watermark.Add(-size)
	// Samples are sorted, so find the first index strictly after the cutoff.
	i := 0
	for i < len(w.samples) && !w.samples[i].t.After(cutoff) {
		i++
	}
	if i > 0 {
		w.samples = append(w.samples[:0], w.samples[i:]...)
	}
}

// compute turns the current window state into a typed feature value. The value's
// EventTime is the watermark: the logical time this feature is current as of.
func compute(spec FeatureSpec, w *window) domain.Value {
	t := w.watermark
	switch spec.Agg {
	case AggCount:
		return domain.NewInt(int64(len(w.samples)), t)
	case AggSum:
		return domain.NewFloat(sum(w.samples), t)
	case AggAvg:
		if len(w.samples) == 0 {
			return domain.NewFloat(0, t)
		}
		return domain.NewFloat(sum(w.samples)/float64(len(w.samples)), t)
	case AggRate:
		minutes := spec.Window.Minutes()
		if minutes <= 0 {
			return domain.NewFloat(0, t)
		}
		return domain.NewFloat(float64(len(w.samples))/minutes, t)
	case AggLast:
		if len(w.samples) == 0 {
			return domain.NewFloat(0, t)
		}
		return domain.NewFloat(w.samples[len(w.samples)-1].v, t)
	default:
		return domain.NewFloat(0, t)
	}
}

func sum(samples []sample) float64 {
	var s float64
	for _, x := range samples {
		s += x.v
	}
	return s
}
