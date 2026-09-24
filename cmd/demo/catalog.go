package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
	"github.com/1morshed1/realtime-feature-store/internal/stream"
)

// baseTime anchors all demo event timestamps for deterministic, readable output.
var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// catalog bundles the three views of our features that different components need:
// registry definitions, stream aggregation specs, and freshness SLAs. Deriving all three
// from one place keeps them consistent (a real system would generate the specs and SLAs
// straight from the registry).
type catalog struct {
	definitions  []domain.Feature
	aggregations []stream.FeatureSpec
	slas         map[string]time.Duration
}

func (c catalog) slaLookup() func(string) (time.Duration, bool) {
	return func(feature string) (time.Duration, bool) {
		d, ok := c.slas[feature]
		return d, ok
	}
}

// featureCatalog defines the demo's feature set: a 1-hour transaction count, a 7-day
// average amount, and a 24-hour total spend — a realistic little fraud-feature set.
func featureCatalog() catalog {
	slas := map[string]time.Duration{
		"txn_count_1h":  60 * time.Second, // tight SLA -> we'll see it go stale in the demo
		"txn_avg_7d":    10 * time.Minute,
		"txn_total_24h": time.Hour,
	}
	defs := []domain.Feature{
		{Name: "txn_count_1h", Entity: "user_id", DType: domain.DataTypeInt, Source: domain.SourceStreaming,
			Computation: "COUNT(*) OVER 1 HOUR", FreshnessSLA: slas["txn_count_1h"], Owner: "fraud-ml-team"},
		{Name: "txn_avg_7d", Entity: "user_id", DType: domain.DataTypeFloat, Source: domain.SourceStreaming,
			Computation: "AVG(amount) OVER 7 DAYS", FreshnessSLA: slas["txn_avg_7d"], Owner: "fraud-ml-team"},
		{Name: "txn_total_24h", Entity: "user_id", DType: domain.DataTypeFloat, Source: domain.SourceStreaming,
			Computation: "SUM(amount) OVER 24 HOURS", FreshnessSLA: slas["txn_total_24h"], Owner: "fraud-ml-team"},
	}
	aggs := []stream.FeatureSpec{
		{FeatureName: "txn_count_1h", Entity: "user_id", EventType: "transaction", Agg: stream.AggCount, Window: time.Hour, DType: domain.DataTypeInt},
		{FeatureName: "txn_avg_7d", Entity: "user_id", EventType: "transaction", Agg: stream.AggAvg, Window: 7 * 24 * time.Hour, DType: domain.DataTypeFloat},
		{FeatureName: "txn_total_24h", Entity: "user_id", EventType: "transaction", Agg: stream.AggSum, Window: 24 * time.Hour, DType: domain.DataTypeFloat},
	}
	return catalog{definitions: defs, aggregations: aggs, slas: slas}
}

// generateEvents builds a deterministic stream of transaction events. u1's amounts rise
// over time (so its 7-day average changes, making point-in-time correctness visible), with
// a two-transaction burst on the last day (so the 1h count and 24h total are non-trivial).
func generateEvents() []domain.Event {
	txn := func(id string, day int, hour int, min int, amount float64) domain.Event {
		return domain.Event{
			Entity:    "user_id",
			EntityID:  id,
			Type:      "transaction",
			Amount:    amount,
			EventTime: baseTime.AddDate(0, 0, day).Add(time.Duration(hour)*time.Hour + time.Duration(min)*time.Minute),
		}
	}
	var events []domain.Event
	// u1: one transaction per day, amount growing 100,120,...,200.
	for d := 0; d <= 5; d++ {
		events = append(events, txn("u1", d, 10, 0, float64(100+20*d)))
	}
	// u1: a second transaction 30 min after the last, same day -> burst.
	events = append(events, txn("u1", 5, 10, 30, 300))
	// u2: transactions on days 1, 3, 5, all 50.
	events = append(events, txn("u2", 1, 9, 0, 50), txn("u2", 3, 9, 0, 50), txn("u2", 5, 9, 0, 50))

	// Sort by event time to mimic an in-order stream (the processor also handles
	// out-of-order, but ordered input keeps the demo's narration simple).
	sort.Slice(events, func(i, j int) bool { return events[i].EventTime.Before(events[j].EventTime) })
	return events
}

func lastEventTime(events []domain.Event) time.Time {
	last := events[0].EventTime
	for _, e := range events {
		if e.EventTime.After(last) {
			last = e.EventTime
		}
	}
	return last
}

// printMetrics renders the gathered Prometheus metrics in a compact, human-readable form.
func printMetrics(m *metrics.Metrics) {
	families, err := m.Registry().Gather()
	if err != nil {
		fmt.Println("  (failed to gather metrics:", err, ")")
		return
	}
	sort.Slice(families, func(i, j int) bool { return families[i].GetName() < families[j].GetName() })
	for _, fam := range families {
		for _, metric := range fam.GetMetric() {
			label := ""
			for _, l := range metric.GetLabel() {
				label += fmt.Sprintf("%s=%s ", l.GetName(), l.GetValue())
			}
			switch {
			case metric.GetCounter() != nil:
				fmt.Printf("  %-32s %s= %.0f\n", fam.GetName(), label, metric.GetCounter().GetValue())
			case metric.GetGauge() != nil:
				fmt.Printf("  %-32s %s= %.1f\n", fam.GetName(), label, metric.GetGauge().GetValue())
			case metric.GetHistogram() != nil:
				h := metric.GetHistogram()
				fmt.Printf("  %-32s count=%d sum=%.6fs\n", fam.GetName(), h.GetSampleCount(), h.GetSampleSum())
			}
		}
	}
}
