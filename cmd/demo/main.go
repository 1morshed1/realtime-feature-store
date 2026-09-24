// Command demo runs the ENTIRE feature store in a single process using only in-memory
// adapters — no Kafka, Redis, or Postgres required. It is both a smoke test of the whole
// data flow and a guided tour: run it and read the narrated output alongside
// docs/learning/*.
//
//	go run ./cmd/demo        (or: make demo)
//
// The flow it demonstrates, end to end:
//
//	register features → generate events → stream-process into online+offline stores →
//	serve features (with freshness) → build a leak-free training set (point-in-time join) →
//	detect train-serve skew → print metrics.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
	"github.com/1morshed1/realtime-feature-store/internal/monitor"
	"github.com/1morshed1/realtime-feature-store/internal/offline"
	"github.com/1morshed1/realtime-feature-store/internal/online"
	"github.com/1morshed1/realtime-feature-store/internal/registry"
	"github.com/1morshed1/realtime-feature-store/internal/serving"
	"github.com/1morshed1/realtime-feature-store/internal/stream"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	section("1. FEATURE REGISTRY — define what features exist")
	reg := registry.NewService(registry.NewMemoryStore(), nil)
	specs := featureCatalog()
	for _, def := range specs.definitions {
		if _, err := reg.Register(ctx, def); err != nil {
			return err
		}
		fmt.Printf("  registered %-22s owner=%s sla=%s\n", def.Name, def.Owner, def.FreshnessSLA)
	}

	section("2. EVENTS — a stream of raw facts (transactions) over several days")
	events := generateEvents()
	fmt.Printf("  generated %d transaction events for users u1, u2 across ~6 days\n", len(events))

	section("3. STREAM PROCESSING — compute features, write to online + offline stores")
	onlineStore := online.NewMemoryStore()
	offlineStore := offline.NewMemoryOfflineStore()
	agg := stream.NewAggregator(specs.aggregations)
	proc := stream.NewProcessor(stream.NewMemorySource(events), agg, onlineStore, offlineStore)
	if err := proc.Run(ctx); err != nil {
		return err
	}
	fmt.Println("  processed all events -> online store (latest) + offline store (history)")

	section("4. SERVING — fetch u1's current features, with freshness")
	m := metrics.New()
	svc := serving.NewService(onlineStore, serving.Options{
		SLA:     specs.slaLookup(),
		Metrics: m,
		Logger:  serving.NewMemoryLogger(),
		// Pin "now" just after the last event so ages are readable and deterministic.
		Now: func() time.Time { return lastEventTime(events).Add(2 * time.Minute) },
	})
	resp, err := svc.GetFeatures(ctx, serving.GetRequest{
		Entity: "user_id", EntityIDs: []string{"u1"},
		Features: []string{"txn_count_1h", "txn_avg_7d", "txn_total_24h", "missing_feature"},
	})
	if err != nil {
		return err
	}
	for _, f := range resp.Vectors[0].Features {
		fmt.Printf("  %-16s = %-8s (age %4.0fs, stale=%v)\n", f.FeatureName, f.Value.String(), f.AgeSeconds, f.Stale)
	}
	fmt.Println("  note: 'missing_feature' was requested but absent -> a cache miss (see metrics)")

	section("5. POINT-IN-TIME CORRECTNESS — why training must NOT use current values")
	// Current (online) avg for u1 reflects ALL of u1's transactions.
	nowAvg, _ := onlineStore.Get(ctx, "user_id", "u1", []string{"txn_avg_7d"})
	cur, _ := nowAvg.Get("txn_avg_7d")
	// The avg AS OF an early moment (day 2) reflects only the transactions up to then.
	early := baseTime.AddDate(0, 0, 2)
	asOf, _ := offlineStore.GetAsOf(ctx, "user_id", "u1", early, []string{"txn_avg_7d"})
	past, _ := asOf.Get("txn_avg_7d")
	fmt.Printf("  u1 txn_avg_7d NOW (online)        = %s\n", cur.String())
	fmt.Printf("  u1 txn_avg_7d AS OF %s = %s  <-- what a label on that day must see\n",
		early.Format("2006-01-02"), past.String())
	fmt.Println("  Training on the NOW value would leak the future into the past (data leakage).")

	// Build an actual training dataset with a point-in-time join.
	requests := []offline.JoinRequest{
		{EntityID: "u1", Timestamp: baseTime.AddDate(0, 0, 2), Label: 0},
		{EntityID: "u1", Timestamp: baseTime.AddDate(0, 0, 4), Label: 1},
		{EntityID: "u2", Timestamp: baseTime.AddDate(0, 0, 3), Label: 0},
	}
	rows, err := offlineStore.PointInTimeJoin(ctx, "user_id", requests, []string{"txn_avg_7d", "txn_count_1h"})
	if err != nil {
		return err
	}
	fmt.Println("  training dataset (leak-free, each row as-of its own timestamp):")
	for _, r := range rows {
		avg, _ := r.Features.Get("txn_avg_7d")
		fmt.Printf("    %s @ %s  label=%.0f  txn_avg_7d=%s\n",
			r.EntityID, r.Timestamp.Format("2006-01-02"), r.Label, avg.String())
	}

	section("6. CONSISTENCY MONITOR — detect train-serve skew")
	// Reference = offline recomputation as of 'now'. It should match the online store.
	refAsNow := asOfReference{store: offlineStore, asOf: lastEventTime(events).Add(2 * time.Minute)}
	checker := monitor.NewChecker(onlineStore, refAsNow, 0.01, m)
	names := []string{"txn_count_1h", "txn_avg_7d", "txn_total_24h"}
	reports, err := checker.Check(ctx, "user_id", "u1", names)
	if err != nil {
		return err
	}
	fmt.Printf("  healthy check (online vs offline-as-of-now): %d skew report(s) -> pipelines agree\n", len(reports))

	// Now INJECT skew: corrupt an online value, as a buggy streaming job would.
	_ = onlineStore.Set(ctx, "user_id", "u1", []domain.FeatureValue{
		{FeatureName: "txn_avg_7d", Value: domain.NewFloat(cur.Float*1.5, time.Now())},
	})
	reports, _ = checker.Check(ctx, "user_id", "u1", names)
	fmt.Printf("  after injecting a bad online value: %d skew report(s) -> DETECTED:\n", len(reports))
	for _, r := range reports {
		fmt.Printf("    feature=%s online=%s reference=%s (%s)\n", r.FeatureName, r.Online, r.Reference, r.Reason)
	}

	section("7. METRICS — the operational vital signs (Prometheus format)")
	printMetrics(m)

	fmt.Println("\n✔ demo complete — the full feature-store data flow ran end to end in memory.")
	return nil
}

// asOfReference adapts the offline store into a monitor.Reference by reading values as of a
// fixed time — i.e. "what the batch recomputation currently believes".
type asOfReference struct {
	store *offline.MemoryOfflineStore
	asOf  time.Time
}

func (r asOfReference) Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error) {
	return r.store.GetAsOf(ctx, entity, id, r.asOf, names)
}

func section(title string) {
	fmt.Printf("\n\033[1m=== %s ===\033[0m\n", title)
}
