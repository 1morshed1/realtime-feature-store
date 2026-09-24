package serving

import (
	"context"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
	"github.com/1morshed1/realtime-feature-store/internal/online"
	"github.com/prometheus/client_golang/prometheus"
)

// fixedNow returns a clock stuck at t.
func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

func seedStore(t *testing.T, eventTime time.Time) *online.MemoryStore {
	t.Helper()
	store := online.NewMemoryStore()
	err := store.Set(context.Background(), "user_id", "u1", []domain.FeatureValue{
		{FeatureName: "avg_7d", Value: domain.NewFloat(142.5, eventTime)},
		{FeatureName: "count_1h", Value: domain.NewInt(3, eventTime)},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return store
}

func TestGetFeaturesReturnsValuesAndFreshness(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	eventTime := now.Add(-30 * time.Second) // value is 30s old
	store := seedStore(t, eventTime)

	svc := NewService(store, Options{Now: fixedNow(now)})
	resp, err := svc.GetFeatures(context.Background(), GetRequest{
		Entity: "user_id", EntityIDs: []string{"u1"}, Features: []string{"avg_7d", "count_1h"},
	})
	if err != nil {
		t.Fatalf("GetFeatures: %v", err)
	}
	if len(resp.Vectors) != 1 {
		t.Fatalf("want 1 vector, got %d", len(resp.Vectors))
	}
	got := resp.Vectors[0]
	f, ok := find(got.Features, "avg_7d")
	if !ok || f.Value.Float != 142.5 {
		t.Fatalf("avg_7d = %+v, ok=%v", f, ok)
	}
	if f.AgeSeconds != 30 {
		t.Fatalf("age = %v, want 30", f.AgeSeconds)
	}
}

func TestStaleFlagFromSLA(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store := seedStore(t, now.Add(-10*time.Minute)) // 10 minutes old

	// SLA: avg_7d must be fresher than 5 minutes -> at 10 min it is stale.
	sla := func(feature string) (time.Duration, bool) {
		if feature == "avg_7d" {
			return 5 * time.Minute, true
		}
		return 0, false
	}
	svc := NewService(store, Options{Now: fixedNow(now), SLA: sla})
	resp, _ := svc.GetFeatures(context.Background(), GetRequest{
		Entity: "user_id", EntityIDs: []string{"u1"}, Features: []string{"avg_7d", "count_1h"},
	})
	avg, _ := find(resp.Vectors[0].Features, "avg_7d")
	if !avg.Stale {
		t.Fatalf("avg_7d should be stale at 10min with 5min SLA")
	}
	// count_1h has no SLA registered -> never flagged stale.
	cnt, _ := find(resp.Vectors[0].Features, "count_1h")
	if cnt.Stale {
		t.Fatalf("count_1h has no SLA and must not be flagged stale")
	}
}

func TestCacheHitMissMetrics(t *testing.T) {
	now := time.Now()
	store := seedStore(t, now)
	m := metrics.New()
	svc := NewService(store, Options{Now: fixedNow(now), Metrics: m})

	// Request 3 features; only 2 exist -> 3 requests, 2 hits, 1 miss.
	_, _ = svc.GetFeatures(context.Background(), GetRequest{
		Entity: "user_id", EntityIDs: []string{"u1"},
		Features: []string{"avg_7d", "count_1h", "does_not_exist"},
	})

	if got := counterValue(t, m.Registry(), "feature_requests_total"); got != 3 {
		t.Fatalf("requests = %v, want 3", got)
	}
	if got := counterValue(t, m.Registry(), "feature_cache_hits_total"); got != 2 {
		t.Fatalf("hits = %v, want 2", got)
	}
	if got := counterValue(t, m.Registry(), "feature_cache_misses_total"); got != 1 {
		t.Fatalf("misses = %v, want 1", got)
	}
}

func TestFeatureLogging(t *testing.T) {
	now := time.Now()
	store := seedStore(t, now)
	logger := NewMemoryLogger()
	svc := NewService(store, Options{Now: fixedNow(now), Logger: logger})

	_, _ = svc.GetFeatures(context.Background(), GetRequest{
		Entity: "user_id", EntityIDs: []string{"u1"}, Features: []string{"avg_7d"},
	})
	entries := logger.Entries()
	if len(entries) != 1 || entries[0].EntityID != "u1" {
		t.Fatalf("expected one logged entry for u1, got %+v", entries)
	}
}

func find(fs []ServedFeature, name string) (ServedFeature, bool) {
	for _, f := range fs {
		if f.FeatureName == name {
			return f, true
		}
	}
	return ServedFeature{}, false
}

// counterValue reads a simple (unlabeled) counter's current value from a registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() == name {
			for _, m := range fam.GetMetric() {
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("counter %q not found", name)
	return 0
}
