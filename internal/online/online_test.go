package online

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// StoreContract validates any online Store implementation. MemoryStore runs it here;
// the Redis integration test (build tag "integration") runs the identical function.
func StoreContract(t *testing.T, newStore func() Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("set then get subset", func(t *testing.T) {
		s := newStore()
		err := s.Set(ctx, "user_id", "123", []domain.FeatureValue{
			{FeatureName: "avg_7d", Value: domain.NewFloat(142.5, now)},
			{FeatureName: "count_1h", Value: domain.NewInt(3, now)},
		})
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		fv, err := s.Get(ctx, "user_id", "123", []string{"avg_7d"})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(fv.Features) != 1 {
			t.Fatalf("want 1 feature, got %d", len(fv.Features))
		}
		v, ok := fv.Get("avg_7d")
		if !ok || v.Float != 142.5 {
			t.Fatalf("avg_7d = %+v, ok=%v", v, ok)
		}
		// The event_time must survive the round trip — this is what freshness relies on.
		if !v.EventTime.Equal(now) {
			t.Fatalf("event_time lost: %v", v.EventTime)
		}
	})

	t.Run("get all", func(t *testing.T) {
		s := newStore()
		_ = s.Set(ctx, "user_id", "1", []domain.FeatureValue{
			{FeatureName: "a", Value: domain.NewInt(1, now)},
			{FeatureName: "b", Value: domain.NewInt(2, now)},
		})
		fv, _ := s.Get(ctx, "user_id", "1", nil)
		if len(fv.Features) != 2 {
			t.Fatalf("want 2 features, got %d", len(fv.Features))
		}
	})

	t.Run("partial update preserves other features", func(t *testing.T) {
		s := newStore()
		_ = s.Set(ctx, "user_id", "9", []domain.FeatureValue{{FeatureName: "a", Value: domain.NewInt(1, now)}})
		_ = s.Set(ctx, "user_id", "9", []domain.FeatureValue{{FeatureName: "b", Value: domain.NewInt(2, now)}})
		fv, _ := s.Get(ctx, "user_id", "9", nil)
		if len(fv.Features) != 2 {
			t.Fatalf("partial update clobbered features: %+v", fv.Features)
		}
	})

	t.Run("missing entity yields empty vector, not error", func(t *testing.T) {
		s := newStore()
		fv, err := s.Get(ctx, "user_id", "does-not-exist", []string{"a"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(fv.Features) != 0 {
			t.Fatalf("expected empty vector, got %+v", fv.Features)
		}
	})

	t.Run("get batch returns one vector per id in order", func(t *testing.T) {
		s := newStore()
		_ = s.Set(ctx, "user_id", "1", []domain.FeatureValue{{FeatureName: "a", Value: domain.NewInt(10, now)}})
		_ = s.Set(ctx, "user_id", "2", []domain.FeatureValue{{FeatureName: "a", Value: domain.NewInt(20, now)}})
		vecs, err := s.GetBatch(ctx, "user_id", []string{"1", "2", "3"}, []string{"a"})
		if err != nil {
			t.Fatalf("GetBatch: %v", err)
		}
		if len(vecs) != 3 {
			t.Fatalf("want 3 vectors, got %d", len(vecs))
		}
		if vecs[0].EntityID != "1" || vecs[1].EntityID != "2" || vecs[2].EntityID != "3" {
			t.Fatalf("batch order not preserved: %+v", vecs)
		}
		// id "3" was never written -> empty vector.
		if len(vecs[2].Features) != 0 {
			t.Fatalf("expected empty vector for unwritten id, got %+v", vecs[2].Features)
		}
	})
}

func TestMemoryStore(t *testing.T) {
	StoreContract(t, func() Store { return NewMemoryStore() })
}

// TestMemoryConcurrent stresses the mutex with concurrent writers and readers; run with
// `go test -race` to catch data races in the store.
func TestMemoryConcurrent(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Now()
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			for i := 0; i < 100; i++ {
				_ = s.Set(ctx, "user_id", "hot", []domain.FeatureValue{
					{FeatureName: "a", Value: domain.NewInt(int64(i), now)},
				})
				_, _ = s.Get(ctx, "user_id", "hot", []string{"a"})
			}
			done <- struct{}{}
		}(w)
	}
	for w := 0; w < 8; w++ {
		<-done
	}
	// Sanity: the feature exists after the storm.
	fv, _ := s.Get(ctx, "user_id", "hot", nil)
	names := make([]string, 0)
	for _, f := range fv.Features {
		names = append(names, f.FeatureName)
	}
	sort.Strings(names)
	if len(names) != 1 || names[0] != "a" {
		t.Fatalf("unexpected state after concurrency: %v", names)
	}
}
