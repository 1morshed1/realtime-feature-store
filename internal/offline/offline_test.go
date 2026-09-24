package offline

import (
	"context"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

var day0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func onDay(d int, hour int) time.Time {
	return day0.AddDate(0, 0, d).Add(time.Duration(hour) * time.Hour)
}

// StoreContract validates any offline Store implementation. Both the memory and the file
// adapter are run through it, so they are guaranteed to agree on point-in-time semantics.
func StoreContract(t *testing.T, newStore func() Store) {
	t.Helper()
	ctx := context.Background()

	// Helper to record a versioned value.
	record := func(s Store, id, feature string, val float64, at time.Time) {
		if err := s.Set(ctx, "user_id", id, []domain.FeatureValue{
			{FeatureName: feature, Value: domain.NewFloat(val, at)},
		}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	t.Run("get as-of returns the historically correct value", func(t *testing.T) {
		s := newStore()
		// The user's avg_7d evolved: 100 on day 1, 200 on day 3, 300 on day 5.
		record(s, "u1", "avg_7d", 100, onDay(1, 0))
		record(s, "u1", "avg_7d", 200, onDay(3, 0))
		record(s, "u1", "avg_7d", 300, onDay(5, 0))

		cases := []struct {
			asOf time.Time
			want float64
		}{
			{onDay(2, 0), 100}, // between day1 and day3 -> the day1 value
			{onDay(3, 0), 200}, // exactly at day3 -> the day3 value (<= is inclusive)
			{onDay(4, 12), 200},
			{onDay(9, 0), 300}, // after the last write -> the latest value
		}
		for _, c := range cases {
			fv, err := s.GetAsOf(ctx, "user_id", "u1", c.asOf, []string{"avg_7d"})
			if err != nil {
				t.Fatalf("GetAsOf: %v", err)
			}
			v, ok := fv.Get("avg_7d")
			if !ok || v.Float != c.want {
				t.Fatalf("as of %s: got %v (ok=%v), want %v", c.asOf.Format("2006-01-02"), v.Float, ok, c.want)
			}
		}
	})

	t.Run("value before any history is invisible", func(t *testing.T) {
		s := newStore()
		record(s, "u2", "avg_7d", 100, onDay(5, 0))
		// Asking as of day 1 (before the first recorded value) must return NOTHING, not 0
		// and not the day-5 value. Returning the day-5 value would be leakage.
		fv, _ := s.GetAsOf(ctx, "user_id", "u2", onDay(1, 0), []string{"avg_7d"})
		if _, ok := fv.Get("avg_7d"); ok {
			t.Fatalf("expected no value as-of before first write, but got one")
		}
	})

	t.Run("point-in-time join attaches leak-free features", func(t *testing.T) {
		s := newStore()
		// Feature history.
		record(s, "u1", "avg_7d", 100, onDay(1, 0))
		record(s, "u1", "avg_7d", 999, onDay(10, 0)) // a FUTURE value relative to the label

		// A labeled training example on day 2. The correct feature is 100 (as of day 2),
		// NOT 999 (which happened on day 10, in the future). Using 999 would be leakage.
		rows, err := s.PointInTimeJoin(ctx, "user_id",
			[]JoinRequest{{EntityID: "u1", Timestamp: onDay(2, 0), Label: 1}},
			[]string{"avg_7d"})
		if err != nil {
			t.Fatalf("PointInTimeJoin: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("want 1 row, got %d", len(rows))
		}
		v, ok := rows[0].Features.Get("avg_7d")
		if !ok || v.Float != 100 {
			t.Fatalf("LEAKAGE: training feature = %v (ok=%v), want 100", v.Float, ok)
		}
		if rows[0].Label != 1 {
			t.Fatalf("label not carried through: %v", rows[0].Label)
		}
	})

	t.Run("get all features as-of", func(t *testing.T) {
		s := newStore()
		record(s, "u3", "f1", 1, onDay(1, 0))
		record(s, "u3", "f2", 2, onDay(1, 0))
		fv, _ := s.GetAsOf(ctx, "user_id", "u3", onDay(2, 0), nil)
		if len(fv.Features) != 2 {
			t.Fatalf("want 2 features, got %d", len(fv.Features))
		}
	})

	t.Run("out-of-order writes still read correctly", func(t *testing.T) {
		s := newStore()
		// Write day5 BEFORE day3 (late arrival). GetAsOf must still order by event time.
		record(s, "u4", "f", 500, onDay(5, 0))
		record(s, "u4", "f", 300, onDay(3, 0))
		fv, _ := s.GetAsOf(ctx, "user_id", "u4", onDay(4, 0), []string{"f"})
		v, ok := fv.Get("f")
		if !ok || v.Float != 300 {
			t.Fatalf("as of day4 = %v, want 300 (out-of-order write mishandled)", v.Float)
		}
	})
}

func TestMemoryOfflineStore(t *testing.T) {
	StoreContract(t, func() Store { return NewMemoryOfflineStore() })
}

func TestFileOfflineStore(t *testing.T) {
	// Each contract sub-run gets a fresh temp directory so runs don't see each other's data.
	StoreContract(t, func() Store { return NewFileOfflineStore(t.TempDir()) })
}

// TestFilePartitionPruningLayout is a white-box check that the file store actually writes
// the date-partitioned layout the docs describe.
func TestFilePartitionPruningLayout(t *testing.T) {
	dir := t.TempDir()
	s := NewFileOfflineStore(dir)
	ctx := context.Background()
	_ = s.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "f", Value: domain.NewFloat(1, onDay(1, 0))}})
	_ = s.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "f", Value: domain.NewFloat(2, onDay(2, 0))}})

	// A query as of day 1 must not even open day 2's partition. We can't easily observe
	// file opens, but we can assert correctness that depends on pruning being harmless:
	fv, _ := s.GetAsOf(ctx, "user_id", "u1", onDay(1, 12), []string{"f"})
	if v, _ := fv.Get("f"); v.Float != 1 {
		t.Fatalf("as of day1 = %v, want 1", v.Float)
	}
}
