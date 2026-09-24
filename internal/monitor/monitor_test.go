package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/online"
)

func TestNoSkewWhenValuesAgree(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	actual := online.NewMemoryStore()
	ref := online.NewMemoryStore()
	for _, s := range []*online.MemoryStore{actual, ref} {
		_ = s.Set(ctx, "user_id", "u1", []domain.FeatureValue{
			{FeatureName: "avg_7d", Value: domain.NewFloat(142.50, now)},
		})
	}

	c := NewChecker(actual, ref, 0.01, nil)
	reports, err := c.Check(ctx, "user_id", "u1", []string{"avg_7d"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(reports) != 0 {
		t.Fatalf("expected no skew, got %+v", reports)
	}
}

func TestDetectsNumericSkewBeyondTolerance(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	actual := online.NewMemoryStore()
	ref := online.NewMemoryStore()
	// Online says 142.5, batch recomputation says 150.0 -> ~5% drift, over 1% tolerance.
	_ = actual.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "avg_7d", Value: domain.NewFloat(142.5, now)}})
	_ = ref.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "avg_7d", Value: domain.NewFloat(150.0, now)}})

	c := NewChecker(actual, ref, 0.01, nil)
	reports, _ := c.Check(ctx, "user_id", "u1", []string{"avg_7d"})
	if len(reports) != 1 {
		t.Fatalf("expected 1 skew report, got %+v", reports)
	}
	if reports[0].FeatureName != "avg_7d" {
		t.Fatalf("wrong feature reported: %+v", reports[0])
	}
}

func TestToleratesSmallNumericDifference(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	actual := online.NewMemoryStore()
	ref := online.NewMemoryStore()
	// 0.5% difference, under the 1% tolerance -> not skew (expected streaming/batch noise).
	_ = actual.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "avg_7d", Value: domain.NewFloat(100.5, now)}})
	_ = ref.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "avg_7d", Value: domain.NewFloat(100.0, now)}})

	c := NewChecker(actual, ref, 0.01, nil)
	reports, _ := c.Check(ctx, "user_id", "u1", []string{"avg_7d"})
	if len(reports) != 0 {
		t.Fatalf("small difference should be tolerated, got %+v", reports)
	}
}

func TestPresenceMismatchIsSkew(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	actual := online.NewMemoryStore()
	ref := online.NewMemoryStore()
	// Reference has the feature; online does not (e.g. streaming pipeline lag/bug).
	_ = ref.Set(ctx, "user_id", "u1", []domain.FeatureValue{{FeatureName: "avg_7d", Value: domain.NewFloat(100.0, now)}})

	c := NewChecker(actual, ref, 0.01, nil)
	reports, _ := c.Check(ctx, "user_id", "u1", []string{"avg_7d"})
	if len(reports) != 1 {
		t.Fatalf("presence mismatch should be reported as skew, got %+v", reports)
	}
}
