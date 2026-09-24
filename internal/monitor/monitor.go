// Package monitor detects TRAIN-SERVE SKEW: the situation where the feature values served
// online drift away from what a batch recomputation says they should be. Skew is the
// silent killer of ML systems — no error is thrown, the model just gets quietly worse — so
// the only defense is to actively look for it, which is what this package does.
package monitor

import (
	"context"
	"fmt"
	"math"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
)

// Actual is the source of the values actually being SERVED (the online store). Only the
// read shape is needed.
type Actual interface {
	Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error)
}

// Reference is the source of TRUTH to compare against — typically a batch recomputation of
// the same features (e.g. the offline store's as-of-now values, or a fresh recompute from
// raw data). If online and reference disagree, that's skew.
type Reference interface {
	Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error)
}

// SkewReport describes one disagreement between online and reference for one feature.
type SkewReport struct {
	EntityID    string  `json:"entity_id"`
	FeatureName string  `json:"feature_name"`
	Online      string  `json:"online"`    // value served online (string-rendered)
	Reference   string  `json:"reference"` // value the batch recomputation expects
	RelDiff     float64 `json:"rel_diff"`  // relative difference for numeric features
	Reason      string  `json:"reason"`    // human-readable explanation
}

// Checker compares online values to reference values and reports skew.
type Checker struct {
	actual    Actual
	reference Reference
	// tolerance is the allowed RELATIVE difference for numeric features before we call it
	// skew, e.g. 0.01 = 1%. Small differences are expected (streaming vs batch rounding);
	// large ones are bugs.
	tolerance float64
	metrics   *metrics.Metrics
}

// NewChecker builds a skew checker. Pass nil metrics to skip instrumentation.
func NewChecker(actual Actual, reference Reference, tolerance float64, m *metrics.Metrics) *Checker {
	if m == nil {
		m = metrics.New()
	}
	return &Checker{actual: actual, reference: reference, tolerance: tolerance, metrics: m}
}

// Check compares the given features for one entity and returns any skew found. A feature
// that is present in the reference but missing online (or vice versa) is itself skew — the
// pipelines disagree about whether the feature exists.
func (c *Checker) Check(ctx context.Context, entity, id string, names []string) ([]SkewReport, error) {
	online, err := c.actual.Get(ctx, entity, id, names)
	if err != nil {
		return nil, fmt.Errorf("monitor: read online: %w", err)
	}
	ref, err := c.reference.Get(ctx, entity, id, names)
	if err != nil {
		return nil, fmt.Errorf("monitor: read reference: %w", err)
	}

	var reports []SkewReport
	for _, name := range names {
		ov, onlineHas := online.Get(name)
		rv, refHas := ref.Get(name)

		switch {
		case !onlineHas && !refHas:
			continue // neither pipeline produced it — not skew, just absence
		case onlineHas != refHas:
			reports = append(reports, c.report(id, name, ov, rv, math.NaN(),
				fmt.Sprintf("presence mismatch (online=%v, reference=%v)", onlineHas, refHas)))
		default:
			if skew, rel, reason := c.compare(ov, rv); skew {
				reports = append(reports, c.report(id, name, ov, rv, rel, reason))
			}
		}
	}
	return reports, nil
}

// compare returns whether two values are in skew, the relative difference (for numerics),
// and a reason. Numeric features use a relative tolerance; non-numeric use exact equality.
func (c *Checker) compare(online, ref domain.Value) (bool, float64, string) {
	on, onNum := numeric(online)
	rn, refNum := numeric(ref)
	if onNum && refNum {
		denom := math.Max(math.Abs(rn), 1e-9)
		rel := math.Abs(on-rn) / denom
		if rel > c.tolerance {
			return true, rel, fmt.Sprintf("numeric drift %.4f exceeds tolerance %.4f", rel, c.tolerance)
		}
		return false, rel, ""
	}
	// Non-numeric: require exact equality.
	if online.Interface() != ref.Interface() {
		return true, math.NaN(), "value mismatch"
	}
	return false, 0, ""
}

func (c *Checker) report(id, name string, online, ref domain.Value, rel float64, reason string) SkewReport {
	c.metrics.SkewDetected.WithLabelValues(name).Inc()
	return SkewReport{
		EntityID:    id,
		FeatureName: name,
		Online:      online.String(),
		Reference:   ref.String(),
		RelDiff:     rel,
		Reason:      reason,
	}
}

// numeric extracts a float64 from a Value if it is int or float, reporting ok=false for
// strings/bools.
func numeric(v domain.Value) (float64, bool) {
	switch v.DType {
	case domain.DataTypeFloat:
		return v.Float, true
	case domain.DataTypeInt:
		return float64(v.Int), true
	default:
		return 0, false
	}
}
