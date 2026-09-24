package domain

import (
	"testing"
	"time"
)

func TestFeatureValidate(t *testing.T) {
	base := func() Feature {
		return Feature{
			Name:         "user_avg_transaction_7d",
			Entity:       "user_id",
			DType:        DataTypeFloat,
			Source:       SourceStreaming,
			Computation:  "AVG(amount) OVER 7 DAYS",
			FreshnessSLA: 5 * time.Minute,
			Owner:        "fraud-ml-team",
		}
	}

	t.Run("valid", func(t *testing.T) {
		f := base()
		if err := f.Validate(); err != nil {
			t.Fatalf("expected valid feature, got %v", err)
		}
	})

	// Table-driven negative cases: each mutation should make Validate fail. This is the
	// idiomatic Go way to test a validator and it doubles as documentation of the rules.
	cases := map[string]func(*Feature){
		"missing name":   func(f *Feature) { f.Name = "" },
		"missing entity": func(f *Feature) { f.Entity = "" },
		"bad dtype":      func(f *Feature) { f.DType = "complex128" },
		"bad source":     func(f *Feature) { f.Source = "telepathy" },
		"missing owner":  func(f *Feature) { f.Owner = "" },
		"negative sla":   func(f *Feature) { f.FreshnessSLA = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := base()
			mutate(&f)
			if err := f.Validate(); err == nil {
				t.Fatalf("expected error for case %q, got nil", name)
			}
		})
	}
}

func TestValueRoundTrip(t *testing.T) {
	now := time.Now()
	v := NewFloat(142.50, now)
	if v.DType != DataTypeFloat {
		t.Fatalf("dtype = %v, want float64", v.DType)
	}
	if got := v.Interface().(float64); got != 142.50 {
		t.Fatalf("Interface() = %v, want 142.50", got)
	}
}

func TestFeatureVectorGet(t *testing.T) {
	fv := FeatureVector{
		Entity:   "user_id",
		EntityID: "123",
		Features: []FeatureValue{
			{FeatureName: "avg_7d", Value: NewFloat(10, time.Now())},
		},
	}
	if _, ok := fv.Get("avg_7d"); !ok {
		t.Fatal("expected to find avg_7d")
	}
	if _, ok := fv.Get("missing"); ok {
		t.Fatal("expected missing feature to be absent")
	}
}

func TestOnlineKey(t *testing.T) {
	if got := OnlineKey("user_id", "123"); got != "fs:user_id:123" {
		t.Fatalf("OnlineKey = %q, want fs:user_id:123", got)
	}
}
