package domain

import (
	"fmt"
	"time"
)

// Value is a single feature value together with the timestamp that makes it meaningful.
//
// DESIGN NOTE — why a value carries a timestamp
// ------------------------------------------------
// A naive feature store stores just the number: user:123 -> {avg_7d: 142.50}. That is
// enough to *serve* a model, but it is NOT enough to do two critical things:
//
//  1. Freshness monitoring: "is this value stale?" needs to know WHEN it was computed.
//  2. Point-in-time correctness (training): "what was this value AT the moment the
//     labeled event happened?" needs the full history of (value, event_time) pairs.
//
// So every value here carries an EventTime — the logical time the underlying data
// pertains to — which is different from the wall-clock time we happened to write it.
type Value struct {
	// The typed payload. Exactly one of these is meaningful, selected by DType.
	// We keep them as separate fields (rather than interface{}) so JSON round-trips
	// are lossless and type-safe — an int stays an int, not a float64.
	Float float64  `json:"float,omitempty"`
	Int   int64    `json:"int,omitempty"`
	Str   string   `json:"str,omitempty"`
	Bool  bool     `json:"bool,omitempty"`
	DType DataType `json:"dtype"`

	// EventTime is the logical timestamp the value pertains to (event time), used for
	// point-in-time joins and freshness. NOT the time we wrote the row.
	EventTime time.Time `json:"event_time"`
}

// NewFloat/NewInt/... are small constructors so callers don't have to remember which
// field pairs with which DType.
func NewFloat(v float64, t time.Time) Value {
	return Value{Float: v, DType: DataTypeFloat, EventTime: t}
}
func NewInt(v int64, t time.Time) Value {
	return Value{Int: v, DType: DataTypeInt, EventTime: t}
}
func NewString(v string, t time.Time) Value {
	return Value{Str: v, DType: DataTypeString, EventTime: t}
}
func NewBool(v bool, t time.Time) Value {
	return Value{Bool: v, DType: DataTypeBool, EventTime: t}
}

// Interface returns the underlying Go value regardless of type, handy for logging
// and equality checks.
func (v Value) Interface() any {
	switch v.DType {
	case DataTypeFloat:
		return v.Float
	case DataTypeInt:
		return v.Int
	case DataTypeString:
		return v.Str
	case DataTypeBool:
		return v.Bool
	default:
		return nil
	}
}

// String renders the value for human consumption.
func (v Value) String() string {
	return fmt.Sprintf("%v", v.Interface())
}

// FeatureValue pairs a feature name with its value. It is the atom that flows through
// the whole system: the stream processor produces these, the stores persist them, and
// the serving API returns them.
type FeatureValue struct {
	FeatureName string `json:"feature_name"`
	Value       Value  `json:"value"`
}

// FeatureVector is the bundle of features for a single entity instance at serving time,
// e.g. everything the fraud model needs about user 123. This is the primary object the
// serving API returns and the ML inference service consumes.
type FeatureVector struct {
	Entity   string         `json:"entity"`    // logical entity type, e.g. "user_id"
	EntityID string         `json:"entity_id"` // the concrete id, e.g. "123"
	Features []FeatureValue `json:"features"`
}

// Get returns the value for a named feature and whether it was present. Serving code
// must handle "absent" explicitly — a missing feature is a real production event
// (cold-start entity, upstream lag) and must not silently become a zero.
func (fv FeatureVector) Get(name string) (Value, bool) {
	for _, f := range fv.Features {
		if f.FeatureName == name {
			return f.Value, true
		}
	}
	return Value{}, false
}

// OnlineKey is the Redis key convention for an entity's online feature hash.
// Centralizing the key format here prevents the classic bug where the writer and the
// reader disagree on the key scheme and every lookup silently misses.
func OnlineKey(entity, entityID string) string {
	return fmt.Sprintf("fs:%s:%s", entity, entityID)
}
