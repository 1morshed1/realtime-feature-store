// Package domain holds the core, dependency-free types of the feature store.
//
// LEARNING NOTE
// -------------
// This package is the "center" of a hexagonal (ports-and-adapters) architecture.
// It contains ONLY business concepts — features, entities, feature values — and it
// imports nothing from Kafka, Redis, Postgres, or HTTP. Every other package depends
// on this one; this one depends on nobody. That direction of dependency is what keeps
// the system testable: we can exercise all the business rules with plain Go values,
// no network, no Docker.
//
// If you only read one file to understand the vocabulary of a feature store, read this one.
package domain

import (
	"fmt"
	"time"
)

// DataType is the value type of a feature. A feature store is schema-aware: it must
// know whether "user_avg_transaction_7d" is a float or an int so it can validate
// writes and serialize values consistently between the training and serving paths.
type DataType string

const (
	DataTypeFloat  DataType = "float64"
	DataTypeInt    DataType = "int64"
	DataTypeString DataType = "string"
	DataTypeBool   DataType = "bool"
)

// Valid reports whether the data type is one the store understands.
func (d DataType) Valid() bool {
	switch d {
	case DataTypeFloat, DataTypeInt, DataTypeString, DataTypeBool:
		return true
	default:
		return false
	}
}

// Source describes HOW a feature is computed. This is one of the most important
// distinctions in a feature store:
//
//   - Streaming features are computed continuously from a live event stream (Kafka).
//     Example: "how many transactions has this user made in the last hour?" — must be
//     fresh to the second for fraud detection.
//   - Batch features are computed on a schedule (nightly Airflow/Spark job) from a
//     warehouse. Example: "lifetime total spent" — expensive to compute, rarely changes.
//
// The store treats them uniformly at serving time (both land in Redis), but their
// freshness expectations and failure modes are completely different.
type Source string

const (
	SourceStreaming Source = "streaming"
	SourceBatch     Source = "batch"
)

// Valid reports whether the source is recognized.
func (s Source) Valid() bool {
	return s == SourceStreaming || s == SourceBatch
}

// Feature is a feature *definition* — the metadata that lives in the registry.
//
// Note carefully: a Feature does NOT contain a value. It is the schema/contract for a
// feature. The actual numbers ("142.50") live in the online store (Redis) and offline
// store (Parquet). Keeping definitions separate from values is what lets multiple teams
// discover, own, and reuse features instead of re-deriving them ad hoc — the core reason
// feature stores exist.
type Feature struct {
	// Name is the globally unique feature name, e.g. "user_avg_transaction_7d".
	// Convention: <entity>_<aggregation>_<window>. Enforced loosely; see Validate.
	Name string `json:"name"`

	// Entity is the thing the feature is about, e.g. "user_id" or "merchant_id".
	// Features are always keyed by an entity; there is no such thing as a "global" feature.
	Entity string `json:"entity"`

	// DType is the value type (see DataType).
	DType DataType `json:"dtype"`

	// Source is streaming or batch (see Source).
	Source Source `json:"source"`

	// Computation is a human-readable description of the math, e.g.
	// "AVG(amount) OVER 7 DAYS". In a fuller system this could be an executable
	// expression; here it is documentation + the key that the stream processor
	// uses to decide which aggregation to run.
	Computation string `json:"computation"`

	// FreshnessSLA is how stale a value is allowed to be before we consider the
	// feature "unhealthy" and raise an alert. Streaming features have tight SLAs
	// (seconds/minutes); batch features have loose ones (a day).
	FreshnessSLA time.Duration `json:"freshness_sla"`

	// Owner is the team responsible, e.g. "fraud-ml-team". Ownership is not
	// bureaucracy — when a feature goes stale at 3am, the on-call needs to know who
	// to page.
	Owner string `json:"owner"`

	// Description is free-form documentation for discovery.
	Description string `json:"description"`

	// CreatedAt / UpdatedAt are registry bookkeeping timestamps.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks that a feature definition is internally consistent before we accept
// it into the registry. Rejecting bad definitions at registration time is far cheaper
// than discovering them when a model silently reads garbage in production.
func (f *Feature) Validate() error {
	if f.Name == "" {
		return fmt.Errorf("feature: name is required")
	}
	if f.Entity == "" {
		return fmt.Errorf("feature %q: entity is required", f.Name)
	}
	if !f.DType.Valid() {
		return fmt.Errorf("feature %q: invalid dtype %q", f.Name, f.DType)
	}
	if !f.Source.Valid() {
		return fmt.Errorf("feature %q: invalid source %q", f.Name, f.Source)
	}
	if f.FreshnessSLA < 0 {
		return fmt.Errorf("feature %q: freshness_sla must not be negative", f.Name)
	}
	if f.Owner == "" {
		return fmt.Errorf("feature %q: owner is required", f.Name)
	}
	return nil
}
