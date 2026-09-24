// Package offline is the OFFLINE feature store: the versioned history of every feature
// value, used to build point-in-time-correct training datasets.
//
// LEARNING NOTE — the single most important idea in the whole project
// ------------------------------------------------------------------
// The online store answers "what is the value NOW?". The offline store answers "what was
// the value AS OF some past moment?". That second question is what makes correct model
// training possible, and getting it wrong is the #1 cause of models that look great
// offline and fail in production (data leakage). Read
// docs/learning/05-point-in-time-correctness.md next to this file.
package offline

import (
	"context"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// Store is the offline-store PORT.
type Store interface {
	// Set records feature values as VERSIONED history. Unlike the online store's Set
	// (which overwrites), each call here APPENDS: it records that at value.EventTime the
	// feature had this value. The method is named Set so the offline store also satisfies
	// stream.Sink and can be wired as a processor sink alongside the online store.
	Set(ctx context.Context, entity, id string, values []domain.FeatureValue) error

	// GetAsOf returns the point-in-time-correct feature vector for one entity: for each
	// requested feature, the LATEST value whose EventTime <= asOf. Values written with an
	// EventTime after asOf are invisible — that invisibility is what prevents future
	// information from leaking into training.
	//
	// Equivalent SQL against a Parquet lake (see ADR-0003):
	//
	//	SELECT feature_name, value FROM features
	//	WHERE entity_id = ? AND event_time <= ?
	//	QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_name ORDER BY event_time DESC) = 1
	GetAsOf(ctx context.Context, entity, id string, asOf time.Time, names []string) (domain.FeatureVector, error)

	// PointInTimeJoin builds a training dataset: for each request row it attaches the
	// features as-of that row's Timestamp. This is the operation a training pipeline calls
	// to turn a table of labeled events into a feature matrix without leakage.
	PointInTimeJoin(ctx context.Context, entity string, requests []JoinRequest, names []string) ([]TrainingRow, error)
}

// JoinRequest is one labeled example to enrich with features: "for entity EntityID at time
// Timestamp, the outcome was Label". The join must attach the features that were valid at
// Timestamp — not now.
type JoinRequest struct {
	EntityID  string    `json:"entity_id"`
	Timestamp time.Time `json:"timestamp"`
	Label     float64   `json:"label"`
}

// TrainingRow is one row of the produced training dataset: the original request plus the
// point-in-time-correct features.
type TrainingRow struct {
	EntityID  string               `json:"entity_id"`
	Timestamp time.Time            `json:"timestamp"`
	Label     float64              `json:"label"`
	Features  domain.FeatureVector `json:"features"`
}
