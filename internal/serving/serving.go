// Package serving is the feature store's data plane: the low-latency API that ML
// inference calls to fetch features at prediction time. It reads from the online store,
// annotates each value with freshness, records metrics, and logs what it served.
package serving

import (
	"context"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
)

// OnlineStore is the narrow PORT serving needs from the online store: batched reads. Note
// it is defined HERE, by the consumer, not in the online package — classic hexagonal style
// (see ADR-0001). online.MemoryStore and online.RedisStore both satisfy it.
type OnlineStore interface {
	GetBatch(ctx context.Context, entity string, ids []string, names []string) ([]domain.FeatureVector, error)
}

// SLALookup returns the freshness SLA for a feature and whether one is known. Serving uses
// it to flag stale values. It is a plain func so callers can back it with the registry, a
// static map, or nothing.
type SLALookup func(feature string) (time.Duration, bool)

// FeatureLogger records what was served, so a training pipeline can later reconstruct
// EXACTLY the features a model saw for a given prediction — the ground truth for debugging
// "why did the model do that?" and for building training sets from live traffic.
type FeatureLogger interface {
	LogServed(ctx context.Context, entity, entityID string, features []domain.FeatureValue, servedAt time.Time)
}

// Service is the serving business logic.
type Service struct {
	store   OnlineStore
	sla     SLALookup
	logger  FeatureLogger
	metrics *metrics.Metrics
	now     func() time.Time
}

// Options configures optional dependencies. store is required; the rest have safe defaults.
type Options struct {
	SLA     SLALookup
	Logger  FeatureLogger
	Metrics *metrics.Metrics
	Now     func() time.Time
}

// NewService builds the serving service.
func NewService(store OnlineStore, opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.New()
	}
	if opts.Logger == nil {
		opts.Logger = nopLogger{}
	}
	return &Service{store: store, sla: opts.SLA, logger: opts.Logger, metrics: opts.Metrics, now: opts.Now}
}

// GetRequest asks for a set of features for one or more entity ids of the same entity type.
type GetRequest struct {
	Entity    string   `json:"entity"`
	EntityIDs []string `json:"entity_ids"`
	Features  []string `json:"features"` // empty => all stored features
}

// ServedFeature is a feature value annotated with freshness at serving time.
type ServedFeature struct {
	FeatureName string       `json:"feature_name"`
	Value       domain.Value `json:"value"`
	AgeSeconds  float64      `json:"age_seconds"` // now - event_time
	Stale       bool         `json:"stale"`       // AgeSeconds exceeds the feature's SLA
}

// ServedVector is the annotated feature vector for one entity id.
type ServedVector struct {
	EntityID string          `json:"entity_id"`
	Features []ServedFeature `json:"features"`
}

// GetResponse is the serving reply.
type GetResponse struct {
	Entity  string         `json:"entity"`
	Vectors []ServedVector `json:"vectors"`
}

// GetFeatures is the core serving operation. It batches the online read, annotates
// freshness, records metrics, and logs the served values.
func (s *Service) GetFeatures(ctx context.Context, req GetRequest) (GetResponse, error) {
	start := s.now()
	// Measure end-to-end latency regardless of outcome.
	defer func() {
		s.metrics.ServingLatency.Observe(s.now().Sub(start).Seconds())
	}()

	vectors, err := s.store.GetBatch(ctx, req.Entity, req.EntityIDs, req.Features)
	if err != nil {
		return GetResponse{}, err
	}

	resp := GetResponse{Entity: req.Entity, Vectors: make([]ServedVector, 0, len(vectors))}
	servedAt := s.now()

	for _, fv := range vectors {
		sv := ServedVector{EntityID: fv.EntityID, Features: make([]ServedFeature, 0, len(fv.Features))}

		// Cache hit/miss accounting is only meaningful when specific features were
		// requested (with "all", we don't know what was expected to exist).
		if len(req.Features) > 0 {
			s.metrics.Requests.Add(float64(len(req.Features)))
			present := len(fv.Features)
			s.metrics.Hits.Add(float64(present))
			if miss := len(req.Features) - present; miss > 0 {
				s.metrics.Misses.Add(float64(miss))
			}
		}

		for _, f := range fv.Features {
			age := servedAt.Sub(f.Value.EventTime).Seconds()
			stale := false
			if s.sla != nil {
				if sla, ok := s.sla(f.FeatureName); ok && age > sla.Seconds() {
					stale = true
				}
			}
			s.metrics.Freshness.WithLabelValues(f.FeatureName).Set(age)
			sv.Features = append(sv.Features, ServedFeature{
				FeatureName: f.FeatureName,
				Value:       f.Value,
				AgeSeconds:  age,
				Stale:       stale,
			})
		}

		// Log exactly what we served for this entity (training-data reconstruction).
		s.logger.LogServed(ctx, req.Entity, fv.EntityID, fv.Features, servedAt)
		resp.Vectors = append(resp.Vectors, sv)
	}
	return resp, nil
}

// nopLogger is the default FeatureLogger: it discards. Serving must work with no logger
// configured, so the default is a no-op rather than nil (which would panic).
type nopLogger struct{}

func (nopLogger) LogServed(context.Context, string, string, []domain.FeatureValue, time.Time) {}
