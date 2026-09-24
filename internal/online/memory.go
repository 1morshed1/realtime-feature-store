package online

import (
	"context"
	"sync"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// MemoryStore is an in-memory online store, safe for concurrent use. It mirrors the Redis
// data model exactly: an outer map keyed by the Redis key, an inner map keyed by feature
// name. This makes it a faithful stand-in and the executable spec for RedisStore.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]map[string]domain.Value // key -> featureName -> Value
}

// NewMemoryStore returns an empty in-memory online store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]map[string]domain.Value)}
}

// Set upserts the given feature values for one entity. Partial update: only the named
// features change.
func (m *MemoryStore) Set(_ context.Context, entity, id string, values []domain.FeatureValue) error {
	key := domain.OnlineKey(entity, id)
	m.mu.Lock()
	defer m.mu.Unlock()
	inner, ok := m.data[key]
	if !ok {
		inner = make(map[string]domain.Value)
		m.data[key] = inner
	}
	for _, v := range values {
		inner[v.FeatureName] = v.Value
	}
	return nil
}

// Get returns the requested (or all) features for one entity.
func (m *MemoryStore) Get(_ context.Context, entity, id string, names []string) (domain.FeatureVector, error) {
	key := domain.OnlineKey(entity, id)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getLocked(entity, id, key, names), nil
}

// GetBatch returns vectors for many ids, taking the read lock once for the whole batch.
func (m *MemoryStore) GetBatch(_ context.Context, entity string, ids []string, names []string) ([]domain.FeatureVector, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.FeatureVector, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.getLocked(entity, id, domain.OnlineKey(entity, id), names))
	}
	return out, nil
}

// getLocked builds a FeatureVector; caller must hold at least the read lock. Splitting
// this out keeps Get and GetBatch consistent and lock-correct.
func (m *MemoryStore) getLocked(entity, id, key string, names []string) domain.FeatureVector {
	fv := domain.FeatureVector{Entity: entity, EntityID: id}
	inner, ok := m.data[key]
	if !ok {
		return fv // no data for this entity — an empty vector, not an error
	}
	if len(names) == 0 {
		// Return everything.
		for name, val := range inner {
			fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: name, Value: val})
		}
		return fv
	}
	// Return only requested features that are present.
	for _, name := range names {
		if val, ok := inner[name]; ok {
			fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: name, Value: val})
		}
	}
	return fv
}
