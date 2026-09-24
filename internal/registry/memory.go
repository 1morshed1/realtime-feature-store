package registry

import (
	"context"
	"sort"
	"sync"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// MemoryStore is an in-memory implementation of Store, safe for concurrent use.
//
// It is used by the demo and by tests, and it is the executable specification of the
// Store contract: the Postgres adapter must behave identically (same errors, same
// semantics). See ADR-0002.
type MemoryStore struct {
	mu       sync.RWMutex
	features map[string]domain.Feature
}

// NewMemoryStore returns an empty in-memory registry store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{features: make(map[string]domain.Feature)}
}

// Create inserts a feature, returning ErrAlreadyExists if the name is taken.
func (m *MemoryStore) Create(_ context.Context, f domain.Feature) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.features[f.Name]; ok {
		return ErrAlreadyExists
	}
	m.features[f.Name] = f
	return nil
}

// Get returns a feature or ErrNotFound.
func (m *MemoryStore) Get(_ context.Context, name string) (domain.Feature, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f, ok := m.features[name]
	if !ok {
		return domain.Feature{}, ErrNotFound
	}
	return f, nil
}

// List returns all features sorted by name for stable, deterministic output — important
// so tests and API responses don't depend on Go's randomized map iteration order.
func (m *MemoryStore) List(_ context.Context) ([]domain.Feature, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.Feature, 0, len(m.features))
	for _, f := range m.features {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Update replaces an existing feature, returning ErrNotFound if absent.
func (m *MemoryStore) Update(_ context.Context, f domain.Feature) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.features[f.Name]; !ok {
		return ErrNotFound
	}
	m.features[f.Name] = f
	return nil
}

// Delete removes a feature, returning ErrNotFound if absent.
func (m *MemoryStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.features[name]; !ok {
		return ErrNotFound
	}
	delete(m.features, name)
	return nil
}
