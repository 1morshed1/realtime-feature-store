package offline

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// MemoryOfflineStore keeps versioned history in memory. Layout:
//
//	key(entity,id) -> featureName -> []domain.Value  (sorted ascending by EventTime)
//
// The sorted slice is what makes GetAsOf a binary search instead of a full scan.
type MemoryOfflineStore struct {
	mu   sync.RWMutex
	data map[string]map[string][]domain.Value
}

// NewMemoryOfflineStore returns an empty offline store.
func NewMemoryOfflineStore() *MemoryOfflineStore {
	return &MemoryOfflineStore{data: make(map[string]map[string][]domain.Value)}
}

// Set appends each value into its feature's history, keeping the slice sorted by EventTime
// so reads can binary-search. Appending (not overwriting) is the whole point: history is
// preserved.
func (s *MemoryOfflineStore) Set(_ context.Context, entity, id string, values []domain.FeatureValue) error {
	key := domain.OnlineKey(entity, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	features, ok := s.data[key]
	if !ok {
		features = make(map[string][]domain.Value)
		s.data[key] = features
	}
	for _, v := range values {
		features[v.FeatureName] = insertSorted(features[v.FeatureName], v.Value)
	}
	return nil
}

// GetAsOf returns, for each requested feature, the latest value with EventTime <= asOf.
func (s *MemoryOfflineStore) GetAsOf(_ context.Context, entity, id string, asOf time.Time, names []string) (domain.FeatureVector, error) {
	key := domain.OnlineKey(entity, id)
	fv := domain.FeatureVector{Entity: entity, EntityID: id}

	s.mu.RLock()
	defer s.mu.RUnlock()
	features, ok := s.data[key]
	if !ok {
		return fv, nil // no history for this entity -> empty vector (a real, expected state)
	}

	// If names is empty, consider every feature this entity has.
	want := names
	if len(want) == 0 {
		want = make([]string, 0, len(features))
		for name := range features {
			want = append(want, name)
		}
		sort.Strings(want) // deterministic output
	}

	for _, name := range want {
		history := features[name]
		if v, ok := latestAsOf(history, asOf); ok {
			fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: name, Value: v})
		}
		// If ok is false, every recorded value is newer than asOf: correctly invisible.
	}
	return fv, nil
}

// PointInTimeJoin attaches point-in-time features to each request row.
func (s *MemoryOfflineStore) PointInTimeJoin(ctx context.Context, entity string, requests []JoinRequest, names []string) ([]TrainingRow, error) {
	rows := make([]TrainingRow, 0, len(requests))
	for _, req := range requests {
		fv, err := s.GetAsOf(ctx, entity, req.EntityID, req.Timestamp, names)
		if err != nil {
			return nil, err
		}
		rows = append(rows, TrainingRow{
			EntityID:  req.EntityID,
			Timestamp: req.Timestamp,
			Label:     req.Label,
			Features:  fv,
		})
	}
	return rows, nil
}

// --- helpers -------------------------------------------------------------------------

// insertSorted inserts v into history keeping ascending EventTime order. History is
// usually appended in time order (fast path), but out-of-order writes are handled too.
func insertSorted(history []domain.Value, v domain.Value) []domain.Value {
	if len(history) == 0 || !v.EventTime.Before(history[len(history)-1].EventTime) {
		return append(history, v)
	}
	i := sort.Search(len(history), func(i int) bool {
		return history[i].EventTime.After(v.EventTime)
	})
	history = append(history, domain.Value{})
	copy(history[i+1:], history[i:])
	history[i] = v
	return history
}

// latestAsOf returns the last value whose EventTime <= asOf, using binary search over the
// ascending-sorted history. Returns ok=false if no such value exists (all newer than asOf).
func latestAsOf(history []domain.Value, asOf time.Time) (domain.Value, bool) {
	// Find the first index with EventTime strictly greater than asOf; the one before it is
	// the latest value <= asOf.
	i := sort.Search(len(history), func(i int) bool {
		return history[i].EventTime.After(asOf)
	})
	if i == 0 {
		return domain.Value{}, false
	}
	return history[i-1], true
}
