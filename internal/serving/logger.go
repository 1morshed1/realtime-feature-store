package serving

import (
	"context"
	"sync"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// LoggedEntry is one record of what serving returned.
type LoggedEntry struct {
	Entity   string
	EntityID string
	Features []domain.FeatureValue
	ServedAt time.Time
}

// MemoryLogger is a FeatureLogger that keeps served entries in memory. In production the
// logger would append to Kafka or the offline store so served features become training
// data; here it's an inspectable buffer for the demo and tests.
type MemoryLogger struct {
	mu      sync.Mutex
	entries []LoggedEntry
}

// NewMemoryLogger returns an empty logger.
func NewMemoryLogger() *MemoryLogger { return &MemoryLogger{} }

// LogServed records one served vector.
func (l *MemoryLogger) LogServed(_ context.Context, entity, entityID string, features []domain.FeatureValue, servedAt time.Time) {
	// Copy the slice so later mutation by the caller can't corrupt the log.
	cp := make([]domain.FeatureValue, len(features))
	copy(cp, features)
	l.mu.Lock()
	l.entries = append(l.entries, LoggedEntry{Entity: entity, EntityID: entityID, Features: cp, ServedAt: servedAt})
	l.mu.Unlock()
}

// Entries returns a snapshot of what has been logged.
func (l *MemoryLogger) Entries() []LoggedEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LoggedEntry, len(l.entries))
	copy(out, l.entries)
	return out
}
