package offline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// FileOfflineStore persists versioned history to date-partitioned JSONL files on disk:
//
//	<base>/entity=<entity>/date=YYYY-MM-DD/part.jsonl
//
// It stands in for a production Parquet lake (see ADR-0003) and exists to make two ideas
// concrete on real files:
//
//   - PARTITION PRUNING: GetAsOf only opens partitions whose date is <= asOf's date, so a
//     query for "as of Jan 3" never reads February's files. Partitioning by time is why
//     feature lakes stay queryable at scale.
//   - POINT-IN-TIME SCAN: within the candidate partitions we filter event_time <= asOf and
//     keep the latest value per feature.
//
// The record on disk is deliberately simple, one JSON object per line.
type FileOfflineStore struct {
	base string
	mu   sync.Mutex // serializes appends; JSONL append is not concurrency-safe otherwise
}

// record is one line in a partition file.
type record struct {
	EntityID    string       `json:"entity_id"`
	FeatureName string       `json:"feature_name"`
	Value       domain.Value `json:"value"`
}

// NewFileOfflineStore roots the store at base (created on first write).
func NewFileOfflineStore(base string) *FileOfflineStore {
	return &FileOfflineStore{base: base}
}

const partitionDateLayout = "2006-01-02"

// Set appends each value to the partition selected by its EventTime's UTC date.
func (s *FileOfflineStore) Set(_ context.Context, entity, id string, values []domain.FeatureValue) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Group values by partition date so we open each partition file once.
	byDate := make(map[string][]record)
	for _, v := range values {
		date := v.Value.EventTime.UTC().Format(partitionDateLayout)
		byDate[date] = append(byDate[date], record{EntityID: id, FeatureName: v.FeatureName, Value: v.Value})
	}

	for date, recs := range byDate {
		dir := filepath.Join(s.base, "entity="+entity, "date="+date)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("offline.file mkdir: %w", err)
		}
		f, err := os.OpenFile(filepath.Join(dir, "part.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("offline.file open: %w", err)
		}
		enc := json.NewEncoder(f)
		for _, r := range recs {
			if err := enc.Encode(r); err != nil {
				f.Close()
				return fmt.Errorf("offline.file encode: %w", err)
			}
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("offline.file close: %w", err)
		}
	}
	return nil
}

// GetAsOf scans only the partitions that could contain rows at or before asOf, filters by
// entity id and event_time, and keeps the latest value per feature.
func (s *FileOfflineStore) GetAsOf(_ context.Context, entity, id string, asOf time.Time, names []string) (domain.FeatureVector, error) {
	fv := domain.FeatureVector{Entity: entity, EntityID: id}

	entityDir := filepath.Join(s.base, "entity="+entity)
	partitions, err := s.candidatePartitions(entityDir, asOf)
	if err != nil {
		return fv, err
	}

	wanted := toSet(names) // empty set => accept all feature names
	latest := make(map[string]domain.Value)

	for _, part := range partitions {
		if err := scanPartition(filepath.Join(part, "part.jsonl"), func(r record) {
			if r.EntityID != id {
				return
			}
			if len(wanted) > 0 {
				if _, ok := wanted[r.FeatureName]; !ok {
					return
				}
			}
			if r.Value.EventTime.After(asOf) {
				return // point-in-time filter: future values are invisible
			}
			// Keep the newest value seen for this feature.
			if cur, ok := latest[r.FeatureName]; !ok || r.Value.EventTime.After(cur.EventTime) {
				latest[r.FeatureName] = r.Value
			}
		}); err != nil {
			return fv, err
		}
	}

	// Deterministic output order.
	featNames := make([]string, 0, len(latest))
	for n := range latest {
		featNames = append(featNames, n)
	}
	sort.Strings(featNames)
	for _, n := range featNames {
		fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: n, Value: latest[n]})
	}
	return fv, nil
}

// PointInTimeJoin loops GetAsOf per request. (A production engine would push all requests
// into one vectorized scan; the semantics are identical.)
func (s *FileOfflineStore) PointInTimeJoin(ctx context.Context, entity string, requests []JoinRequest, names []string) ([]TrainingRow, error) {
	rows := make([]TrainingRow, 0, len(requests))
	for _, req := range requests {
		fv, err := s.GetAsOf(ctx, entity, req.EntityID, req.Timestamp, names)
		if err != nil {
			return nil, err
		}
		rows = append(rows, TrainingRow{EntityID: req.EntityID, Timestamp: req.Timestamp, Label: req.Label, Features: fv})
	}
	return rows, nil
}

// candidatePartitions returns the partition directories with date <= asOf's date. This is
// the partition-pruning step: dates after asOf cannot hold point-in-time-valid rows.
func (s *FileOfflineStore) candidatePartitions(entityDir string, asOf time.Time) ([]string, error) {
	entries, err := os.ReadDir(entityDir)
	if os.IsNotExist(err) {
		return nil, nil // nothing written yet for this entity
	}
	if err != nil {
		return nil, fmt.Errorf("offline.file readdir: %w", err)
	}
	cutoff := asOf.UTC().Format(partitionDateLayout)
	var parts []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name() // "date=YYYY-MM-DD"
		date, ok := parseDatePartition(name)
		if !ok {
			continue
		}
		// String comparison works because the date layout is lexicographically ordered.
		if date <= cutoff {
			parts = append(parts, filepath.Join(entityDir, name))
		}
	}
	return parts, nil
}

func parseDatePartition(name string) (string, bool) {
	const prefix = "date="
	if len(name) <= len(prefix) || name[:len(prefix)] != prefix {
		return "", false
	}
	return name[len(prefix):], true
}

func scanPartition(path string, fn func(record)) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("offline.file open partition: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var r record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return fmt.Errorf("offline.file decode line: %w", err)
		}
		fn(r)
	}
	return sc.Err()
}

func toSet(names []string) map[string]struct{} {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}
