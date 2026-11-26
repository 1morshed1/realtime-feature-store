// Package registry is the feature store's catalog: it stores and serves feature
// *definitions* (not values). Think of it as the schema registry + documentation +
// ownership database for every feature in the organization.
//
// DESIGN NOTE — why a registry is the beating heart of a feature store
// ----------------------------------------------------------------------
// Without a registry, features are "tribal knowledge": a SQL snippet in someone's
// notebook, re-implemented slightly differently by the next team. The registry makes
// features first-class, discoverable objects with a name, a type, an owner, and a
// freshness SLA. Every other component reads the registry to know:
//   - the stream processor: which aggregations to run and what types to emit;
//   - the serving API: which features exist and their SLAs (to flag staleness);
//   - the monitor: what "fresh" means for each feature.
package registry

import (
	"context"
	"errors"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// ErrNotFound is returned when a feature name is not in the registry.
var ErrNotFound = errors.New("registry: feature not found")

// ErrAlreadyExists is returned when registering a name that already exists.
var ErrAlreadyExists = errors.New("registry: feature already exists")

// Store is the persistence PORT for feature definitions. It is deliberately tiny:
// the registry service defines exactly what it needs and nothing more. Adapters
// (memory, Postgres) implement this interface. See ADR-0001.
type Store interface {
	Create(ctx context.Context, f domain.Feature) error
	Get(ctx context.Context, name string) (domain.Feature, error)
	List(ctx context.Context) ([]domain.Feature, error)
	Update(ctx context.Context, f domain.Feature) error
	Delete(ctx context.Context, name string) error
}

// Clock lets tests control time (for CreatedAt/UpdatedAt) instead of using the wall
// clock. Injecting the clock is a small habit that makes timestamp behavior testable.
type Clock func() time.Time

// Service is the business logic of the registry. It validates definitions, stamps
// timestamps, and enforces invariants, delegating storage to a Store.
type Service struct {
	store Store
	now   Clock
}

// NewService builds a registry service. Pass nil for clock to use time.Now.
func NewService(store Store, clock Clock) *Service {
	if clock == nil {
		clock = time.Now
	}
	return &Service{store: store, now: clock}
}

// Register validates and stores a new feature definition. It rejects invalid
// definitions and duplicates. Timestamps are stamped here, not trusted from the caller.
func (s *Service) Register(ctx context.Context, f domain.Feature) (domain.Feature, error) {
	if err := f.Validate(); err != nil {
		return domain.Feature{}, err
	}
	// Ignore caller-supplied timestamps; the registry owns them.
	now := s.now()
	f.CreatedAt = now
	f.UpdatedAt = now

	if err := s.store.Create(ctx, f); err != nil {
		return domain.Feature{}, err
	}
	return f, nil
}

// Get returns a single feature definition by name.
func (s *Service) Get(ctx context.Context, name string) (domain.Feature, error) {
	return s.store.Get(ctx, name)
}

// List returns all feature definitions — the "browse the catalog" operation that lets
// engineers discover existing features before building new ones.
func (s *Service) List(ctx context.Context) ([]domain.Feature, error) {
	return s.store.List(ctx)
}

// Update changes an existing feature's mutable metadata (SLA, owner, description,
// computation). Name/entity/dtype are identity and should not change under the same
// name; we re-validate and refresh UpdatedAt.
func (s *Service) Update(ctx context.Context, f domain.Feature) (domain.Feature, error) {
	if err := f.Validate(); err != nil {
		return domain.Feature{}, err
	}
	existing, err := s.store.Get(ctx, f.Name)
	if err != nil {
		return domain.Feature{}, err
	}
	// Preserve the original creation time; only the update stamp moves.
	f.CreatedAt = existing.CreatedAt
	f.UpdatedAt = s.now()
	if err := s.store.Update(ctx, f); err != nil {
		return domain.Feature{}, err
	}
	return f, nil
}

// Delete removes a feature definition.
func (s *Service) Delete(ctx context.Context, name string) error {
	return s.store.Delete(ctx, name)
}
