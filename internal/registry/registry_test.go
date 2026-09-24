package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

func sampleFeature(name string) domain.Feature {
	return domain.Feature{
		Name:         name,
		Entity:       "user_id",
		DType:        domain.DataTypeFloat,
		Source:       domain.SourceStreaming,
		Computation:  "AVG(amount) OVER 7 DAYS",
		FreshnessSLA: 5 * time.Minute,
		Owner:        "fraud-ml-team",
		Description:  "rolling 7-day average transaction amount",
	}
}

// StoreContract exercises the full Store interface. Any implementation (memory,
// Postgres) can be validated by passing a fresh instance here. This is how we guarantee
// the in-memory fake and the real adapter behave identically — see ADR-0002.
func StoreContract(t *testing.T, newStore func() Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("create/get round trip", func(t *testing.T) {
		s := newStore()
		f := sampleFeature("f1")
		if err := s.Create(ctx, f); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := s.Get(ctx, "f1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "f1" || got.Owner != "fraud-ml-team" {
			t.Fatalf("round trip mismatch: %+v", got)
		}
	})

	t.Run("duplicate create is rejected", func(t *testing.T) {
		s := newStore()
		f := sampleFeature("dup")
		_ = s.Create(ctx, f)
		if err := s.Create(ctx, f); !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("want ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("get missing is ErrNotFound", func(t *testing.T) {
		s := newStore()
		if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("list is sorted and complete", func(t *testing.T) {
		s := newStore()
		_ = s.Create(ctx, sampleFeature("b"))
		_ = s.Create(ctx, sampleFeature("a"))
		_ = s.Create(ctx, sampleFeature("c"))
		list, err := s.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) != 3 || list[0].Name != "a" || list[2].Name != "c" {
			t.Fatalf("list not sorted/complete: %+v", list)
		}
	})

	t.Run("update missing is ErrNotFound", func(t *testing.T) {
		s := newStore()
		if err := s.Update(ctx, sampleFeature("ghost")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("delete then get is ErrNotFound", func(t *testing.T) {
		s := newStore()
		_ = s.Create(ctx, sampleFeature("x"))
		if err := s.Delete(ctx, "x"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := s.Get(ctx, "x"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound after delete, got %v", err)
		}
	})
}

// TestMemoryStore runs the shared contract against the in-memory adapter.
func TestMemoryStore(t *testing.T) {
	StoreContract(t, func() Store { return NewMemoryStore() })
}

// fixedClock returns a deterministic clock for timestamp assertions.
func fixedClock(t time.Time) Clock { return func() time.Time { return t } }

func TestServiceStampsTimestamps(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	svc := NewService(NewMemoryStore(), fixedClock(ts))

	// Caller supplies bogus timestamps; the service must overwrite them.
	f := sampleFeature("t")
	f.CreatedAt = time.Unix(0, 0)
	created, err := svc.Register(ctx, f)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !created.CreatedAt.Equal(ts) || !created.UpdatedAt.Equal(ts) {
		t.Fatalf("timestamps not stamped by service: %+v", created)
	}
}

func TestServiceRejectsInvalid(t *testing.T) {
	ctx := context.Background()
	svc := NewService(NewMemoryStore(), nil)
	bad := sampleFeature("bad")
	bad.Owner = "" // violates Validate
	if _, err := svc.Register(ctx, bad); err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

func TestServiceUpdatePreservesCreatedAt(t *testing.T) {
	ctx := context.Background()
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	store := NewMemoryStore()
	svc := NewService(store, fixedClock(created))
	if _, err := svc.Register(ctx, sampleFeature("u")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	svc2 := NewService(store, fixedClock(updated))
	f := sampleFeature("u")
	f.Owner = "new-owner"
	got, err := svc2.Update(ctx, f)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt changed on update: %v", got.CreatedAt)
	}
	if !got.UpdatedAt.Equal(updated) {
		t.Fatalf("UpdatedAt not refreshed: %v", got.UpdatedAt)
	}
}
