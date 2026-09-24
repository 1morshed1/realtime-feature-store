package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// PostgresStore is the production adapter for Store, backed by PostgreSQL.
//
// LEARNING NOTE — why Postgres for the registry (but Redis for values)?
// ---------------------------------------------------------------------
// Feature *definitions* are low-volume, highly-relational, and must be durable and
// queryable ("show me every feature the fraud team owns with an SLA under 5 minutes").
// That is exactly what a relational database is good at. Feature *values* are
// high-volume, key-access, latency-critical — that is Redis's job. Using the right store
// for each workload is a recurring theme in data infrastructure.
//
// This adapter imports only database/sql — no concrete driver. The driver (e.g.
// github.com/jackc/pgx/v5/stdlib) is registered for its side effect in cmd/registry,
// which keeps this package free of a heavy dependency and lets the whole registry
// package be tested with the in-memory store alone.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wraps an already-open *sql.DB. The caller opens the connection with
// its chosen driver, which inverts the dependency: this package doesn't pick the driver.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// Schema is the DDL for the registry table. FreshnessSLA is stored as nanoseconds
// (BIGINT) because that is the exact, lossless representation of a Go time.Duration —
// no interval-parsing ambiguity between app and DB.
const Schema = `
CREATE TABLE IF NOT EXISTS features (
    name          TEXT PRIMARY KEY,
    entity        TEXT NOT NULL,
    dtype         TEXT NOT NULL,
    source        TEXT NOT NULL,
    computation   TEXT NOT NULL DEFAULT '',
    freshness_ns  BIGINT NOT NULL DEFAULT 0,
    owner         TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);`

// Migrate creates the table if it does not exist. In a larger system you'd use a
// migration tool (golang-migrate, goose); here one idempotent statement is enough.
func (p *PostgresStore) Migrate(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, Schema)
	return err
}

func (p *PostgresStore) Create(ctx context.Context, f domain.Feature) error {
	const q = `
INSERT INTO features (name, entity, dtype, source, computation, freshness_ns, owner, description, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	_, err := p.db.ExecContext(ctx, q,
		f.Name, f.Entity, f.DType, f.Source, f.Computation,
		int64(f.FreshnessSLA), f.Owner, f.Description, f.CreatedAt, f.UpdatedAt)
	if err != nil {
		// Translate the unique-violation into our domain error so callers get the same
		// ErrAlreadyExists regardless of which Store implementation they use.
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("registry.postgres create: %w", err)
	}
	return nil
}

func (p *PostgresStore) Get(ctx context.Context, name string) (domain.Feature, error) {
	const q = `
SELECT name, entity, dtype, source, computation, freshness_ns, owner, description, created_at, updated_at
FROM features WHERE name = $1`
	return scanFeature(p.db.QueryRowContext(ctx, q, name))
}

func (p *PostgresStore) List(ctx context.Context) ([]domain.Feature, error) {
	const q = `
SELECT name, entity, dtype, source, computation, freshness_ns, owner, description, created_at, updated_at
FROM features ORDER BY name`
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("registry.postgres list: %w", err)
	}
	defer rows.Close()

	var out []domain.Feature
	for rows.Next() {
		f, err := scanFeature(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (p *PostgresStore) Update(ctx context.Context, f domain.Feature) error {
	const q = `
UPDATE features
SET entity=$2, dtype=$3, source=$4, computation=$5, freshness_ns=$6, owner=$7, description=$8, created_at=$9, updated_at=$10
WHERE name=$1`
	res, err := p.db.ExecContext(ctx, q,
		f.Name, f.Entity, f.DType, f.Source, f.Computation,
		int64(f.FreshnessSLA), f.Owner, f.Description, f.CreatedAt, f.UpdatedAt)
	if err != nil {
		return fmt.Errorf("registry.postgres update: %w", err)
	}
	return requireOneRow(res)
}

func (p *PostgresStore) Delete(ctx context.Context, name string) error {
	res, err := p.db.ExecContext(ctx, `DELETE FROM features WHERE name=$1`, name)
	if err != nil {
		return fmt.Errorf("registry.postgres delete: %w", err)
	}
	return requireOneRow(res)
}

// --- small helpers -------------------------------------------------------------------

// rowScanner is satisfied by both *sql.Row and *sql.Rows, so scanFeature works for
// single-row and multi-row queries alike.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanFeature(r rowScanner) (domain.Feature, error) {
	var (
		f     domain.Feature
		fresh int64
	)
	err := r.Scan(&f.Name, &f.Entity, &f.DType, &f.Source, &f.Computation,
		&fresh, &f.Owner, &f.Description, &f.CreatedAt, &f.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Feature{}, ErrNotFound
	}
	if err != nil {
		return domain.Feature{}, fmt.Errorf("registry.postgres scan: %w", err)
	}
	f.FreshnessSLA = time.Duration(fresh)
	return f, nil
}

func requireOneRow(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// isUniqueViolation detects a Postgres unique-constraint error without importing a
// specific driver. Postgres SQLSTATE 23505 is "unique_violation"; pgx and lib/pq both
// expose a .SQLState() method, so we probe for it via an interface. This keeps the
// adapter driver-agnostic.
func isUniqueViolation(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	if errors.As(err, &s) {
		return s.SQLState() == "23505"
	}
	return false
}
