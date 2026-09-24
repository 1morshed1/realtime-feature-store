# 02 · The Feature Registry

The registry is the **catalog** of the feature store. It answers: *what features exist,
who owns them, what type are they, and how fresh must they be?* It stores **definitions**,
never values.

## Why definitions and values are separate

A `Feature` in our code has a name, entity, dtype, source, computation, freshness SLA, and
owner — but **no number**. The number `142.50` lives in Redis (online) and Parquet
(offline). Why the split?

- Definitions are **metadata**: tiny, relational, changed by humans, queried for
  discovery. → PostgreSQL.
- Values are **data**: huge, key-accessed, changed by machines every second, read under a
  latency budget. → Redis / columnar files.

Mixing them would force one storage technology to be good at two opposite workloads. The
registry keeping only definitions is the first example of a theme you'll see throughout:
**pick the right store for each workload.** (See [ADR-0001](../adr/0001-hexagonal-architecture.md).)

## The port and its two adapters

```go
type Store interface {
    Create(ctx, Feature) error
    Get(ctx, name) (Feature, error)
    List(ctx) ([]Feature, error)
    Update(ctx, Feature) error
    Delete(ctx, name) error
}
```

Two implementations satisfy it:

- `MemoryStore` — a mutex-guarded map. Used by tests and the demo.
- `PostgresStore` — real SQL. Used in production.

Both are held to the **same contract test** (`StoreContract` in `registry_test.go`). When
you add the Postgres integration test later, you call the *same* function with a Postgres
`Store` and it must pass identically. That's how we know the fake didn't lie.

## Design details worth noticing (they generalize)

1. **The service stamps timestamps, not the caller.** `Register` overwrites any
   `CreatedAt`/`UpdatedAt` the client sent. Never trust a client to set server-owned
   fields — someone will send garbage and you'll debug it for hours.

2. **Injected clock.** `NewService(store, clock)` takes a `Clock func() time.Time`. Tests
   pass a fixed clock and assert exact timestamps. Reaching for `time.Now()` directly
   inside business logic makes time-dependent behavior untestable.

3. **Driver-agnostic Postgres adapter.** `postgres.go` imports only `database/sql`. The
   concrete driver (`pgx`) is imported in `cmd/registry` for its registration side effect.
   The adapter even detects a unique-violation without importing the driver, by probing
   for a `SQLState() string` method via `errors.As`. This keeps the adapter portable
   across `pgx`/`lib/pq`.

4. **Domain errors, not driver errors, cross the boundary.** The Postgres adapter
   translates SQLSTATE 23505 into `registry.ErrAlreadyExists`, so callers handle one error
   regardless of backend. Leaking `*pgconn.PgError` upward would recouple everything to
   the driver.

5. **`freshness_ns BIGINT`.** A Go `time.Duration` is an int64 of nanoseconds; storing it
   as a bigint is exact and avoids Postgres `INTERVAL` parsing ambiguity.

## The HTTP API

Standard REST over the Go 1.22+ `net/http` router (method+path patterns, `{name}`
wildcards — no third-party router needed):

```
POST   /features         register
GET    /features         list (browse the catalog)
GET    /features/{name}  fetch one
PUT    /features/{name}  update mutable metadata
DELETE /features/{name}  delete
```

Domain errors map to status codes: validation → 400, duplicate → 409, missing → 404. The
control-plane registry uses REST/JSON precisely because it's low-traffic and
human-operated; the high-traffic *serving* path will use a leaner protocol. Match the
protocol to the traffic.

## Try it

```bash
go test ./internal/registry/...      # unit + contract + HTTP tests, no DB needed
go run ./cmd/registry                # starts on :8081 with the in-memory store
# then, in another shell:
curl -s localhost:8081/features -X POST -d '{
  "name":"user_avg_transaction_7d","entity":"user_id","dtype":"float64",
  "source":"streaming","computation":"AVG(amount) OVER 7 DAYS",
  "freshness_sla":300000000000,"owner":"fraud-ml-team"}' | jq
curl -s localhost:8081/features | jq
```

(`freshness_sla` is nanoseconds in JSON because Go marshals `time.Duration` as its int64
nanosecond count — 300000000000 ns = 5 minutes.)
