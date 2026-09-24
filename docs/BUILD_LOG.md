# Build Log — a narrated walkthrough

This is a chronological, plain-English narration of how the feature store was built,
layer by layer, and the reasoning at each step. Read it alongside the git history: each
section corresponds to one or more commits. It is meant to be a *learning* artifact — if
you were building this yourself, this is the order and the thinking you'd want.

---

## Phase 0 — Scaffolding & the domain layer

**Goal:** establish the vocabulary and the project shape before any I/O.

We started at the *center* of the system — the `internal/domain` package — because every
other package depends on it and it depends on nothing. Getting the core nouns right first
means the rest of the code has a shared language.

Types introduced:

- `Feature` — a feature **definition** (schema, owner, freshness SLA). Crucially it holds
  *no value*; definitions and values are separate concerns.
- `DataType`, `Source` — small enums making the store schema- and origin-aware
  (streaming vs batch).
- `Value` — a typed value that **carries its own `EventTime`**. This one design choice
  (a value knows *when* it pertains to) is what later enables both freshness monitoring
  and point-in-time correctness. It's worth pausing on: most bugs in feature stores come
  from throwing away the timestamp.
- `FeatureValue`, `FeatureVector` — the atoms that flow through serving.
- `Event` — a raw fact from Kafka, also timestamped by **event time**, not processing time.

We also wrote `Feature.Validate()` and a table-driven test for it. Rejecting bad
definitions at registration is far cheaper than debugging garbage in production.

Decisions recorded:
- [ADR-0001](adr/0001-hexagonal-architecture.md): hexagonal architecture.
- [ADR-0002](adr/0002-in-memory-and-real-adapters.md): an in-memory adapter for every
  external dependency, so the whole thing runs and tests without Docker.

Tooling: `Makefile` (test/vet/demo/up/down), `.gitignore`, module
`github.com/1morshed1/realtime-feature-store`.

## Phase 1 — Feature Registry

**Goal:** the catalog — store and serve feature *definitions*, with a real Postgres
adapter and an in-memory fake behind one interface.

We defined the `Store` port (Create/Get/List/Update/Delete) and its `Service` (business
logic: validation, timestamp stamping, invariants). Then two adapters:

- `MemoryStore` — mutex-guarded map, deterministic sorted `List`.
- `PostgresStore` — `database/sql` only; the driver (`pgx`) is registered in
  `cmd/registry`, keeping the adapter driver-agnostic. It even detects unique violations
  via an `errors.As` probe for `SQLState()`, and translates them to `ErrAlreadyExists` so
  callers never see driver-specific errors.

Both adapters are validated by one shared `StoreContract` test — the mechanism that keeps
the fake honest against the real thing.

The HTTP API uses the Go 1.22+ `net/http` router (method+path patterns), mapping domain
errors to status codes (400/404/409). `cmd/registry` is the composition root that picks
the adapter from `DATABASE_URL`.

Key teaching points, written up in [learning/02-feature-registry.md](learning/02-feature-registry.md):
separating definitions from values, injected clocks for testable time, and keeping
driver errors from leaking across the port boundary.

## Phase 2 — Online store (low-latency serving)

**Goal:** the fast key-value store holding the *current* value of every feature, on the
inference critical path.

Defined the `online.Store` port (`Set` / `Get` / `GetBatch`) and two adapters:

- `MemoryStore` — nested maps mirroring the Redis model, mutex-guarded.
- `RedisStore` — one Redis **hash per entity** (`fs:<entity>:<id>`), each field a
  JSON-encoded `domain.Value` (payload + event_time, so a read returns freshness for
  free). `GetBatch` uses a **pipeline** — N logical reads, one network round trip — which
  is the key trick for surviving serving fan-out.

Both adapters share one `StoreContract` test asserting the subtle guarantees: event_time
survives the round trip, partial updates don't clobber, missing ids give empty vectors
(not errors), batch order is preserved. A `//go:build integration` Redis test runs the
same contract against real Redis. Added `TestMemoryConcurrent` (validated under
`go test -race`).

Teaching write-up: [learning/03-online-store.md](learning/03-online-store.md) — the
online/offline dichotomy and why pipelining is the serving path's most important
optimization.

## Phase 3 — Stream processor (event-time windowed aggregations)

**Goal:** turn raw events into features in real time, correctly, over event time.

The core is `Aggregator` — pure, dependency-free windowing math. Each `FeatureSpec`
declares a feature (event type, aggregation, window). For each event we update a
per-(feature, entity) `window` that keeps samples **sorted by event time** and tracks a
**watermark** (max event time = window's right edge). Aggregations: count, sum, avg, rate,
last. Produced values are stamped with `EventTime = watermark`.

Why keep raw samples rather than a running counter? Because a counter cannot evict or
handle out-of-order events. Documented the production tradeoff (ring buffers/sketches,
RocksDB-backed state) rather than hiding it.

Ports: `EventSource` (KafkaSource / MemorySource) and `Sink` (satisfied by both stores).
`Processor` consumes events and fans each computed value out to all sinks. The Kafka
adapter uses consumer groups, partitions by entity (order guarantee), and manual offset
commits for at-least-once processing; poison messages are skipped, not blocking.

Tests assert the subtle properties by name: window slide, out-of-order insertion,
per-entity isolation, event-type filtering, and a full source→aggregator→online-store
pipeline. (One test initially double-counted by replaying events through a shared
aggregator — a good illustration that aggregators are stateful; fixed with a feed-once
helper.)

Deep-dive: [learning/04-stream-processing-and-windows.md](learning/04-stream-processing-and-windows.md).

*(Later phases are appended below as they are built.)*
