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

## Phase 4 — Offline store & point-in-time correctness

**Goal:** the versioned history that makes leak-free training data possible — the crux of
the whole project.

Defined `offline.Store` with `Set` (append versioned history — same signature as the
online Sink, but append not overwrite), `GetAsOf` (latest value with `event_time <= asOf`),
and `PointInTimeJoin` (build a training matrix from labeled rows). Two adapters:

- `MemoryOfflineStore` — per-(entity,feature) history kept sorted by event time; `GetAsOf`
  is a binary search (`sort.Search`), O(log n).
- `FileOfflineStore` — **date-partitioned JSONL** on disk, standing in for a Parquet lake
  ([ADR-0003](adr/0003-offline-store-format.md)). Demonstrates **partition pruning** (skip
  partitions newer than `asOf`) and a point-in-time scan on real files.

The contract test encodes the safety property itself: a value before any history is
invisible, and the point-in-time join fails loudly with `LEAKAGE:` if a future value ever
bleeds into a past label. Both adapters pass it, so they agree on semantics.

This is the concept most ML engineers get wrong; the write-up
[learning/05-point-in-time-correctness.md](learning/05-point-in-time-correctness.md) walks
through the data-leakage trap and the as-of fix in full.

## Phase 5 — Serving API & observability

**Goal:** the data-plane read API, plus the metrics that make the store operable.

`serving.Service.GetFeatures` batches the online read (one pipelined round trip),
annotates each value with **freshness** (`age = now − event_time`, `stale` if it exceeds
the feature's SLA), records metrics, and logs exactly what it served. The service depends
only on a narrow, self-defined `OnlineStore` port and is tested fully in-memory.

`internal/metrics` wraps the four vital signs — serving latency (histogram), freshness
(gauge per feature), cache hit/miss (counters), skew (counter) — each on a **private**
`prometheus.Registry` so multiple services can run in one test binary without
duplicate-registration panics.

Feature logging (`FeatureLogger` + `MemoryLogger`) records served vectors for
training-data reconstruction and live-traffic training sets.

Decisions: [ADR-0004](adr/0004-serving-protocol-rest-vs-grpc.md) keeps serving logic
transport-agnostic with REST/JSON now and gRPC as an additive production adapter. Full
teaching write-up: [learning/06-serving-and-metrics.md](learning/06-serving-and-metrics.md).

## Phase 6 — Consistency monitor, production binaries, and the end-to-end demo

**Goal:** close the loop (verify online and offline actually agree) and make the whole
system runnable — both as one in-memory process and as a real Docker stack.

`monitor.Checker` compares **actual** (online store) against a **reference** (batch
recomputation — here the offline store as-of-now). Comparison rules that matter: numeric
features use a **relative tolerance** (streaming/batch differ by tiny amounts legitimately;
only larger drift is a bug), and a **presence mismatch** (one side has the feature, the
other doesn't) is itself skew. Each disagreement becomes a `SkewReport` and bumps
`skew_detected_total{feature}`.

`cmd/demo` runs the entire flow in one process with in-memory adapters and narrates each
stage: registry → events → stream processing → serving (with freshness) → point-in-time
training join (showing NOW avg 171.4 vs as-of-day-3 avg 110, i.e. leakage avoided) → skew
detection (healthy 0, then injected 1) → metrics. `make demo`.

Production entrypoints wire the real adapters via env: `cmd/registry` (Postgres),
`cmd/streamproc` (Kafka→Redis/offline), `cmd/serving` (Redis + registry SLAs), `cmd/monitor`
(Redis vs offline). `Dockerfile` (static binaries on distroless) + `deploy/docker-compose.yml`
(Kafka KRaft, Redis, Postgres, Prometheus + the four services) make `make up` bring the
real stack online.

Teaching write-up: [learning/07-consistency-and-skew.md](learning/07-consistency-and-skew.md).

---

## The finished system

All seven components from the original spec are implemented, tested (`go test ./...`, race-clean),
and runnable. The data flow the project set out to build — events → one computation →
online+offline → serving + point-in-time training + skew detection — runs end to end. See
[docs/README.md](README.md) for the guided reading path.
