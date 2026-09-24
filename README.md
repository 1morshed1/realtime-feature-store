# Real-Time ML Feature Store

A production-shaped **feature store** built in Go, with Kafka (real-time feature
computation), Redis (low-latency online serving), and a versioned offline store
(point-in-time correct training data).

> **What is a feature store, in one sentence?**
> It is the data infrastructure that guarantees an ML model sees the *same* feature
> values in production that it saw during training — eliminating the silent accuracy
> killer known as **train-serve skew**.

This repository is also a **teaching project**. Nearly every file is heavily commented,
and the [`docs/`](docs/) folder contains standalone learning notes and Architecture
Decision Records (ADRs) explaining *why* each choice was made. If you are learning ML
infrastructure, read the code top-to-bottom alongside [`docs/learning`](docs/learning).

---

## The problem this solves

Every ML team hits the same wall:

- **Training** features are computed in *batch* from a data warehouse.
- **Serving** features are computed in *real time* from live data.

These two code paths drift apart. The model was trained on `avg_transaction_7d` computed
one way and is served a value computed a slightly different way. Accuracy silently
degrades and nobody notices until a customer complains. A feature store fixes this by
being the **single source of truth** for feature values on both paths.

## Architecture

```
 events ──▶ Kafka (raw-events) ──▶ Stream Processor ──▶ Online Store (Redis)  ──▶ Serving API ──▶ ML inference
                                        │                     ▲
                                        └──▶ Offline Store ────┘ (point-in-time history)
                                                  │
 Feature Registry (definitions) ◀── owns schema ──┘        Consistency Monitor (skew detection)
```

Components (each is a Go package under [`internal/`](internal) with a runnable entrypoint
under [`cmd/`](cmd)):

| Component | Package | What it does |
|-----------|---------|--------------|
| **Domain** | `internal/domain` | Core types: `Feature`, `Value`, `FeatureVector`, `Event`. No I/O. |
| **Registry** | `internal/registry` | Stores/serves feature *definitions* (schema, owner, SLA). |
| **Online store** | `internal/online` | Low-latency current feature values (Redis / in-memory). |
| **Stream processor** | `internal/stream` | Consumes events, computes windowed aggregates. |
| **Offline store** | `internal/offline` | Versioned history for point-in-time training joins. |
| **Serving API** | `internal/serving` | Batched, sub-ms feature reads for inference. |
| **Monitor** | `internal/monitor` | Detects train-serve skew and stale features. |
| **Metrics** | `internal/metrics` | Prometheus instrumentation. |

## The one concept to understand: point-in-time correctness

When training a fraud model you must answer *"what was this user's average transaction
**at the moment** the labeled transaction happened?"* — **not** their current average.
Using current values leaks the future into training (data leakage): the model looks great
in offline evaluation and fails in production. See
[`docs/learning/03-point-in-time-correctness.md`](docs/learning/03-point-in-time-correctness.md).

## Design philosophy: hexagonal architecture with real *and* in-memory adapters

Every external system (Redis, Kafka, Postgres) sits behind a Go interface. Each interface
has **two** implementations:

1. A **real adapter** (Redis, Kafka, ...) used in production via `docker-compose`.
2. An **in-memory fake** used in tests and the `demo` binary.

This means the entire feature store **runs and is fully tested with zero external
dependencies** (`go test ./...`), while the same code runs against real infrastructure in
production. See [ADR-0002](docs/adr/0002-in-memory-and-real-adapters.md).

## Quickstart

```bash
# Run the whole test suite — no Docker, Kafka, or Redis required.
make test

# Run the end-to-end demo: generates events, computes features, serves them,
# and demonstrates a point-in-time training join and skew detection — all in-process.
make demo
```

To run against real infrastructure (Kafka + Redis + Postgres + Prometheus):

```bash
make up      # docker compose up
make down
```

## Repository layout

```
cmd/          # main() entrypoints (registry, streamproc, serving, monitor, demo)
internal/     # the actual implementation, one package per component
docs/
  learning/   # standalone tutorials on the concepts
  adr/        # Architecture Decision Records — the "why" behind each choice
deploy/       # docker-compose, prometheus config
plan.md       # the original project brief
```

## Services and endpoints (Docker stack)

| Service | Port | Key endpoints |
|---------|------|---------------|
| `registry` | 8081 | `POST/GET /features`, `GET /features/{name}` |
| `serving` | 8082 | `POST /getFeatures`, `GET /metrics`, `GET /healthz` |
| `monitor` | 8084 | `GET /metrics`, `GET /healthz` |
| `streamproc` | — | consumes the `raw-events` Kafka topic |
| Prometheus | 9090 | scrapes `serving` and `monitor` |

Example serving call (against the Docker stack):

```bash
curl -s localhost:8082/getFeatures -d '{
  "entity":"user_id","entity_ids":["u1"],
  "features":["txn_count_1h","txn_avg_7d","txn_total_24h"]}' | jq
```

## Project status — complete

All seven components in [`plan.md`](plan.md) are implemented, unit-tested (race-clean), and
runnable end to end. The fastest way to see it work is `make demo`, which runs the whole
pipeline in one process and narrates each stage (registry → events → stream processing →
serving with freshness → point-in-time training join → skew detection → metrics).

See [`docs/README.md`](docs/README.md) for the guided reading path and
[`docs/BUILD_LOG.md`](docs/BUILD_LOG.md) for a narrated, phase-by-phase account of how each
layer was built and why.
