# ADR-0002 · Provide an in-memory adapter for every external dependency

**Status:** Accepted · **Date:** 2026-09-24

## Context

This project was developed in an environment with **no Docker daemon**, so Kafka, Redis,
and Postgres could not be run during development or CI. More importantly, even where Docker
*is* available, requiring it for the test suite makes tests slow, flaky, and hard to run.

## Decision

For every port (interface) we provide **two adapters**:

1. A **real** adapter backed by the production technology (`RedisStore`, `KafkaSource`,
   `PostgresStore`).
2. An **in-memory** adapter that implements the exact same interface using Go maps and
   slices (`MemoryStore`, `MemorySource`, ...), safe for concurrent use.

The in-memory adapters are not toys — they implement the full contract, including
concurrency safety and (where relevant) ordering and point-in-time semantics. The demo
binary (`cmd/demo`) wires the entire system together using only in-memory adapters, so the
complete data flow — events → windowed aggregation → online/offline stores → serving →
point-in-time join → skew detection — runs in a single process with `go run`.

## Consequences

**Good**

- `go test ./...` and `make demo` run anywhere, instantly, with no external services.
- The in-memory adapters serve as executable *specifications* of what each real adapter
  must do — a new adapter is "correct" when it passes the same contract tests.
- Fast feedback loop while learning: change code, `go test`, see results in milliseconds.

**Costs**

- The real adapters (Redis/Kafka/Postgres) cannot be exercised in this environment's CI;
  they are validated by code review and are exercised via `docker-compose` locally. Their
  behavior is pinned to the same interface + shared contract tests, which catches most
  divergence.

## Testing strategy that follows from this

- **Contract tests** live next to the interface and run against *any* implementation. Both
  the memory and (when available) the real adapter are fed through the same test function.
- **Unit tests** for business logic use the memory adapters.
- **Integration tests** that need real infra are tagged `//go:build integration` and are
  skipped by default, run only when infra is present.
