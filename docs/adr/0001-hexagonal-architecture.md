# ADR-0001 · Use hexagonal (ports-and-adapters) architecture

**Status:** Accepted · **Date:** 2026-09-24

## Context

The feature store touches many external systems: Kafka, Redis, PostgreSQL, object
storage, Prometheus. A tempting approach is to sprinkle `redis.Client` and Kafka consumer
calls directly through the business logic. That is fast to start but leads to:

- Business rules that can only be tested by standing up Docker containers.
- Tight coupling to a specific client library (swapping Redis for KeyDB touches everything).
- No clear boundary between "what a feature store does" and "how it talks to Redis".

## Decision

Adopt **hexagonal architecture** (a.k.a. ports and adapters):

- **Core / domain** (`internal/domain`) holds pure types with no I/O imports.
- **Ports** are Go interfaces defined *by the core's consumers*, e.g. `OnlineStore`,
  `OfflineStore`, `EventSource`, `Registry`. They describe *what* the system needs, in the
  domain's own vocabulary.
- **Adapters** implement those ports against real tech: `online.RedisStore`,
  `stream.KafkaSource`, `registry.PostgresStore`. There is also an in-memory adapter for
  each port (see [ADR-0002](0002-in-memory-and-real-adapters.md)).

Dependencies always point **inward**: adapters depend on the domain, never the reverse.

```
   HTTP handler ─┐
   Kafka adapter ─┼─▶  service (business logic)  ─▶  domain types
   Redis adapter ─┘         depends on PORTS (interfaces), not concrete tech
```

## Consequences

**Good**

- Business logic is unit-testable with plain Go values and fakes — `go test ./...` needs
  no Docker.
- Swapping infrastructure is a new adapter, not a rewrite.
- The interfaces document exactly what each component requires.

**Costs**

- More indirection: an interface for things that (today) have one real implementation.
- Some boilerplate wiring in `cmd/` to choose adapters.

We accept these costs; for infrastructure software the testability and clarity are worth
far more than the boilerplate.

## Go-specific notes

Idiomatic Go says *"accept interfaces, return structs"* and *define interfaces where they
are consumed, not where they are implemented.* We follow that: e.g. the serving package
defines the `OnlineStore` interface it needs, and the `online` package provides structs
that happen to satisfy it. This keeps interfaces small (often 1–2 methods) and honest.
