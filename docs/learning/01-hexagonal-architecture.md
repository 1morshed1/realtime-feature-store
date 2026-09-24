# 01 · Hexagonal architecture, explained with this codebase

You will hear "hexagonal architecture", "ports and adapters", and "clean architecture".
They are roughly the same idea. Here is the practical version, grounded in our code.

## The one rule

> **Dependencies point inward. The business logic never imports infrastructure.**

Picture three rings:

```
        ┌─────────────────────────────────────────┐
        │  adapters (outer): Redis, Kafka, HTTP    │
        │   ┌───────────────────────────────────┐  │
        │   │  services (middle): registry,      │  │
        │   │  stream, serving, monitor          │  │
        │   │   ┌─────────────────────────────┐  │  │
        │   │   │  domain (inner): Feature,    │  │  │
        │   │   │  Value, Event — no I/O       │  │  │
        │   │   └─────────────────────────────┘  │  │
        │   └───────────────────────────────────┘  │
        └─────────────────────────────────────────┘
```

- The **domain** ring (`internal/domain`) imports nothing but the standard library.
- The **service** ring contains business logic and defines **ports** — Go interfaces like
  `OnlineStore` — describing what it needs.
- The **adapter** ring implements those ports against real technology.

## Ports are interfaces, defined by the consumer

A *port* is just a Go interface. The trick is **where** it's defined: in the package that
*uses* it, not the package that *implements* it. Example (simplified):

```go
// internal/serving/serving.go  — the CONSUMER declares what it needs
type OnlineStore interface {
    GetFeatures(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error)
}

// internal/online/redis.go — the ADAPTER satisfies it, without importing serving
type RedisStore struct { /* ... */ }
func (s *RedisStore) GetFeatures(...) (domain.FeatureVector, error) { /* ... */ }
```

`RedisStore` never mentions `serving`. `serving` never mentions Redis. They meet only
through the interface and the shared `domain` types. That's the whole trick.

## Why this pays off here

1. **Tests need no Docker.** We pass a `MemoryStore` where production passes `RedisStore`.
   Same interface, so the service can't tell the difference — see
   [ADR-0002](../adr/0002-in-memory-and-real-adapters.md).
2. **Swapping tech is local.** Move from Redis to KeyDB? Write one new adapter. No service
   code changes.
3. **The interfaces are documentation.** Want to know everything the serving layer needs
   from storage? Read its port. It's a couple of methods, not a sprawling client API.

## Common beginner mistake

Defining a giant `Store` interface with 20 methods in one shared package and having
everything depend on it. That recreates the coupling you were trying to avoid. Keep ports
**small** and **local** to their consumer. In Go, a one-method interface is not a smell —
it's the goal.
