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

*(Later phases are appended below as they are built.)*
