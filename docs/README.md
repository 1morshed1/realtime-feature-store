# Documentation index

This project is built to be *read*. Start here.

## Learning path (read in order)

The `learning/` notes are a self-contained course on feature stores. Each maps to a phase
of the build and to the code it explains.

| # | Doc | Code it explains |
|---|-----|------------------|
| 00 | [What is a feature store?](learning/00-what-is-a-feature-store.md) | the whole thing |
| 01 | [Hexagonal architecture](learning/01-hexagonal-architecture.md) | `internal/*` structure |
| 02 | [The feature registry](learning/02-feature-registry.md) | `internal/registry` |
| 03 | [The online store](learning/03-online-store.md) | `internal/online` |
| 04 | [Stream processing & windows](learning/04-stream-processing-and-windows.md) | `internal/stream` |
| 05 | [Point-in-time correctness](learning/05-point-in-time-correctness.md) ⭐ | `internal/offline` |
| 06 | [Serving & metrics](learning/06-serving-and-metrics.md) | `internal/serving`, `internal/metrics` |
| 07 | [Consistency & skew](learning/07-consistency-and-skew.md) | `internal/monitor` |

⭐ Doc 05 is the single most important concept. If you read one, read that.

## Architecture Decision Records (the "why")

ADRs capture the reasoning behind non-obvious choices, including the tradeoffs and the
alternatives rejected.

| # | Decision |
|---|----------|
| [0001](adr/0001-hexagonal-architecture.md) | Hexagonal (ports & adapters) architecture |
| [0002](adr/0002-in-memory-and-real-adapters.md) | An in-memory adapter for every external dependency |
| [0003](adr/0003-offline-store-format.md) | Offline store format: JSONL now, Parquet/DuckDB in prod |
| [0004](adr/0004-serving-protocol-rest-vs-grpc.md) | Serving protocol: REST now, gRPC as the prod path |

## Build narration

[`BUILD_LOG.md`](BUILD_LOG.md) is a chronological, plain-English account of how each layer
was built and why — read it alongside the git history for the full story.

## How to explore the code

1. Run `make demo` and read its narrated output.
2. Open `cmd/demo/main.go` — it wires every component together and is a map of the system.
3. Follow each component into `internal/<name>` with its learning doc open beside it.
4. Run `go test ./...`; the tests are written to document behavior (each names the property
   it protects).
