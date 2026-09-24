# ADR-0003 · Offline store format: JSONL partitions now, Parquet/DuckDB in production

**Status:** Accepted · **Date:** 2026-09-24

## Context

The offline store keeps the full *history* of every feature value so we can reconstruct
what a feature was worth at any past moment (point-in-time correctness). Its workload is
the opposite of the online store's: large scans over historical data, write-once,
read-for-training. The canonical technology is **columnar Parquet files** on object storage,
queried by a columnar engine (**DuckDB**, Spark, Trino).

Two constraints shaped the decision:

1. This project must **run and be tested with zero external services and no CGO** (see
   ADR-0002). DuckDB's Go bindings require CGO; a full Parquet+predicate-pushdown setup
   adds substantial surface area.
2. The *teaching* goal is to make the **point-in-time join** legible. That algorithm — "for
   each label, find the latest feature value with `event_time <= label_time`" — is the
   same regardless of file format.

## Decision

Define one `offline.Store` interface and provide two adapters:

- `MemoryOfflineStore` — versioned in-memory history; used by the demo and tests.
- `FileOfflineStore` — **date-partitioned JSONL** on local disk
  (`data/<entity>/date=YYYY-MM-DD/part.jsonl`). It demonstrates the two ideas that matter:
  **partition pruning** (only scan partitions whose date could contain relevant rows) and a
  **point-in-time scan** (filter `event_time <= asOf`, keep the latest per feature).

Document explicitly that production swaps JSONL → Parquet and the file scan → DuckDB/Spark
SQL, behind the **same interface**. The `GetAsOf` docstring shows the equivalent SQL.

## Consequences

**Good**
- The whole store — including point-in-time joins — is testable with `go test`, no CGO, no
  Docker.
- The partitioning and PIT-scan concepts are visible in plain Go, which is the point.

**Costs**
- JSONL is row-oriented and uncompressed: fine for a demo, wrong for terabytes. That is
  exactly the boundary a real Parquet adapter would cross, and the interface is ready for
  it.

## The production shape (for reference)

```sql
-- What GetAsOf does, expressed against a Parquet lake via DuckDB:
SELECT feature_name, value, event_time
FROM read_parquet('s3://fs/features/entity=user_id/*/*.parquet')
WHERE entity_id = ? AND event_time <= ?      -- point-in-time filter
QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_name ORDER BY event_time DESC) = 1;
```
