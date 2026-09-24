# 03 · The Online Store (low-latency serving)

The online store answers one question as fast as physically possible: *"what are the
current feature values for this entity?"* It sits on the **critical path** of every
prediction, so its entire design is about latency.

## Online vs offline — the central dichotomy

| | Online store | Offline store |
|---|---|---|
| Question | "what is user 123's avg *now*?" | "what was it on Jan 3 at 14:22?" |
| Contains | latest value only | full history |
| Optimized for | point reads, low latency | large scans, throughput |
| Tech | Redis | Parquet / columnar |
| On critical path? | **yes** (inference) | no (training) |

Every feature store has both. This doc is about the online half; the offline half (and
point-in-time correctness) is [doc 05](05-point-in-time-correctness.md).

## Data model: one Redis hash per entity

```
HSET fs:user_id:123 avg_transaction_7d '{"float":142.5,"dtype":"float64","event_time":"..."}'
HSET fs:user_id:123 transaction_count_1h '{"int":3,"dtype":"int64","event_time":"..."}'
```

- **Key** = `fs:<entity>:<id>` (centralized in `domain.OnlineKey`, so writer and reader
  can never disagree on the format — a classic source of silent cache misses).
- **Field** = feature name.
- **Value** = the JSON-encoded `domain.Value`, which includes the **event_time**.

Why a hash instead of one Redis key per feature?

- `HMGET` fetches a *subset* of features in one command.
- `HGETALL` fetches everything in one command.
- `HSET` updates a single feature without rewriting the others.
- Related features share a key → better locality.

Why embed `event_time` in the value? So a read returns the value **and** its freshness in
a single round trip. Freshness monitoring needs "when was this computed?"; making that a
second lookup would double our latency.

## The API (the port)

```go
type Store interface {
    Set(ctx, entity, id, []FeatureValue) error
    Get(ctx, entity, id, names []string) (FeatureVector, error)
    GetBatch(ctx, entity, ids []string, names []string) ([]FeatureVector, error)
}
```

Two things to internalize:

1. **Missing is not an error.** `Get` for an unknown entity returns an *empty vector*, not
   an error. A cold-start user or a lagging pipeline is a normal production event; serving
   code must decide what to do (use a default, skip the feature) rather than treating
   absence as a crash.

2. **`GetBatch` exists because of fan-out.** One prediction often needs features for
   several entities (user + merchant + device). Doing that as N sequential `Get`s means N
   network round trips. The Redis adapter implements `GetBatch` with a **pipeline**: all N
   commands are sent in one round trip, then results are read back in order. This is the
   single most important performance technique in the serving path.

## The pipeline, concretely

```go
pipe := rdb.Pipeline()
for _, id := range ids {
    cmds = append(cmds, pipe.HMGet(ctx, key(id), names...)) // queued, not sent
}
pipe.Exec(ctx)                                              // ONE round trip
for _, c := range cmds { /* read c.Val() in order */ }
```

Latency of N sequential gets ≈ N × RTT. Latency of a pipeline ≈ 1 × RTT + N × (tiny
server time). At N=50 and RTT=1ms that's 50ms vs ~1ms.

## Testing without Redis

`MemoryStore` mirrors the Redis data model (outer map = key, inner map = feature) and
passes the same `StoreContract` as the Redis adapter. The contract asserts the subtle
things: event_time survives the round trip, partial updates don't clobber, missing ids
yield empty vectors, and batch order is preserved. Run the Redis adapter against the
identical contract with `go test -tags=integration` once `make up` is running.

We also added `TestMemoryConcurrent` — run `go test -race ./internal/online/...` to prove
the store is safe under concurrent writers/readers. The online store is hammered by many
goroutines in production; the race detector is not optional here.
