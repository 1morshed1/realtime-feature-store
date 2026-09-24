# 06 · The Serving API and observability

The serving API is the **data plane**: the endpoint ML inference hits at prediction time.
Everything about it is shaped by one fact — it sits on the critical path of a user-facing
request, so it must be fast, and when it isn't (or when features go stale), you must be
able to see it.

## What `GetFeatures` does

```
request(entity, entity_ids[], features[])
  └─▶ online.GetBatch (one pipelined round trip)
        └─▶ for each returned value: annotate freshness (age, stale?)
              └─▶ record metrics (latency, freshness, hit/miss)
                    └─▶ log served features (training-data reconstruction)
                          └─▶ respond
```

Notice the service depends only on a **narrow port** it defines itself:

```go
type OnlineStore interface {
    GetBatch(ctx, entity, ids, names) ([]FeatureVector, error)
}
```

It never imports Redis. `online.MemoryStore` and `online.RedisStore` both satisfy it, so
serving is tested entirely in-memory. (Hexagonal architecture paying off again —
[doc 01](01-hexagonal-architecture.md).)

## Freshness: the silent-failure detector

Every served value carries an `event_time` (that's why `domain.Value` has one). At serve
time we compute `age = now − event_time` and compare it to the feature's **SLA**:

```go
stale = age > sla(featureName)
```

A stale feature is dangerous precisely because nothing errors — the model just reads an
old number and quietly degrades. Surfacing `age_seconds` and a `stale` flag on every
response, plus a `feature_freshness_seconds` Prometheus gauge, turns an invisible failure
into an alertable one. Features without a registered SLA are never flagged (we don't
guess).

## Feature logging: reconstructing what the model saw

`GetFeatures` calls a `FeatureLogger` with exactly the values it returned. Why log a read?

- **Debugging:** "why did the model flag this transaction?" → replay the exact features it
  was given.
- **Training from live traffic:** logged served-features + later-observed labels = a
  training set that is guaranteed skew-free, because it's literally what production saw.

In this repo the logger is a `MemoryLogger` (inspectable buffer); in production it appends
to Kafka or the offline store.

## The four metrics that matter

| Metric | Type | Question it answers |
|---|---|---|
| `feature_serving_latency_seconds` | histogram | Are we within the p99 latency SLO? |
| `feature_freshness_seconds{feature}` | gauge | How stale is each feature? |
| `feature_cache_hits/misses_total` | counters | What fraction of requested features exist? |
| `skew_detected_total{feature}` | counter | How often does online disagree with batch? |

Design detail worth copying: each `Metrics` instance owns a **private
`prometheus.Registry`** instead of the global default. This avoids "duplicate registration"
panics when multiple services run in one test binary and makes the dependency explicit.
`Metrics.Handler()` serves `/metrics` for Prometheus to scrape.

## Cache hit rate — a subtlety

We only count hits/misses when **specific** features were requested. If a caller asks for
"all" features, we don't know what *should* have existed, so counting a "miss" would be
meaningless. Measuring the right thing sometimes means declining to measure when you can't
define it.

## Why REST here (and gRPC in production)

The serving path is where gRPC earns its keep (binary Protobuf, HTTP/2). We keep the
business logic transport-agnostic and use REST/JSON as the concrete transport so the repo
builds with no `protoc` toolchain. Adding a gRPC server is a thin adapter over the same
`serving.Service`. Full reasoning: [ADR-0004](../adr/0004-serving-protocol-rest-vs-grpc.md).

## Try it

```bash
go test ./internal/serving/...        # freshness, stale flag, hit/miss metrics, logging
```
