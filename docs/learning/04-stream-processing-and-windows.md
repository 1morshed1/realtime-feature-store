# 04 · Stream processing and event-time windows

This is where raw events become features. It's the most conceptually rich part of the
system, so take your time here.

## The pipeline

```
Kafka(raw-events) ──▶ EventSource ──▶ Aggregator.Update(event) ──▶ [FeatureValue...] ──▶ Sinks(online, offline)
```

- `EventSource` is a port: `KafkaSource` in prod, `MemorySource` in tests/demo.
- `Aggregator` is pure, dependency-free windowing math.
- `Sink` is a port satisfied by both stores; the processor fans each computed value out to
  all sinks (online = servable now, offline = history for training).

## Event time vs processing time — the idea everything hinges on

Two clocks exist for every event:

- **Event time**: when the thing actually happened (set by the producer).
- **Processing time**: when our code happened to see it.

They differ constantly: a phone was offline for an hour, Kafka rebalanced, a consumer
lagged. If you compute "transactions in the last hour" using *processing time* (`now`),
then **replaying the same stream tomorrow gives different features** — because "now" is
different. That destroys reproducibility, which is the entire reason a feature store
exists.

So every window in this system is defined over **event time**. Concretely:

- We track a **watermark** = the maximum event time seen so far. It is the window's right
  edge.
- A window of size `W` retains samples in the half-open interval `(watermark − W,
  watermark]`.

## Out-of-order events

Because event time ≠ arrival order, a "late" event can arrive whose timestamp is in the
past. Our `window.add` keeps samples **sorted by event time**, inserting late events into
their correct position (fast-path append for the common in-order case, slow-path insert
otherwise). A late event that still falls inside the window correctly counts; one that
falls before `watermark − W` is immediately evicted. See `TestOutOfOrder`.

Crucially, a late event does **not** move the watermark backwards (`add` only advances it
on a newer timestamp). The window's right edge only ever moves forward.

## The five aggregations

| Agg | Meaning | Example feature |
|---|---|---|
| `count` | number of events in window | `transaction_count_1h` |
| `sum` | sum of amounts | `total_spent_24h` |
| `avg` | mean amount | `avg_transaction_7d` |
| `rate` | events per minute over window | `clicks_per_min_5m` |
| `last` | most recent amount | `last_transaction_amount` |

They are computed in `compute()` from the retained samples. Each produced `Value` is
stamped with `EventTime = watermark` — "this feature is current as of this logical time" —
which is exactly what the freshness monitor and point-in-time joins later consume.

## Why keep raw samples instead of a running counter?

A running `count++` / `sum +=` is O(1) but **cannot evict** (you can't un-add an event
leaving the window) and **cannot handle out-of-order** inserts. Keeping the samples lets us
recompute correctly for both. The cost is O(n) per event (n = samples in window).

> **Production tradeoff (documented, not hidden):** at high throughput / long windows you
> replace the slice with a bucketed ring buffer (aggregate per small time-bucket, sum the
> buckets) or a probabilistic sketch (e.g. count-min, t-digest for quantiles). The
> *interface* stays the same; only the state representation changes. We chose the explicit
> version because you can read it and see exactly what a window is.

Also note **where the state lives**: `map[feature]map[entityID]*window`, in memory. In a
real deployment (Kafka Streams, Flink) this state is large and must survive crashes, so it
lives in an embedded key-value store (RocksDB) with changelog topics for recovery. The
aggregation math is identical; only the state backend is more serious.

## Kafka specifics you should know

- **Partition by entity.** The `Publisher` keys each message by `EntityID` (`kafka.Hash`
  balancer). All events for one entity therefore land on the same partition and are
  processed **in order** by a single consumer — required for correct per-entity windows.
  If events for one user were spread across partitions and consumers, their order (and
  thus the window) would be nondeterministic.
- **Consumer groups scale you out.** Run more processor instances; Kafka splits partitions
  across the group and rebalances automatically.
- **At-least-once via manual commit.** We commit an event's offset only *after* its
  features are written. Crash before commit → the event is redelivered on restart.
  At-least-once is the safe default: a briefly double-counted event is far less harmful
  than a silently dropped one. See the long comment in `kafka_source.go`.
- **Poison messages.** An undecodable message is committed and skipped (a real system
  routes it to a dead-letter topic) so one bad record can't block a partition forever.

## Try it

```bash
go test -race ./internal/stream/...   # windowing math + pipeline, no Kafka needed
```

Read `aggregate_test.go` alongside this doc — each test names the property it protects
(window slide, out-of-order, per-entity isolation, event-type filtering).
