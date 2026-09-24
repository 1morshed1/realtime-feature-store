# 00 · What is a feature store, and why does it exist?

> Read this first. It gives you the vocabulary and the *problem* before we write any code.

## The 30-second version

A **feature** is a number (or string/bool) that describes an entity and is fed to an ML
model. Example features about a user:

- `avg_transaction_7d = 142.50`
- `transaction_count_1h = 3`
- `lifetime_total_spent = 28450.00`

A **feature store** is the system that computes, stores, and serves these values so that:

1. The **training** pipeline and the **serving** pipeline get *identical* values.
2. Multiple teams can **discover and reuse** features instead of re-deriving them.
3. Every feature has an **owner**, a **freshness expectation**, and **monitoring**.

## The problem: train-serve skew

This is the reason feature stores exist. Consider a fraud model.

**During training** (offline, batch), you compute `avg_transaction_7d` with a SQL query
over your warehouse:

```sql
SELECT AVG(amount) FROM transactions
WHERE user_id = ? AND ts BETWEEN ? - INTERVAL '7 days' AND ?
```

**During serving** (online, real-time), you cannot run that query — it is too slow and the
warehouse doesn't have the last few seconds of data. So someone writes a *different* piece
of code (maybe a Kafka stream job) to compute "the same" feature.

Now you have **two implementations of one feature**. They differ subtly:

- One includes the current transaction, the other doesn't.
- One uses a rolling 7×24h window, the other uses "last 7 calendar days".
- One treats refunds as negative, the other ignores them.

The model was trained on version A and is served version B. Predictions degrade. There is
no error, no exception, no alert — just quietly worse fraud detection. **This is
train-serve skew, and it is the #1 cause of silent ML failures in production.**

## How a feature store fixes it

```
              ┌─────────────────────────────┐
  events ────▶│  ONE computation path        │
              │  (stream processor)          │
              └──────────┬──────────┬────────┘
                         │          │
              online store          offline store
              (serve now)           (history for training)
                         │          │
                     ▼   ▼          ▼
                  serving API   point-in-time join
                     │              │
                  inference      training data
```

The feature is computed **once**. The result is written to:

- the **online store** (Redis) for fast serving, and
- the **offline store** (versioned history) for building training datasets.

Because both paths read from the same computed values, they cannot skew. And a
**consistency monitor** continuously double-checks by recomputing offline and comparing to
online.

## The four hard problems (and where we solve them in this repo)

| Hard problem | Why it's hard | Where |
|---|---|---|
| **Real-time computation** | Events arrive late & out of order; windows must be over *event time* | `internal/stream` |
| **Low-latency serving** | Inference has a p99 latency budget of a few ms | `internal/online` + `internal/serving` |
| **Point-in-time correctness** | Training must see values *as of* the label time, not now | `internal/offline` |
| **Skew detection** | Failures are silent; you must actively look for them | `internal/monitor` |

## Key terms cheat-sheet

- **Entity** — the thing a feature describes (`user_id`, `merchant_id`). Features are
  always keyed by an entity.
- **Online store** — current values, optimized for reads (Redis). "What is user 123's
  avg *right now*?"
- **Offline store** — full history, optimized for large scans (Parquet/columnar). "What
  was user 123's avg on Jan 3rd at 14:22?"
- **Materialization** — the act of computing feature values and writing them to a store.
- **Freshness / SLA** — how old a value is allowed to be before it's considered stale.
- **Point-in-time join** — joining labels to the feature values that were valid *at each
  label's timestamp*. The heart of correct training data.

Next: [`01-hexagonal-architecture.md`](01-hexagonal-architecture.md) — how we structure
the code so it's testable without Docker.
