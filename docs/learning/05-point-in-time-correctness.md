# 05 · Point-in-time correctness (the concept that makes feature stores hard)

If you understand nothing else about feature stores, understand this. It is the single
idea that separates a real feature store from "a cache with extra steps".

## The trap: data leakage

You're training a fraud model. For each historical transaction you have a **label** (fraud
/ not fraud) and a **time** it happened. You want to enrich each with a feature like
`avg_transaction_7d`. The naive code:

```go
// WRONG — reads the CURRENT value
func trainingFeatures(userID string) FeatureVector {
    return onlineStore.Get(userID) // today's avg, not the avg at label time
}
```

The transaction you're labeling happened on **January 3rd**. But `onlineStore.Get` returns
the user's average *today* — which includes months of transactions that happened *after*
January 3rd, including possibly the very fraud you're trying to predict. You have just
leaked the future into the past.

The model trains on features that "know" the outcome. It scores 0.99 AUC offline. You ship
it. In production it can only see the past (the future hasn't happened yet), so it performs
like a coin flip. Nobody gets an error. This is **data leakage via train-serve skew**, and
it is astonishingly common.

## The fix: as-of reads

Training must ask: *"what was `avg_transaction_7d` **as of** January 3rd, the moment this
transaction happened?"* — using only information available at that instant.

```go
// RIGHT — reads the value valid AT the label's timestamp
func trainingFeatures(userID string, labelTime time.Time) FeatureVector {
    return offlineStore.GetAsOf(userID, labelTime) // as-of, leak-free
}
```

`GetAsOf` returns, for each feature, the **latest value whose `event_time <= labelTime`**.
Anything recorded after `labelTime` is invisible. That invisibility is the entire safety
mechanism.

## Why the online store can't do this

The online store keeps only the **latest** value per feature — no history. It physically
cannot answer "what was it on January 3rd". That's why the offline store exists and why it
keeps **versioned history**: every `(feature, value, event_time)` ever computed. The online
and offline stores are fed by the *same* stream processor (so their values agree), but they
store different shapes of data for different questions:

- Online: "value **now**" → one value per feature. Serving.
- Offline: "value **as of T**" → full history per feature. Training.

## How we implement `GetAsOf`

History for each `(entity, feature)` is stored **sorted by event time**. Then:

```
latest value <= asOf  ==  the element just before the first element with event_time > asOf
```

which is a **binary search** (`sort.Search`) — O(log n), not a scan. See
`latestAsOf` in `memory.go`. The file store does the same idea across date-partitioned
files, pruning partitions newer than `asOf` before scanning (`file.go`).

The contract test `offline_test.go` encodes the safety property directly:

- `value before any history is invisible` — asking as-of *before* the first write returns
  **nothing**, not zero, not the later value.
- `point-in-time join attaches leak-free features` — a label on day 2 gets the day-1
  value, never the day-10 "future" value. The test literally fails with the message
  `LEAKAGE:` if that guarantee breaks.

## The point-in-time join

Training data is built by `PointInTimeJoin`: given a table of `(entity_id, timestamp,
label)` rows, it attaches each row's features **as of that row's timestamp**. The result is
a leak-free feature matrix ready for model training. Because both training (via
`GetAsOf`) and serving (via the online store) ultimately read values produced by the same
stream processor, the features the model trains on are the same ones it will see live —
train-serve skew eliminated by construction.

## Mental model to keep

> The online store is a photograph of *now*. The offline store is the full film reel. Never
> train a model on the photograph — train it on the frame that was showing at each label's
> moment.
