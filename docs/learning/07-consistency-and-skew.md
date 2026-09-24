# 07 · The Consistency Monitor (catching train-serve skew)

We've built the online store, the offline store, and made sure a single stream processor
feeds both so they *should* agree. This doc is about the "should". Pipelines drift.
Someone changes the streaming aggregation but not the batch one. A bug rounds differently.
An upstream schema change breaks one path. The consistency monitor is the smoke detector.

## What skew is, precisely

**Train-serve skew** = the feature value used at serving time differs from the value the
training/batch process computed for the same entity and moment. The model was trained
expecting one distribution of a feature and sees another in production. No error is raised.
Accuracy just erodes.

## How we detect it

Compare two sources for the same features:

- **Actual** = what's being served (the online store).
- **Reference** = the batch recomputation's truth (here: the offline store read
  *as of now*; in production it could be a fresh Spark recompute).

```go
online  := actual.Get(entity, id, names)      // served values
truth   := reference.Get(entity, id, names)   // batch-recomputed values
for each feature:
    compare(online, truth)
```

## The comparison rules (and why they're not "just ==")

Naive equality is wrong for two reasons, both handled in `monitor.compare`:

1. **Numeric features need a tolerance.** Streaming and batch legitimately differ by tiny
   amounts (floating-point order of operations, a few late events). So for numeric
   features we compute a **relative difference** and only flag it when it exceeds a
   tolerance (e.g. 1%). Below tolerance = expected noise; above = a bug worth paging.

   ```
   rel = |online − reference| / max(|reference|, ε)
   skew if rel > tolerance
   ```

2. **Presence mismatch is itself skew.** If the reference has a feature and online doesn't
   (or vice versa), the pipelines disagree about whether the feature even exists — a real
   defect (streaming lag, a dropped write). We report that explicitly, separately from a
   value difference.

Non-numeric features (strings/bools) use exact equality — there's no meaningful "close".

## What happens when skew is found

Each disagreement:

- becomes a `SkewReport` (feature, online value, reference value, relative diff, reason),
  returned to the caller for logging/alerting, and
- increments the `skew_detected_total{feature}` Prometheus counter, so you can alert on
  *rate of skew per feature* and graph which feature is drifting.

## See it happen

The demo (`make demo`, section 6) does exactly this:

1. A **healthy** check: online vs offline-as-of-now → **0 skew reports** (same processor
   fed both, so they agree).
2. It then **injects** a bad online value (simulating a buggy streaming job) and re-checks
   → **1 skew report**, printing the drift and reason, and the `skew_detected_total`
   counter ticks up (visible in section 7's metrics dump).

The tests (`monitor_test.go`) pin each rule: agreement → no report, >tolerance drift →
report, <tolerance drift → tolerated, presence mismatch → report.

## Where this sits in the big picture

This closes the loop the whole project set out to close:

- The **stream processor** computes each feature once and writes it to both stores, so
  online and offline can't skew *by construction*.
- **Point-in-time joins** ensure training uses leak-free historical values.
- The **consistency monitor** continuously verifies the "can't skew by construction"
  assumption actually holds in practice — because in real systems, assumptions rot.

Together they are the defense against the silent accuracy decay that motivated building a
feature store in the first place ([doc 00](00-what-is-a-feature-store.md)).
