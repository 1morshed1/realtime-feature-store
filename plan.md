## Project F: Real-Time ML Feature Store (Go + Kafka + Redis)

### Why This Project

Feature stores are the missing infrastructure layer in most ML teams. Every ML engineer has the same problem: the features used for training (computed in batch from a data warehouse) are different from the features used for inference (computed in real-time from live data). This train-serve skew causes silent accuracy degradation that nobody catches until a user complains.

You've trained models. You've served models. But have you built the infrastructure that ensures the model sees the same feature values in production that it saw during training? That's what a feature store does.

This project uses Go (for the serving layer), Kafka (for real-time feature computation), Redis (for low-latency feature serving), and Spark or a Go stream processor (for batch feature computation). It connects directly to your ML pipeline knowledge (Project C) and your Kafka/Spark skills from the earlier mega-projects.

### Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        FEATURE STORE                                         │
│                                                                              │
│  ┌───────────────────────────────────────────────────┐                       │
│  │  Feature Registry (Go API + PostgreSQL)            │                      │
│  │                                                   │                      │
│  │  Stores feature definitions:                      │                      │
│  │  - name: "user_avg_transaction_7d"                │                      │
│  │  - entity: "user_id"                              │                      │
│  │  - dtype: float64                                 │                      │
│  │  - source: streaming / batch                      │                      │
│  │  - computation: "AVG(amount) OVER 7 DAYS"         │                      │
│  │  - freshness_sla: 5 minutes                       │                      │
│  │  - owner: "fraud-ml-team"                         │                      │
│  └───────────────────────────────────────────────────┘                       │
│                                                                              │
│  DATA FLOW:                                                                  │
│                                                                              │
│  ┌─────────────┐     ┌──────────────────────┐     ┌──────────────────┐      │
│  │ Event Source │────▶│ Kafka                │────▶│ Go Stream        │      │
│  │ (app events,│     │ topic: raw-events     │     │ Processor        │      │
│  │  clicks,    │     │                      │     │                  │      │
│  │  purchases) │     │                      │     │ Computes:        │      │
│  └─────────────┘     └──────────────────────┘     │ - Running avg    │      │
│                                                   │ - Count in window│      │
│                                                   │ - Last-N events  │      │
│                                                   │ - Rate (events/  │      │
│                                                   │   minute)        │      │
│                                                   └────────┬─────────┘      │
│                                                            │                │
│  ┌──────────────────────┐                                  │                │
│  │ Batch Pipeline        │                                  │                │
│  │ (Airflow + Spark/SQL) │                                  │                │
│  │                       │                                  │                │
│  │ Nightly:              │                                  │                │
│  │ - Lifetime stats      │                                  │                │
│  │ - Historical aggs     │──────┐                           │                │
│  │ - Training dataset    │      │                           │                │
│  │   materialization     │      │                           │                │
│  └───────────────────────┘      │                           │                │
│                                 ▼                           ▼                │
│                       ┌──────────────────────────────────────────┐           │
│                       │  Feature Storage                         │           │
│                       │                                          │           │
│                       │  Online Store (Redis)                    │           │
│                       │  key: "user:12345"                       │           │
│                       │  value: {                                │           │
│                       │    "avg_transaction_7d": 142.50,         │           │
│                       │    "transaction_count_1h": 3,            │           │
│                       │    "last_login_seconds_ago": 3600,       │           │
│                       │    "lifetime_total_spent": 28450.00      │           │
│                       │  }                                       │           │
│                       │                                          │           │
│                       │  Offline Store (S3 / Parquet)            │           │
│                       │  Historical feature values for training  │           │
│                       │  Point-in-time correct joins             │           │
│                       └──────────────────────┬───────────────────┘           │
│                                              │                              │
│                                              ▼                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Feature Serving API (Go + gRPC)                                     │   │
│  │                                                                      │   │
│  │  rpc GetFeatures(entity_id, feature_names[]) returns FeatureVector   │   │
│  │                                                                      │   │
│  │  - Sub-millisecond reads from Redis                                  │   │
│  │  - Batched multi-entity lookups                                      │   │
│  │  - Feature freshness monitoring (alert if stale)                     │   │
│  │  - Feature logging for training data reconstruction                  │   │
│  │                                                                      │   │
│  │  Called by:                                                          │   │
│  │  - ML inference service (at prediction time)                         │   │
│  │  - Training pipeline (for dataset generation)                        │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Consistency Monitor                                                 │   │
│  │                                                                      │   │
│  │  - Compares online feature values vs batch-recomputed values         │   │
│  │  - Detects train-serve skew (the #1 cause of silent ML failures)     │   │
│  │  - Alerts if online != offline by more than threshold                │   │
│  │  - Tracks feature freshness (how stale is each feature?)             │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Prometheus: feature_serving_latency, feature_freshness_seconds,             │
│              cache_hit_rate, skew_detected_total                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

### The Key Concept: Point-in-Time Correctness

This is the most important concept in feature stores and most ML engineers get it wrong.

When training a fraud detection model, you need to answer: "What was the user's average transaction amount *at the time* the transaction occurred?" NOT their current average. If you use current values, you're leaking future information into training (data leakage), and your model will appear accurate in training but fail in production.

```go
// WRONG: This uses current feature values for training
func getTrainingFeatures(userID string, transactionTime time.Time) FeatureVector {
    return redis.Get("user:" + userID)  // current values, not historical
}

// RIGHT: Point-in-time lookup from the offline store
func getTrainingFeatures(userID string, transactionTime time.Time) FeatureVector {
    // Query the offline store for feature values AS OF transactionTime
    return offlineStore.GetAsOf(userID, transactionTime)
    // Returns the feature values that were valid at that exact moment
}
```

Building this correctly — maintaining a versioned history of every feature value and supporting efficient point-in-time lookups — is the engineering challenge that makes feature stores hard.

```go
// Offline store: versioned feature storage in Parquet
type OfflineStore struct {
    basePath string  // s3://feature-store/features/
}

func (s *OfflineStore) GetAsOf(entityID string, asOf time.Time) (FeatureVector, error) {
    // Read Parquet files partitioned by date
    // Find the latest feature values for this entity where timestamp <= asOf
    // This is a "point-in-time join"
    
    partitionPath := fmt.Sprintf("%s/entity=%s/", s.basePath, entityID)
    
    // Scan partitions from asOf backwards until we find values
    // In practice, use a columnar query engine (DuckDB, Spark)
    query := fmt.Sprintf(`
        SELECT * FROM read_parquet('%s/*.parquet')
        WHERE entity_id = '%s' AND event_time <= '%s'
        ORDER BY event_time DESC
        LIMIT 1
    `, partitionPath, entityID, asOf.Format(time.RFC3339))
    
    return duckdb.Query(query)
}
```

### Why This Is Special for Your Profile

You've built RAG systems where feature engineering is implicit (the "features" are embeddings). You've deployed ML models via FastAPI. But you've never built the *data infrastructure* that feeds ML models in production. This project proves you understand the full ML lifecycle, not just the model-building part. And you build it in Go with Kafka and Redis — exactly the infrastructure skills you want to develop.

---
---
