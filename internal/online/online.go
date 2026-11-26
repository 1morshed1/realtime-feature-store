// Package online is the ONLINE feature store: the low-latency key-value store that holds
// the *current* value of every feature for every entity, optimized for reads at inference
// time.
//
// DESIGN NOTE — the online store's job and its brutal latency budget
// --------------------------------------------------------------------
// When a model scores a request (is this transaction fraud?), it needs the user's current
// features in single-digit milliseconds, because that read sits on the critical path of a
// user-facing request. The online store exists purely to make that read fast. It holds
// only the LATEST value per (entity, feature) — no history. History lives in the offline
// store (a different workload). This split — "latest, fast" vs "all history, big" — is
// the online/offline dichotomy at the center of every feature store.
//
// Data model: one hash per entity instance.
//
//	key   = fs:<entity>:<id>          e.g. "fs:user_id:123"
//	field = <feature name>            e.g. "avg_transaction_7d"
//	value = JSON-encoded domain.Value (payload + event_time)
//
// Storing each feature as its own hash field lets us read a subset of features cheaply and
// update one feature without rewriting the others. Embedding event_time in the value is
// what makes freshness monitoring possible without a second lookup.
package online

import (
	"context"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// Store is the online-store PORT. Two adapters implement it: RedisStore (production) and
// MemoryStore (tests/demo). Consumers (the serving API) may accept even narrower
// interfaces; this is the full contract the adapters honor.
type Store interface {
	// Set upserts feature values for one entity instance. Existing features not named in
	// values are left untouched (partial update), matching Redis HSET semantics.
	Set(ctx context.Context, entity, id string, values []domain.FeatureValue) error

	// Get returns the requested features for one entity. If names is empty, all stored
	// features for that entity are returned. Missing features are simply absent from the
	// returned vector — the caller must handle absence explicitly (cold start, lag).
	Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error)

	// GetBatch returns feature vectors for many entity ids in one call. Batching is
	// essential for serving: a single prediction often needs features for several
	// entities (user + merchant + device), and one round trip beats N.
	GetBatch(ctx context.Context, entity string, ids []string, names []string) ([]domain.FeatureVector, error)
}
