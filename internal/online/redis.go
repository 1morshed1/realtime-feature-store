package online

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// RedisStore is the production online store, backed by Redis hashes.
//
// LEARNING NOTE — why Redis, and why a hash per entity
// ----------------------------------------------------
// Redis keeps everything in memory and speaks a simple protocol, so reads are typically
// sub-millisecond — exactly what the inference critical path needs. We model each entity
// instance as a Redis HASH:
//
//	HSET fs:user_id:123 avg_transaction_7d '{"float":142.5,"dtype":"float64",...}'
//
// A hash (vs one key per feature) lets us:
//   - fetch a subset of features with one HMGET (or all with HGETALL),
//   - update a single feature with one HSET without touching the rest,
//   - keep related features under one key for locality.
//
// We JSON-encode the whole domain.Value (payload + event_time) into each field, so a read
// returns the value AND its freshness with no extra round trip.
type RedisStore struct {
	rdb *redis.Client
}

// NewRedisStore wraps a go-redis client.
func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

// Set writes feature values into the entity's hash using HSET with all field/value pairs
// in one command (one round trip, regardless of how many features).
func (s *RedisStore) Set(ctx context.Context, entity, id string, values []domain.FeatureValue) error {
	if len(values) == 0 {
		return nil
	}
	key := domain.OnlineKey(entity, id)
	pairs := make([]any, 0, len(values)*2)
	for _, v := range values {
		encoded, err := json.Marshal(v.Value)
		if err != nil {
			return fmt.Errorf("online.redis marshal %q: %w", v.FeatureName, err)
		}
		pairs = append(pairs, v.FeatureName, encoded)
	}
	if err := s.rdb.HSet(ctx, key, pairs...).Err(); err != nil {
		return fmt.Errorf("online.redis hset: %w", err)
	}
	return nil
}

// Get reads requested features (HMGET) or all features (HGETALL) for one entity.
func (s *RedisStore) Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error) {
	key := domain.OnlineKey(entity, id)
	fv := domain.FeatureVector{Entity: entity, EntityID: id}

	if len(names) == 0 {
		m, err := s.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return fv, fmt.Errorf("online.redis hgetall: %w", err)
		}
		for name, raw := range m {
			val, err := decodeValue(raw)
			if err != nil {
				return fv, err
			}
			fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: name, Value: val})
		}
		return fv, nil
	}

	// HMGET returns results positionally; a nil entry means that field is absent.
	res, err := s.rdb.HMGet(ctx, key, names...).Result()
	if err != nil {
		return fv, fmt.Errorf("online.redis hmget: %w", err)
	}
	for i, raw := range res {
		if raw == nil {
			continue // feature not present for this entity
		}
		val, err := decodeValue(fmt.Sprint(raw))
		if err != nil {
			return fv, err
		}
		fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: names[i], Value: val})
	}
	return fv, nil
}

// GetBatch fetches many entities with a single pipeline — N logical reads, but one network
// round trip. This is the difference between a serving API that scales and one that melts
// under fan-out.
func (s *RedisStore) GetBatch(ctx context.Context, entity string, ids []string, names []string) ([]domain.FeatureVector, error) {
	pipe := s.rdb.Pipeline()
	// Queue one command per id. We keep the *cmd handles so we can read results in order.
	type pending struct {
		id      string
		all     *redis.MapStringStringCmd
		partial *redis.SliceCmd
	}
	cmds := make([]pending, 0, len(ids))
	for _, id := range ids {
		key := domain.OnlineKey(entity, id)
		if len(names) == 0 {
			cmds = append(cmds, pending{id: id, all: pipe.HGetAll(ctx, key)})
		} else {
			cmds = append(cmds, pending{id: id, partial: pipe.HMGet(ctx, key, names...)})
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("online.redis pipeline exec: %w", err)
	}

	out := make([]domain.FeatureVector, 0, len(ids))
	for _, c := range cmds {
		fv := domain.FeatureVector{Entity: entity, EntityID: c.id}
		if c.all != nil {
			for name, raw := range c.all.Val() {
				val, err := decodeValue(raw)
				if err != nil {
					return nil, err
				}
				fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: name, Value: val})
			}
		} else {
			for i, raw := range c.partial.Val() {
				if raw == nil {
					continue
				}
				val, err := decodeValue(fmt.Sprint(raw))
				if err != nil {
					return nil, err
				}
				fv.Features = append(fv.Features, domain.FeatureValue{FeatureName: names[i], Value: val})
			}
		}
		out = append(out, fv)
	}
	return out, nil
}

func decodeValue(raw string) (domain.Value, error) {
	var v domain.Value
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return domain.Value{}, fmt.Errorf("online.redis decode value: %w", err)
	}
	return v, nil
}
