//go:build integration

// This file only compiles/runs under `go test -tags=integration`, and only when a Redis
// server is reachable at REDIS_ADDR (default localhost:6379). It runs the SAME
// StoreContract as the in-memory store, proving the Redis adapter honors the identical
// behavior. Run it with real infra up:
//
//	make up
//	go test -tags=integration ./internal/online/...
package online

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreContract(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}

	// Each contract sub-run needs a clean DB; FlushDB before handing out a store.
	StoreContract(t, func() Store {
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flushdb: %v", err)
		}
		return NewRedisStore(rdb)
	})
}
