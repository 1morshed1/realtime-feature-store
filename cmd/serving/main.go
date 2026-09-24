// Command serving runs the feature Serving API (data plane).
//
// Storage/config by environment:
//   - REDIS_ADDR set    -> Redis online store; unset -> in-memory (dev only)
//   - DATABASE_URL set   -> load feature SLAs from the registry to flag stale features
//   - SERVING_ADDR       -> listen address (default :8082)
//
// This is a composition root: it chooses concrete adapters and injects them into the
// transport-agnostic serving.Service.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/realtime-feature-store/internal/metrics"
	"github.com/1morshed1/realtime-feature-store/internal/online"
	"github.com/1morshed1/realtime-feature-store/internal/registry"
	"github.com/1morshed1/realtime-feature-store/internal/serving"
)

func main() {
	addr := envOr("SERVING_ADDR", ":8082")
	ctx := context.Background()

	store, cleanup := buildOnlineStore(ctx)
	defer cleanup()

	m := metrics.New()
	svc := serving.NewService(store, serving.Options{
		SLA:     buildSLALookup(ctx),
		Metrics: m,
		Logger:  serving.NewMemoryLogger(), // swap for a Kafka/offline-backed logger in prod
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           serving.NewHandler(svc, m),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("serving: listening on %s (metrics at /metrics)", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serving: %v", err)
		}
	}()

	waitForSignal()
	log.Println("serving: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// buildOnlineStore returns a Redis-backed store if REDIS_ADDR is set, else in-memory.
func buildOnlineStore(ctx context.Context) (serving.OnlineStore, func()) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Println("serving: REDIS_ADDR not set — using in-memory online store (dev only)")
		return online.NewMemoryStore(), func() {}
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("serving: redis ping: %v", err)
	}
	log.Printf("serving: using Redis online store at %s", addr)
	return online.NewRedisStore(rdb), func() { _ = rdb.Close() }
}

// buildSLALookup loads feature definitions from the registry (if DATABASE_URL is set) and
// returns a lookup used to flag stale features. Without a registry, no SLAs are known and
// nothing is flagged stale.
func buildSLALookup(ctx context.Context) serving.SLALookup {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Printf("serving: registry open failed, SLAs disabled: %v", err)
		return nil
	}
	features, err := registry.NewPostgresStore(db).List(ctx)
	_ = db.Close()
	if err != nil {
		log.Printf("serving: registry list failed, SLAs disabled: %v", err)
		return nil
	}
	slas := make(map[string]time.Duration, len(features))
	for _, f := range features {
		slas[f.Name] = f.FreshnessSLA
	}
	log.Printf("serving: loaded %d feature SLAs from registry", len(slas))
	return func(name string) (time.Duration, bool) {
		d, ok := slas[name]
		return d, ok
	}
}

func waitForSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
