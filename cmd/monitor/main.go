// Command monitor runs the consistency monitor: on an interval it compares the online
// feature values against a batch recomputation (the offline store as-of-now) and increments
// skew_detected_total when they disagree. It exposes /metrics for Prometheus.
//
// Config by environment:
//   - REDIS_ADDR        -> online store (actual values); unset -> in-memory (dev only)
//   - OFFLINE_PATH      -> file offline store (reference values); required for real checks
//   - MONITOR_ENTITY    -> entity type (default user_id)
//   - MONITOR_IDS       -> comma-separated entity ids to check
//   - MONITOR_FEATURES  -> comma-separated feature names to compare
//   - MONITOR_INTERVAL  -> check interval (default 30s)
//   - MONITOR_TOLERANCE -> allowed relative numeric drift (default 0.01)
//   - MONITOR_ADDR      -> metrics listen address (default :8084)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/metrics"
	"github.com/1morshed1/realtime-feature-store/internal/monitor"
	"github.com/1morshed1/realtime-feature-store/internal/offline"
	"github.com/1morshed1/realtime-feature-store/internal/online"
)

func main() {
	ctx := context.Background()

	actual, cleanup := buildActual(ctx)
	defer cleanup()

	offlinePath := os.Getenv("OFFLINE_PATH")
	if offlinePath == "" {
		log.Fatal("monitor: OFFLINE_PATH is required (the batch/reference store)")
	}
	reference := asOfReference{store: offline.NewFileOfflineStore(offlinePath)}

	m := metrics.New()
	tolerance := floatEnv("MONITOR_TOLERANCE", 0.01)
	checker := monitor.NewChecker(actual, reference, tolerance, m)

	entity := envOr("MONITOR_ENTITY", "user_id")
	ids := splitCSV(os.Getenv("MONITOR_IDS"))
	features := splitCSV(os.Getenv("MONITOR_FEATURES"))
	interval := durationEnv("MONITOR_INTERVAL", 30*time.Second)

	// Serve metrics.
	go func() {
		addr := envOr("MONITOR_ADDR", ":8084")
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", m.Handler())
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
		log.Printf("monitor: metrics at %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("monitor: metrics server: %v", err)
		}
	}()

	// Check loop.
	log.Printf("monitor: checking %d id(s) every %s (tolerance %.3f)", len(ids), interval, tolerance)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		for _, id := range ids {
			reports, err := checker.Check(ctx, entity, id, features)
			if err != nil {
				log.Printf("monitor: check %s/%s: %v", entity, id, err)
				continue
			}
			for _, r := range reports {
				log.Printf("monitor: SKEW %s/%s %s online=%s reference=%s (%s)",
					entity, id, r.FeatureName, r.Online, r.Reference, r.Reason)
			}
		}
		<-ticker.C
	}
}

// asOfReference reads offline values as of the current wall-clock time — "what the batch
// pipeline currently believes the feature should be". It satisfies monitor.Reference.
type asOfReference struct {
	store *offline.FileOfflineStore
}

func (r asOfReference) Get(ctx context.Context, entity, id string, names []string) (domain.FeatureVector, error) {
	return r.store.GetAsOf(ctx, entity, id, time.Now(), names)
}

func buildActual(ctx context.Context) (monitor.Actual, func()) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		log.Println("monitor: REDIS_ADDR not set — using in-memory online store (dev only)")
		return online.NewMemoryStore(), func() {}
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("monitor: redis ping: %v", err)
	}
	return online.NewRedisStore(rdb), func() { _ = rdb.Close() }
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func floatEnv(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func durationEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
