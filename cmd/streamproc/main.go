// Command streamproc runs the stream processor: consume raw events from Kafka, compute
// windowed features, and write them to the online store (and optionally the offline store).
//
// Config by environment:
//   - KAFKA_BROKERS (csv) + KAFKA_TOPIC (default raw-events) + KAFKA_GROUP (default feature-store)
//   - REDIS_ADDR   -> online store; unset -> in-memory (dev only)
//   - OFFLINE_PATH -> if set, also append history to a file-backed offline store
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
	"github.com/1morshed1/realtime-feature-store/internal/offline"
	"github.com/1morshed1/realtime-feature-store/internal/online"
	"github.com/1morshed1/realtime-feature-store/internal/stream"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("streamproc: KAFKA_BROKERS is required (comma-separated). See deploy/docker-compose.yml")
	}
	source := stream.NewKafkaSource(stream.KafkaConfig{
		Brokers: strings.Split(brokers, ","),
		Topic:   envOr("KAFKA_TOPIC", "raw-events"),
		GroupID: envOr("KAFKA_GROUP", "feature-store"),
	})
	defer source.Close()

	// Build the sinks: online is required, offline is optional.
	sinks, cleanup := buildSinks()
	defer cleanup()

	agg := stream.NewAggregator(defaultSpecs())
	proc := stream.NewProcessor(source, agg, sinks...)

	// Cancel the run on SIGINT/SIGTERM for clean shutdown (commits in-flight offsets).
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop
		log.Println("streamproc: shutting down")
		cancel()
	}()

	log.Printf("streamproc: consuming topic %q", envOr("KAFKA_TOPIC", "raw-events"))
	if err := proc.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("streamproc: %v", err)
	}
}

// buildSinks wires the online store (Redis or memory) and, if OFFLINE_PATH is set, the
// file-backed offline store.
func buildSinks() ([]stream.Sink, func()) {
	var sinks []stream.Sink
	var closers []func()

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Fatalf("streamproc: redis ping: %v", err)
		}
		sinks = append(sinks, online.NewRedisStore(rdb))
		closers = append(closers, func() { _ = rdb.Close() })
		log.Printf("streamproc: online sink = Redis (%s)", addr)
	} else {
		sinks = append(sinks, online.NewMemoryStore())
		log.Println("streamproc: online sink = in-memory (dev only)")
	}

	if path := os.Getenv("OFFLINE_PATH"); path != "" {
		sinks = append(sinks, offline.NewFileOfflineStore(path))
		log.Printf("streamproc: offline sink = files (%s)", path)
	}

	return sinks, func() {
		for _, c := range closers {
			c()
		}
	}
}

// defaultSpecs is the demo's fraud-feature set. In a fuller system these would be derived
// from the registry's feature definitions at startup (and refreshed on change).
func defaultSpecs() []stream.FeatureSpec {
	return []stream.FeatureSpec{
		{FeatureName: "txn_count_1h", Entity: "user_id", EventType: "transaction", Agg: stream.AggCount, Window: time.Hour, DType: domain.DataTypeInt},
		{FeatureName: "txn_avg_7d", Entity: "user_id", EventType: "transaction", Agg: stream.AggAvg, Window: 7 * 24 * time.Hour, DType: domain.DataTypeFloat},
		{FeatureName: "txn_total_24h", Entity: "user_id", EventType: "transaction", Agg: stream.AggSum, Window: 24 * time.Hour, DType: domain.DataTypeFloat},
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
