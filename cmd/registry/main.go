// Command registry runs the feature-definition catalog as an HTTP service.
//
// Storage is selected by environment:
//   - DATABASE_URL set  -> PostgreSQL (durable, production)
//   - DATABASE_URL unset -> in-memory (great for local dev and the demo)
//
// This is the composition root: the one place that knows about concrete adapters and
// wires them into the business logic. Keeping wiring here (and out of internal packages)
// is what lets the internals stay dependency-light and testable.
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

	// pgx's stdlib driver registers itself with database/sql under the name "pgx".
	// It is imported for that side effect only; the registry package never sees it.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/1morshed1/realtime-feature-store/internal/registry"
)

func main() {
	addr := envOr("REGISTRY_ADDR", ":8081")

	store, cleanup, err := buildStore(context.Background())
	if err != nil {
		log.Fatalf("registry: build store: %v", err)
	}
	defer cleanup()

	svc := registry.NewService(store, nil)
	handler := registry.NewHandler(svc)

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run the server in a goroutine so main can wait for a shutdown signal.
	go func() {
		log.Printf("registry: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("registry: serve: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM — drain in-flight requests before exit.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("registry: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// buildStore chooses the storage adapter based on the environment and returns a cleanup
// function to close any resources.
func buildStore(ctx context.Context) (registry.Store, func(), error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Println("registry: DATABASE_URL not set — using in-memory store")
		return registry.NewMemoryStore(), func() {}, nil
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, nil, err
	}
	pg := registry.NewPostgresStore(db)
	if err := pg.Migrate(ctx); err != nil {
		return nil, nil, err
	}
	log.Println("registry: using PostgreSQL store")
	return pg, func() { _ = db.Close() }, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
