# Real-Time Feature Store — developer tasks.
# Every target here runs WITHOUT external services except `up`/`down`.

.PHONY: help test test-race vet fmt build demo up down clean tidy

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'

test: ## Run all unit tests (no Docker needed).
	go test ./...

test-race: ## Run tests with the race detector — important for the concurrent stores.
	go test -race ./...

vet: ## Static analysis.
	go vet ./...

fmt: ## Format all Go code.
	gofmt -s -w .

build: ## Compile all binaries into ./bin.
	@mkdir -p bin
	go build -o bin/ ./cmd/...

demo: ## Run the end-to-end in-memory demo of the whole feature store.
	go run ./cmd/demo

up: ## Start real infrastructure (Kafka, Redis, Postgres, Prometheus) via Docker.
	docker compose -f deploy/docker-compose.yml up -d

down: ## Stop the Docker infrastructure.
	docker compose -f deploy/docker-compose.yml down

tidy: ## Tidy go.mod / go.sum.
	go mod tidy

clean: ## Remove build artifacts.
	rm -rf bin
