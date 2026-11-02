.DEFAULT_GOAL := test
.PHONY: test race vet build infra-up infra-down integration latency

# Requires infra-up; a fresh evidence directory is created for each run.
latency: build
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/run_latency.py --count 10000 --warmup 500 --rate 100 --out benchmarks/$$(date +%Y%m%d-%H%M%S)-10k-100eps

test:
	go test -p 1 ./...

race:
	go test -p 1 -race ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build -o bin/simulator ./cmd/simulator
	go build -o bin/publisher ./cmd/publisher
	go build -o bin/book-service ./cmd/book-service
	go build -o bin/matching-service ./cmd/matching-service
	go build -o bin/matching-capacity ./cmd/matching-capacity
	go build -o bin/matching-workload ./cmd/matching-workload
	go build -o bin/load-source ./cmd/load-source
	go build -o bin/outcome-relay ./cmd/outcome-relay
	go build -o bin/order-service ./cmd/order-service
	go build -o bin/ticker-matcher ./cmd/ticker-matcher
	go build -o bin/ticker-workload ./cmd/ticker-workload

infra-up:
	docker compose up -d

infra-down:
	docker compose down

integration:
	MTBT_TEST_DATABASE_URL='postgres://mtbt:local-dev-only@127.0.0.1:15432/mtbt?sslmode=disable' MTBT_TEST_NATS_URL='nats://127.0.0.1:14222' go test -p 1 -count=1 -race ./...
