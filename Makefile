.PHONY: build test cover bench func-test lint vet tidy run docker clean

BIN := bin/presto-mcp
IMAGE ?= presto-mcp:dev
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	go build -ldflags "-X github.com/yabinma/presto-mcp/internal/server.Version=$(VERSION)" -o $(BIN) ./cmd/presto-mcp

# Unit tests with the race detector.
test:
	go test -race ./...

# Unit tests with the 80% coverage gate (override via COVERAGE_THRESHOLD).
cover:
	./scripts/check-coverage.sh

# Benchmarks across the major code paths (run under -race to surface contention).
bench:
	go test -race -bench=. -benchmem -run='^$$' ./...

# Functional suite: drives every tool against real Trino + Presto (needs Docker).
func-test:
	go test -tags functional -timeout 25m ./test/functional/...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

run: build
	$(BIN) --config config.yaml

# Build the container image (multi-stage, distroless). Override IMAGE/VERSION.
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

clean:
	rm -rf bin coverage.out
