# syntax=docker/dockerfile:1

# --- build stage ----------------------------------------------------------
FROM golang:1.26 AS build
WORKDIR /src

# Download modules first so they are cached across source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# VERSION is stamped into the binary; pass --build-arg VERSION=$(git describe ...).
ARG VERSION=0.0.0-docker
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X github.com/yabinma/presto-mcp/internal/server.Version=${VERSION}" \
    -o /out/presto-mcp ./cmd/presto-mcp

# --- runtime stage --------------------------------------------------------
# distroless static + nonroot: no shell, no package manager, runs as uid 65532.
# The image carries no engine secret — in enterprise (passthrough) mode the
# caller's credential rides each request, so nothing sensitive is baked in.
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/presto-mcp /usr/local/bin/presto-mcp

# Streamable-HTTP listen port (match server.http.port in the mounted config).
EXPOSE 8080

# The config is mounted at runtime (e.g. a ConfigMap); it is not part of the image.
# Liveness/readiness should target GET /healthz (distroless has no shell for
# HEALTHCHECK; use the orchestrator's HTTP probe instead).
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/presto-mcp"]
CMD ["--config", "/etc/presto-mcp/config.yaml"]
