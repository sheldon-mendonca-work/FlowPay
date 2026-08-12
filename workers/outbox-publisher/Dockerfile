FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY workers/outbox-publisher/go.mod ./workers/outbox-publisher/
COPY workers/outbox-publisher/go.sum ./workers/outbox-publisher/

WORKDIR /workspace/workers/outbox-publisher

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY workers/outbox-publisher ./workers/outbox-publisher

WORKDIR /workspace/workers/outbox-publisher

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /outbox-publisher \
      ./cmd/worker

FROM gcr.io/distroless/static-debian12

COPY --from=builder /outbox-publisher /outbox-publisher

ENTRYPOINT ["/outbox-publisher"]
