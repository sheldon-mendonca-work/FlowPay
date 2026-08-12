FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY workers/offer-expiry-worker/go.mod ./workers/offer-expiry-worker/
COPY workers/offer-expiry-worker/go.sum ./workers/offer-expiry-worker/

WORKDIR /workspace/workers/offer-expiry-worker

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY workers/offer-expiry-worker ./workers/offer-expiry-worker

WORKDIR /workspace/workers/offer-expiry-worker

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /offer-expiry-worker \
      ./cmd/worker

FROM gcr.io/distroless/static-debian12

COPY --from=builder /offer-expiry-worker /offer-expiry-worker

ENTRYPOINT ["/offer-expiry-worker"]
