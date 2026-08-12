FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY workers/offer-outbox-publisher/go.mod ./workers/offer-outbox-publisher/
COPY workers/offer-outbox-publisher/go.sum ./workers/offer-outbox-publisher/

WORKDIR /workspace/workers/offer-outbox-publisher

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY workers/offer-outbox-publisher ./workers/offer-outbox-publisher

WORKDIR /workspace/workers/offer-outbox-publisher

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /offer-outbox-publisher \
      ./cmd/worker

FROM gcr.io/distroless/static-debian12

COPY --from=builder /offer-outbox-publisher /offer-outbox-publisher

ENTRYPOINT ["/offer-outbox-publisher"]
