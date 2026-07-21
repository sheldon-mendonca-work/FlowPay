FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/reconciliation-service/go.mod ./services/reconciliation-service/
COPY services/reconciliation-service/go.sum ./services/reconciliation-service/

WORKDIR /workspace/services/reconciliation-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/reconciliation-service ./services/reconciliation-service

WORKDIR /workspace/services/reconciliation-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /reconciliation-service \
      ./cmd/service

FROM gcr.io/distroless/static-debian12

COPY --from=builder /reconciliation-service /reconciliation-service

EXPOSE 8013

ENTRYPOINT ["/reconciliation-service"]
