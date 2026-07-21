FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/api-gateway/go.mod ./services/api-gateway/
COPY services/api-gateway/go.sum ./services/api-gateway/

WORKDIR /workspace/services/api-gateway

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/api-gateway ./services/api-gateway

WORKDIR /workspace/services/api-gateway

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /api-gateway \
      ./cmd/server

FROM gcr.io/distroless/static-debian12

COPY --from=builder /api-gateway /api-gateway

EXPOSE 8000

ENTRYPOINT ["/api-gateway"]