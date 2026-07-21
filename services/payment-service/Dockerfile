FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/payment-service/go.mod ./services/payment-service/
COPY services/payment-service/go.sum ./services/payment-service/

WORKDIR /workspace/services/payment-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/payment-service ./services/payment-service

WORKDIR /workspace/services/payment-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /payment-service \
      ./cmd/server

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /payment-service /payment-service

EXPOSE 8001

ENTRYPOINT ["/payment-service"]