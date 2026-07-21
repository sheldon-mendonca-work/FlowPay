FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/payment-executor/go.mod ./services/payment-executor/
COPY services/payment-executor/go.sum ./services/payment-executor/

WORKDIR /workspace/services/payment-executor

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/payment-executor ./services/payment-executor

WORKDIR /workspace/services/payment-executor

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /payment-executor \
      ./cmd/server

FROM gcr.io/distroless/static-debian12

COPY --from=builder /payment-executor /payment-executor

ENTRYPOINT ["/payment-executor"]
