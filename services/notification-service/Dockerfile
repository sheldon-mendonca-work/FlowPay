FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/notification-service/go.mod ./services/notification-service/
COPY services/notification-service/go.sum ./services/notification-service/

WORKDIR /workspace/services/notification-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/notification-service ./services/notification-service

WORKDIR /workspace/services/notification-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /notification-service \
      ./cmd/service

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /notification-service /notification-service

EXPOSE 8008

ENTRYPOINT ["/notification-service"]
