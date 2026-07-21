FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/auth-service/go.mod ./services/auth-service/
COPY services/auth-service/go.sum ./services/auth-service/

WORKDIR /workspace/services/auth-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/auth-service ./services/auth-service

WORKDIR /workspace/services/auth-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /auth-service \
      ./cmd/service

FROM gcr.io/distroless/static-debian12

COPY --from=builder /auth-service /auth-service

EXPOSE 8007

ENTRYPOINT ["/auth-service"]
