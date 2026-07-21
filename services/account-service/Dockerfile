FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/account-service/go.mod ./services/account-service/
COPY services/account-service/go.sum ./services/account-service/

WORKDIR /workspace/services/account-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/account-service ./services/account-service

WORKDIR /workspace/services/account-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /account-service \
      ./cmd/server

FROM gcr.io/distroless/static-debian12

COPY --from=builder /account-service /account-service

EXPOSE 8005

ENTRYPOINT ["/account-service"]