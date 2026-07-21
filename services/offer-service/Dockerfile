FROM golang:1.26.5 AS builder

WORKDIR /workspace

ENV GOWORK=off

# Dependency metadata
COPY pkg/go.mod ./pkg/
COPY pkg/go.sum ./pkg/

COPY services/offer-service/go.mod ./services/offer-service/
COPY services/offer-service/go.sum ./services/offer-service/

WORKDIR /workspace/services/offer-service

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

WORKDIR /workspace

# Source
COPY pkg ./pkg
COPY services/offer-service ./services/offer-service

WORKDIR /workspace/services/offer-service

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /offer-service \
      ./cmd/service

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /offer-service /offer-service

EXPOSE 8010

ENTRYPOINT ["/offer-service"]
