FROM golang:1.26.5 AS builder

ENV GOWORK=off

WORKDIR /app

# Copy dependency files
COPY go.mod .
# COPY go.sum .

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source
COPY . .

# Build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOMAXPROCS=1 \
    go build \
      -p=1 \
      -trimpath \
      -ldflags="-s -w" \
      -o /deployment-controller \
      ./cmd/service

FROM gcr.io/distroless/static-debian12:debug

COPY --from=builder /deployment-controller /deployment-controller

EXPOSE 8000

ENTRYPOINT ["/deployment-controller"]
