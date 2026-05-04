package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"flowpay/pkg/observability/metrics"
)

type contextKey string
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

const (
	traceIDContextKey   contextKey = "trace_id"
	requestIDContextKey contextKey = "request_id"
)

func generateId(prefix string) string {
	buf := make([]byte, 8)

	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}

func getOrCreateHeaderId(r *http.Request, headerName string, prefix string) string {
	if value := strings.TrimSpace(r.Header.Get(headerName)); value != "" {
		return value
	}

	return generateId(prefix)
}

func GetTraceID(ctx context.Context) string {
	if value, ok := ctx.Value(traceIDContextKey).(string); ok {
		return value
	}
	return ""
}

func GetRequestID(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDContextKey).(string); ok {
		return value
	}
	return ""
}

func TracingMiddleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		traceId := getOrCreateHeaderId(r, "X-Trace-Id", "trace")
		requestId := getOrCreateHeaderId(r, "X-Request-Id", "req")

		ctx := context.WithValue(r.Context(), traceIDContextKey, traceId)
		ctx = context.WithValue(ctx, requestIDContextKey, requestId)

		w.Header().Set("X-Trace-Id", traceId)
		w.Header().Set("X-Request-Id", requestId)

		recorder := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}
		start := time.Now()
		next.ServeHTTP(recorder, r.WithContext(ctx))
		latency := time.Since(start)
		statusLabel := fmt.Sprintf("%d", recorder.statusCode)

		metrics.RequestReceived.WithLabelValues(serviceName, r.URL.Path, r.Method, statusLabel).Inc()
		metrics.RequestLatency.WithLabelValues(serviceName, r.URL.Path, r.Method).Observe(latency.Seconds())
		if recorder.statusCode >= http.StatusBadRequest {
			metrics.ErrorCount.WithLabelValues(serviceName, r.URL.Path, r.Method, statusLabel).Inc()
		}

		// log.Printf(
		// 	"service=%s endpoint=%s trace_id=%s request_id=%s latency=%s status=%d",
		// 	serviceName,
		// 	r.URL.Path,
		// 	traceId,
		// 	requestId,
		// 	latency,
		// 	recorder.statusCode,
		// )

	})
}
