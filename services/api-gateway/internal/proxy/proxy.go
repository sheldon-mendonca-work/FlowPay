package proxy

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"flowpay/api-gateway/internal/middleware"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
)

const upstreamTimeout = 16500 * time.Millisecond

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func New(target, serviceName string) http.Handler {
	u, err := url.Parse(target)
	if err != nil {
		panic("invalid proxy target: " + target)
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)

			ctx := pr.In.Context()

			if traceID := tracing.GetTraceID(ctx); traceID != "" {
				pr.Out.Header.Set("X-Trace-Id", traceID)
			}

			if requestID := tracing.GetRequestID(ctx); requestID != "" {
				pr.Out.Header.Set("X-Request-Id", requestID)
			}

			if accountID := middleware.GetAccountID(ctx); accountID != "" {
				pr.Out.Header.Set("X-Account-Id", accountID)
			}

			if userID := middleware.GetUserID(ctx); userID != "" {
				pr.Out.Header.Set("X-User-Id", userID)
			}

			if companyID := middleware.GetCompanyID(ctx); companyID != "" {
				pr.Out.Header.Set("X-Company-Id", companyID)
			}
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		route := r.URL.Path

		ctx := r.Context()
		if r.Header.Get("Accept") != "text/event-stream" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, upstreamTimeout)
			defer cancel()
			r = r.WithContext(ctx)
		}

		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		logger.LogEvent(r.Context(), "INFO", serviceName, "gateway_request_started", logger.Fields{
			"http_method": r.Method,
			"http_path":   route,
			"user_id":     middleware.GetUserID(r.Context()),
		})

		defer func() {
			durationMs := float64(time.Since(start).Milliseconds())
			outcome := "success"
			if ctx.Err() != nil {
				outcome = "timeout"
			} else if rec.statusCode >= 400 {
				outcome = "error"
			}

			metrics.GatewayRequestsTotal.WithLabelValues(serviceName, outcome).Inc()
			metrics.GatewayRequestDurationMs.WithLabelValues(serviceName, outcome).Observe(durationMs)

			if outcome != "success" {
				metrics.GatewayRequestErrorsTotal.WithLabelValues(serviceName, route).Inc()
				logger.LogEvent(r.Context(), "ERROR", serviceName, "gateway_request_failed", logger.Fields{
					"http_method":      r.Method,
					"http_path":        route,
					"http_status":      rec.statusCode,
					"http_status_text": http.StatusText(rec.statusCode),
					"outcome":          outcome,
					"duration_ms":      durationMs,
				})
			} else {
				logger.LogEvent(r.Context(), "INFO", serviceName, "gateway_request_completed", logger.Fields{
					"http_method": r.Method,
					"http_path":   route,
					"http_status": rec.statusCode,
					"outcome":     outcome,
					"duration_ms": durationMs,
				})
			}
		}()

		proxy.ServeHTTP(rec, r)
	})
}
