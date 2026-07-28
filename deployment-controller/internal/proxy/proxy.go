package proxy

import (
	"context"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

const upstreamTimeout = 16500 * time.Millisecond

var logger = log.New(os.Stderr, "", log.LstdFlags)

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

func New(resolveTarget func(context.Context) (string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		logger.Printf(
			"Started %s %s from %s",
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
		)

		target, err := resolveTarget(r.Context())
		if err != nil {
			logger.Printf("Proxy target unavailable: %v", err)
			http.Error(w, "Deployment target unavailable", http.StatusServiceUnavailable)
			return
		}

		u, err := url.Parse(target)
		if err != nil {
			logger.Printf("Invalid proxy target %q: %v", target, err)
			http.Error(w, "Deployment target unavailable", http.StatusServiceUnavailable)
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Printf("Proxy error: %v", err)
			http.Error(w, "Service temporarily unavailable", http.StatusBadGateway)
		}

		rec := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
		defer cancel()

		proxy.ServeHTTP(rec, r.WithContext(ctx))

		logger.Printf(
			"Completed %s %s -> %d (%v)",
			r.Method,
			r.URL.Path,
			rec.statusCode,
			time.Since(start),
		)
	})
}
