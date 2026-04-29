package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var RequestCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "request_count",
		Help: "Total Number of HTTP requests received",
	},
	[]string{"service", "endpoint", "method", "status"},
)

var ErrorCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "error_count",

		Help: "Total number of HTTP requests resulting in an error response.",
	},
	[]string{"service", "endpoint", "method", "status"},
)

var RequestLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "request_latency",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "endpoint", "method"},
)

func InitMetrics() {
	prometheus.MustRegister(RequestCount, ErrorCount, RequestLatency)
}
