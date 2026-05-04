package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var RequestReceived = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "request_received",
		Help: "Total Number of HTTP requests received",
	},
	[]string{"service", "endpoint", "method", "status"},
)
var SuccessCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "success_count",

		Help: "Total number of successful HTTP requests.",
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

var PaymentRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "payment_requests_total",
		Help: "Total number of payment requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var PaymentRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "payment_request_duration_seconds",
		Help:    "Payment request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitMetrics() {
	prometheus.MustRegister(RequestReceived, SuccessCount, ErrorCount, RequestLatency, PaymentRequestsTotal, PaymentRequestDuration)
}
