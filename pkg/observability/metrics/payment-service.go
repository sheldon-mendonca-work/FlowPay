package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
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

var PaymentRedisCreateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "payment_create_redis_requests_total",
		Help: "Total number of payment requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var PaymentDBCreateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "payment_create_db_requests_total",
		Help: "Total number of payment requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var PaymentRedisRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "payment_create_redis_request_duration_seconds",
		Help:    "Payment request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var PaymentDBRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "payment_create_db_request_duration_seconds",
		Help:    "Payment request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitPaymentMetrics() {
	prometheus.MustRegister(PaymentRequestsTotal, PaymentRequestDuration,
		PaymentRedisCreateRequestsTotal,
		PaymentDBCreateRequestsTotal,
		PaymentRedisRequestDuration,
		PaymentDBRequestDuration)
}
