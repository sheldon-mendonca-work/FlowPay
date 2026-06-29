package metrics

import "github.com/prometheus/client_golang/prometheus"

var AuthDurationMs = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "auth_duration_ms",
		Buckets: []float64{5, 10, 25, 50, 100, 500, 1000},
	},
)

var UserCreationRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_creation_requests_total",
		Help: "Total number of offer creation requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var UserCreationRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_creation_request_duration_seconds",
		Help:    "Offer creation request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitAuthMetrics() {
	prometheus.MustRegister(AuthDurationMs, UserCreationRequestsTotal, UserCreationRequestDuration, OfferRedemptionRequestDuration)
}
