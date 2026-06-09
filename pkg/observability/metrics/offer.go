package metrics

import "github.com/prometheus/client_golang/prometheus"

var OfferDurationMs = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "offer_duration_ms",
		Buckets: []float64{5, 10, 25, 50, 100, 500, 1000},
	},
)

var OfferRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "offer_requests_total",
		Help: "Total number of offer requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var OfferRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "offer_request_duration_seconds",
		Help:    "Offer request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitOfferMetrics() {
	prometheus.MustRegister(OfferDurationMs, OfferRequestsTotal, OfferRequestDuration)
}
