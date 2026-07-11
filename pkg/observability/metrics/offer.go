package metrics

import "github.com/prometheus/client_golang/prometheus"

var OfferDurationMs = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "offer_duration_ms",
		Buckets: []float64{5, 10, 25, 50, 100, 500, 1000},
	},
)

var OfferCreationRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "offer_creation_requests_total",
		Help: "Total number of offer creation requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var OfferCreationRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "offer_creation_request_duration_seconds",
		Help:    "Offer creation request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var OfferRedemptionRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "offer_redemption_requests_total",
		Help: "Total number of offer redemption requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var OfferRedemptionRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "offer_redemption_request_duration_seconds",
		Help:    "Offer redemption request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var OfferReserveRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "offer_reserve_requests_total",
		Help: "Total number of offer reserve requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var OfferReserveRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "offer_reserve_request_duration_seconds",
		Help:    "Offer reserve request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var OfferStatusUpdateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "offer_status_update_requests_total",
		Help: "Total number of offer status update requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var OfferStatusUpdateRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "offer_status_update_request_duration_seconds",
		Help:    "Offer status update request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitOfferMetrics() {
	prometheus.MustRegister(OfferDurationMs, OfferCreationRequestsTotal, OfferCreationRequestDuration, OfferRedemptionRequestDuration, OfferRedemptionRequestsTotal, OfferReserveRequestsTotal, OfferReserveRequestDuration, OfferStatusUpdateRequestsTotal, OfferStatusUpdateRequestDuration)
}
