package metrics

import "github.com/prometheus/client_golang/prometheus"

var OfferExpiryWorkerDurationMs = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "offer_expiry_worker_duration_ms",
		Help:    "Duration of offer expiry worker batch processing in milliseconds.",
		Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 5000},
	},
)

var OfferExpiryWorkerBatchSize = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "offer_expiry_worker_batch_size",
		Help:    "Number of reservations processed per batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
	},
)

var OfferExpiryWorkerExpiredTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "offer_expiry_worker_reservations_expired_total",
		Help: "Total number of reservations expired by worker.",
	},
)

var OfferExpiryWorkerFailuresTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "offer_expiry_worker_failures_total",
		Help: "Total number of worker batch failures.",
	},
)

func InitOfferExpiryWorkerMetrics() {
	prometheus.MustRegister(
		OfferExpiryWorkerDurationMs,
		OfferExpiryWorkerBatchSize,
		OfferExpiryWorkerExpiredTotal,
		OfferExpiryWorkerFailuresTotal,
	)
}
