package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ReconciliationRunsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reconciliation_runs_total",
		},
	)

	ReconciliationFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reconciliation_failures_total",
		},
	)

	ReconciliationAnomaliesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "reconciliation_anomalies_total",
		},
	)

	ReconciliationDurationMs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "reconciliation_duration_ms",
			Buckets: []float64{5, 10, 25, 50, 100, 500, 1000},
		},
	)

	ReconciliationAnomaliesByCheck = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reconciliation_anomalies_by_check",
		},
		[]string{"check"},
	)

	ReconciliationAnomaliesBySeverity = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reconciliation_anomalies_by_severity",
		},
		[]string{"severity"},
	)
)

func InitReconciliationMetrics() {
	prometheus.MustRegister(ReconciliationRunsTotal,
		ReconciliationFailuresTotal,
		ReconciliationAnomaliesTotal,
		ReconciliationDurationMs,
		ReconciliationAnomaliesByCheck,
		ReconciliationAnomaliesBySeverity)
}
