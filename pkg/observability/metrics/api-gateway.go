package metrics

import "github.com/prometheus/client_golang/prometheus"

var GatewayRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gateway_requests_total",
		Help: "Total gateway requests by service and outcome.",
	},
	[]string{"service", "outcome"},
)

var GatewayRequestDurationMs = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "gateway_request_duration_ms",
		Buckets: []float64{5, 10, 25, 50, 100, 500, 1000},
	},
	[]string{"service", "outcome"},
)

var GatewayRequestErrorsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gateway_request_errors_total",
		Help: "Total gateway errors by service and route.",
	},
	[]string{"service", "route"},
)

var GatewayUnauthorizedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gateway_unauthorized_requests_total",
		Help: "Total unauthorized gateway requests by service and reason.",
	},
	[]string{"service", "reason"},
)

func InitGatewayMetrics() {
	prometheus.MustRegister(
		GatewayRequestsTotal,
		GatewayRequestDurationMs,
		GatewayRequestErrorsTotal,
		GatewayUnauthorizedTotal,
	)
}
