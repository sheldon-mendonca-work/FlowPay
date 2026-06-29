package metrics

import "github.com/prometheus/client_golang/prometheus"

var AccountCreateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "account_create_requests_total",
		Help: "Total number of account creation requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var AccountCreateRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "account_create_request_duration_seconds",
		Help:    "Account creation request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var CompanyCreateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "company_create_requests_total",
		Help: "Total number of company creation requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var CompanyCreateRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "company_create_request_duration_seconds",
		Help:    "Company creation request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var UserCreateRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "user_create_requests_total",
		Help: "Total number of user creation requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var UserCreateRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "user_create_request_duration_seconds",
		Help:    "User creation request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitAccountMetrics() {
	prometheus.MustRegister(
		AccountCreateRequestsTotal,
		AccountCreateRequestDuration,
		CompanyCreateRequestsTotal,
		CompanyCreateRequestDuration,
		UserCreateRequestsTotal,
		UserCreateRequestDuration,
	)
}
