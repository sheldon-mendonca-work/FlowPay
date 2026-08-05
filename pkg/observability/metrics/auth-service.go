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
		Help: "Total number of register requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var UserCreationRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_creation_request_duration_seconds",
		Help:    "Register request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AuthLoginRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_login_requests_total",
		Help: "Total number of login requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var AuthLoginRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_login_request_duration_seconds",
		Help:    "Login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AuthRefreshRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_refresh_requests_total",
		Help: "Total number of token refresh requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var AuthRefreshRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_refresh_request_duration_seconds",
		Help:    "Token refresh request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AuthLogoutRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_logout_requests_total",
		Help: "Total number of logout requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var AuthLogoutRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_logout_request_duration_seconds",
		Help:    "Logout request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AuthDefaultLoginRedisCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_default_login_redis",
		Help: "Total No of Default Auth Login Served From Redis",
	},
	[]string{"service", "outcome"},
)

var AuthDefaultLoginDatabaseCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_default_login_db",
		Help: "Total No of Default Auth Login Served From Database",
	},
	[]string{"service", "outcome"},
)

var AuthDefaultLoginRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_default_login_requests_total",
		Help: "Total number of default login requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var AuthDefaultLoginRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_default_login_request_duration_seconds",
		Help:    "Default login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AuthDefaultLoginAccountRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_default_login_account_requests_total",
		Help: "Total number of default account login requests by outcome and account_type.",
	},
	[]string{"service", "outcome", "account_type"},
)

var AuthDefaultLoginAccountRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_default_login_account_request_duration_seconds",
		Help:    "Default account login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome", "account_type"},
)

var AuthDefaultLoginUserRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "auth_default_login_user_requests_total",
		Help: "Total number of default user login requests by outcome and account_type.",
	},
	[]string{"service", "outcome", "account_type"},
)

var AuthDefaultLoginUserRequestRedisDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_default_login_user_redis_request_duration_seconds",
		Help:    "Default user login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome", "account_type"},
)

var AuthDefaultLoginUserRequestDBDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_default_login_user_db_request_duration_seconds",
		Help:    "Default user login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome", "account_type"},
)

var AuthDefaultLoginUserRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "auth_default_login_user_request_duration_seconds",
		Help:    "Default user login request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome", "account_type"},
)

func InitAuthMetrics() {
	prometheus.MustRegister(
		AuthDurationMs,
		UserCreationRequestsTotal,
		UserCreationRequestDuration,
		AuthLoginRequestsTotal,
		AuthLoginRequestDuration,
		AuthRefreshRequestsTotal,
		AuthRefreshRequestDuration,
		AuthLogoutRequestsTotal,
		AuthLogoutRequestDuration,
		OfferRedemptionRequestDuration,
		AuthDefaultLoginRequestsTotal,
		AuthDefaultLoginRequestDuration,
		AuthDefaultLoginAccountRequestsTotal,
		AuthDefaultLoginAccountRequestDuration,
		AuthDefaultLoginUserRequestsTotal,
		AuthDefaultLoginUserRequestRedisDuration,
		AuthDefaultLoginUserRequestDBDuration,
		AuthDefaultLoginRedisCount,
		AuthDefaultLoginDatabaseCount,
	)
}
