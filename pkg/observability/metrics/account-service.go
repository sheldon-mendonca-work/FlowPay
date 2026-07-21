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

var DefaultAccountsGetRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "default_accounts_get_requests_total",
		Help: "Total number of default accounts list requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var DefaultAccountsGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "default_accounts_get_request_duration_seconds",
		Help:    "Default accounts list request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var DefaultUsersGetRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "default_users_get_requests_total",
		Help: "Total number of default users list requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var DefaultUsersGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "default_users_get_request_duration_seconds",
		Help:    "Default users list request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var DefaultCompaniesGetRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "default_companies_get_requests_total",
		Help: "Total number of default companies list requests by outcome.",
	},
	[]string{"service", "outcome"},
)

var DefaultCompaniesGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "default_companies_get_request_duration_seconds",
		Help:    "Default companies list request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var UserInfoGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "user_info_get_request_duration_seconds",
		Help:    "User info request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var DefaultListGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "default_list_get_request_duration_seconds",
		Help:    "Default list (users/company toggle) request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var AccountsListRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "accounts_list_request_duration_seconds",
		Help:    "Accounts list request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var BalanceGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "balance_get_request_duration_seconds",
		Help:    "Account balance request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var UserGetRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "user_get_request_duration_seconds",
		Help:    "User lookup by id request processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

var TransactionsListRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "transactions_list_request_duration_seconds",
		Help:    "Account transactions list request processing latency in seconds.",
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
		DefaultAccountsGetRequestsTotal,
		DefaultAccountsGetRequestDuration,
		DefaultUsersGetRequestsTotal,
		DefaultUsersGetRequestDuration,
		DefaultCompaniesGetRequestsTotal,
		DefaultCompaniesGetRequestDuration,
		DefaultListGetRequestDuration,
		AccountsListRequestDuration,
		UserInfoGetRequestDuration,
		BalanceGetRequestDuration,
		UserGetRequestDuration,
		TransactionsListRequestDuration,
	)
}
