package main

import (
	"net/http"

	"log"

	"flowpay/api-gateway/internal/config"
	"flowpay/api-gateway/internal/constants"
	"flowpay/api-gateway/internal/heartbeat"
	"flowpay/api-gateway/internal/middleware"
	"flowpay/api-gateway/internal/proxy"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.Load()
	metrics.InitGatewayMetrics()

	paymentProxy := proxy.New(cfg.PaymentServiceURL, constants.ServiceName)
	authProxy := proxy.New(cfg.AuthServiceURL, constants.ServiceName)
	accountProxy := proxy.New(cfg.AccountServiceURL, constants.ServiceName)
	offerProxy := proxy.New(cfg.OfferServiceURL, constants.ServiceName)
	notificationProxy := proxy.New(cfg.NotificationServiceURL, constants.ServiceName)
	reconciliationProxy := proxy.New(cfg.ReconciliationServiceURL, constants.ServiceName)

	withJWT := func(h http.Handler) http.Handler {
		return middleware.JWTAuth(cfg.JWTSecret, constants.ServiceName, h)
	}
	withJWTSSE := func(h http.Handler) http.Handler {
		return middleware.JWTAuthSSE(cfg.JWTSecret, constants.ServiceName, h)
	}

	// Notifies the deployment controller that the app is in use, throttled
	// to at most one outbound heartbeat per minute. Only wraps routes that
	// proxy real traffic to backend services — /health and /metrics are
	// synthetic/infra traffic and shouldn't keep the instance alive.
	heartbeatNotifier := heartbeat.New(cfg.DeploymentControllerURL + "/deployment/heartbeat")
	withHeartbeat := heartbeatNotifier.Middleware

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("api gateway ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	// auth service - no JWT required
	mux.Handle("/auth", withHeartbeat(authProxy))
	mux.Handle("/auth/", withHeartbeat(authProxy))

	// account service - no JWT required
	mux.Handle("/accounts", withHeartbeat(accountProxy))
	mux.Handle("/accounts/", withHeartbeat(accountProxy))

	// account service - JWT required
	mux.Handle("/accounts/defaults/list", withHeartbeat(withJWT(accountProxy)))
	mux.Handle("/accounts/list", withHeartbeat(withJWT(accountProxy)))
	mux.Handle("/accounts/userinfo", withHeartbeat(withJWT(accountProxy)))
	mux.Handle("/accounts/balance/", withHeartbeat(withJWT(accountProxy)))
	mux.Handle("/accounts/transactions/", withHeartbeat(withJWT(accountProxy)))

	// payment-service — JWT required
	mux.Handle("/payments", withHeartbeat(withJWT(paymentProxy)))
	mux.Handle("/payments/", withHeartbeat(withJWT(paymentProxy)))

	// offer-service — JWT required
	mux.Handle("/offers", withHeartbeat(withJWT(offerProxy)))
	mux.Handle("/offers/", withHeartbeat(withJWT(offerProxy)))

	// notification-service SSE endpoints — JWT via header or ?token= query
	// param, since EventSource can't set an Authorization header. More
	// specific than /notification/ above, so they win for these subpaths.
	mux.Handle("/notification/timeline/", withHeartbeat(withJWTSSE(notificationProxy)))
	mux.Handle("/notification/fp/metrics", withHeartbeat(withJWTSSE(notificationProxy)))

	// notification-service — JWT required
	mux.Handle("/notification", withHeartbeat(withJWT(notificationProxy)))
	mux.Handle("/notification/", withHeartbeat(withJWT(notificationProxy)))

	// reconciliation-service — internal, no JWT
	mux.Handle("/reconciliation/", withHeartbeat(reconciliationProxy))

	corsMiddleware := middleware.CORS(cfg.AllowedOrigins)

	log.Printf("API gateway running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, corsMiddleware(tracing.TracingMiddleware(constants.ServiceName, mux))))
}
