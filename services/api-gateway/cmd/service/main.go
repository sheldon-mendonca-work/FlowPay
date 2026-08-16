package main

import (
	"net/http"

	"log"

	"flowpay/api-gateway/internal/config"
	"flowpay/api-gateway/internal/constants"
	"flowpay/api-gateway/internal/heartbeat"
	"flowpay/api-gateway/internal/middleware"
	"flowpay/api-gateway/internal/proxy"
	"flowpay/pkg/healthcheck"
	"flowpay/pkg/httpx"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	healthcheck.RunIfRequested("http://localhost:8000/health")

	cfg := config.Load()
	metrics.InitMetrics()
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

	log.Println("Deployment URL - " + cfg.DeploymentControllerURL)
	router := httpx.NewRouter()

	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("api gateway ok"))
	})
	router.Handle("/metrics", promhttp.Handler())

	// auth service - no JWT required
	router.Handle("/auth", withHeartbeat(authProxy, cfg.IsProduction))
	router.Handle("/auth/", withHeartbeat(authProxy, cfg.IsProduction))

	// account service - no JWT required
	router.Handle("/accounts", withHeartbeat(accountProxy, cfg.IsProduction))
	router.Handle("/accounts/", withHeartbeat(accountProxy, cfg.IsProduction))

	// account service - JWT required
	router.Handle("/accounts/defaults/list", withHeartbeat(withJWT(accountProxy), cfg.IsProduction))
	router.Handle("/accounts/list", withHeartbeat(withJWT(accountProxy), cfg.IsProduction))
	router.Handle("/accounts/userinfo", withHeartbeat(withJWT(accountProxy), cfg.IsProduction))
	router.Handle("/accounts/balance/", withHeartbeat(withJWT(accountProxy), cfg.IsProduction))
	router.Handle("/accounts/transactions/", withHeartbeat(withJWT(accountProxy), cfg.IsProduction))

	// payment-service — JWT required
	router.Handle("/payments", withHeartbeat(withJWT(paymentProxy), cfg.IsProduction))
	router.Handle("/payments/", withHeartbeat(withJWT(paymentProxy), cfg.IsProduction))

	// offer-service — JWT required
	router.Handle("/offers", withHeartbeat(withJWT(offerProxy), cfg.IsProduction))
	router.Handle("/offers/", withHeartbeat(withJWT(offerProxy), cfg.IsProduction))

	// notification-service SSE endpoints — JWT via header or ?token= query
	// param, since EventSource can't set an Authorization header. More
	// specific than /notification/ above, so they win for these subpaths.
	router.Handle("/notification/timeline/", withHeartbeat(withJWTSSE(notificationProxy), cfg.IsProduction))
	router.Handle("/notification/fp/metrics", withHeartbeat(withJWTSSE(notificationProxy), cfg.IsProduction))

	// notification-service — JWT required
	router.Handle("/notification", withHeartbeat(withJWT(notificationProxy), cfg.IsProduction))
	router.Handle("/notification/", withHeartbeat(withJWT(notificationProxy), cfg.IsProduction))

	// reconciliation-service — internal, no JWT
	router.Handle("/reconciliation/", withHeartbeat(reconciliationProxy, cfg.IsProduction))

	corsMiddleware := middleware.CORS(cfg.AllowedOrigins)

	log.Printf("API gateway running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, corsMiddleware(tracing.TracingMiddleware(constants.ServiceName, router))))
}
