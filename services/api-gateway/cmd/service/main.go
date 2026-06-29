package main

import (
	"net/http"

	"log"

	"flowpay/api-gateway/internal/config"
	"flowpay/api-gateway/internal/constants"
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
	reconciliationProxy := proxy.New(cfg.ReconciliationServiceURL, constants.ServiceName)

	withJWT := func(h http.Handler) http.Handler {
		return middleware.JWTAuth(cfg.JWTSecret, constants.ServiceName, h)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	// auth service - no JWT required
	mux.Handle("/auth", authProxy)
	mux.Handle("/auth/", authProxy)

	// account service - no JWT required
	mux.Handle("/accounts", accountProxy)
	mux.Handle("/accounts/", accountProxy)

	// account service - JWT required
	mux.Handle("/accounts/defaults/list", withJWT(accountProxy))
	mux.Handle("/accounts/list", withJWT(accountProxy))

	// payment-service — JWT required
	mux.Handle("/payments", withJWT(paymentProxy))
	mux.Handle("/payments/", withJWT(paymentProxy))

	// offer-service — JWT required
	mux.Handle("/offers", withJWT(offerProxy))
	mux.Handle("/offers/", withJWT(offerProxy))

	// reconciliation-service — internal, no JWT
	mux.Handle("/reconciliation/", reconciliationProxy)

	corsMiddleware := middleware.CORS(cfg.AllowedOrigins)

	log.Printf("API gateway running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, corsMiddleware(tracing.TracingMiddleware(constants.ServiceName, mux))))
}
