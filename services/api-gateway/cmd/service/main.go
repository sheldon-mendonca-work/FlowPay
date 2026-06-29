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
	offerProxy := proxy.New(cfg.OfferServiceURL, constants.ServiceName)
	reconciliationProxy := proxy.New(cfg.ReconciliationServiceURL, constants.ServiceName)
	authProxy := proxy.New(cfg.PaymentServiceURL, constants.ServiceName)

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

	// payment-service — JWT required
	mux.Handle("/payments", withJWT(paymentProxy))
	mux.Handle("/payments/", withJWT(paymentProxy))

	// offer-service — JWT required
	mux.Handle("/offers", withJWT(offerProxy))
	mux.Handle("/offers/", withJWT(offerProxy))

	// reconciliation-service — internal, no JWT
	mux.Handle("/reconciliation/", reconciliationProxy)

	log.Printf("API gateway running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, tracing.TracingMiddleware(constants.ServiceName, mux)))
}
