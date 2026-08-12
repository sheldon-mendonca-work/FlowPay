package main

import (
	"log"
	"net/http"

	"flowpay/pkg/healthcheck"
	"flowpay/pkg/httpx"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"flowpay/reconciliation-service/internal/api"
	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/infra"
	"flowpay/reconciliation-service/internal/repository"
	"flowpay/reconciliation-service/internal/service"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("reconciliation service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	healthcheck.RunIfRequested("http://localhost:8013/health")

	metrics.InitMetrics()
	metrics.InitReconciliationMetrics()
	db := infra.InitDB()
	defer db.Close()

	paymentRepository := repository.NewPaymentRepository(db)
	outboxRepository := repository.NewOutboxEventsRepository(db)
	idempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	accountRepository := repository.NewAccountRepository(db)
	offerRepository := repository.NewOfferRepository(db)

	reconciliationService := service.NewReconciliationService(
		db,
		paymentRepository,
		outboxRepository,
		idempotencyRepository,
		transactionRepository,
		accountRepository,
		offerRepository,
	)
	handler := api.NewHandler(reconciliationService)

	router := httpx.NewRouter()
	router.HandleFunc("/health", getHealthCheck)
	router.HandleFunc("/metrics", handleMetrics)

	router.HandleFunc("/reconciliation/all", handler.HandleAllReconciliationChecks)

	router.HandleFunc("/reconciliation/payments", handler.HandlePaymentChecks)
	router.HandleFunc("/reconciliation/payments/{payment_check_type}", handler.HandleIndividualPaymentCheck)

	router.HandleFunc("/reconciliation/idempotency", handler.HandleIdempotencyChecks)
	router.HandleFunc("/reconciliation/idempotency/{idempotency_check_type}", handler.HandleIndividualIdempotencyCheck)

	router.HandleFunc("/reconciliation/outbox", handler.HandleOutboxChecks)
	router.HandleFunc("/reconciliation/outbox/{outbox_check_type}", handler.HandleIndividualOutboxCheck)

	router.HandleFunc("/reconciliation/transactions", handler.HandleTransactionChecks)
	router.HandleFunc("/reconciliation/transactions/{transaction_check_type}", handler.HandleIndividualTransactionCheck)

	router.HandleFunc("/reconciliation/offers", handler.HandleOfferChecks)
	router.HandleFunc("/reconciliation/offers/{offer_check_type}", handler.HandleIndividualOfferCheck)

	log.Println("Reconciliation service running on :8013")
	log.Fatal(http.ListenAndServe(":8013", tracing.TracingMiddleware(constants.ServiceName, router)))
}
