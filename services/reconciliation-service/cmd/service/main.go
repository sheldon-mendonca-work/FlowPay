package main

import (
	"log"
	"net/http"

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
	w.Write([]byte("ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	metrics.InitReconciliationMetrics()
	db := infra.InitDB()
	defer db.Close()

	paymentRepository := repository.NewPaymentRepository(db)
	outboxRepository := repository.NewOutboxEventsRepository(db)
	idempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	accountRepository := repository.NewAccountRepository(db)

	reconciliationService := service.NewReconciliationService(
		db,
		paymentRepository,
		outboxRepository,
		idempotencyRepository,
		transactionRepository,
		accountRepository,
	)
	handler := api.NewHandler(reconciliationService)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)

	mux.HandleFunc("/reconciliation/all", handler.HandleAllReconciliationChecks)

	mux.HandleFunc("/reconciliation/payments", handler.HandlePaymentChecks)
	mux.HandleFunc("/reconciliation/payments/{payment_check_type}", handler.HandleIndividualPaymentCheck)

	mux.HandleFunc("/reconciliation/idempotency", handler.HandleIdempotencyChecks)
	mux.HandleFunc("/reconciliation/idempotency/{idempotency_check_type}", handler.HandleIndividualIdempotencyCheck)

	mux.HandleFunc("/reconciliation/outbox", handler.HandleOutboxChecks)
	mux.HandleFunc("/reconciliation/outbox/{outbox_check_type}", handler.HandleIndividualOutboxCheck)

	mux.HandleFunc("/reconciliation/transactions", handler.HandleTransactionChecks)
	mux.HandleFunc("/reconciliation/transactions/{transaction_check_type}", handler.HandleIndividualTransactionCheck)

	log.Println("Reconciliation service running on :8004")
	log.Fatal(http.ListenAndServe(":8004", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
