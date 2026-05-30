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
	metrics.InitMetrics()
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
	mux.HandleFunc("/reconciliation/payments", handler.HandlePaymentChecks)

	log.Println("Reconciliation service running on :8004")
	log.Fatal(http.ListenAndServe(":8004", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
