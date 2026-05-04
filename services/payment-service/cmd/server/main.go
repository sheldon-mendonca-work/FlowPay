package main

import (
	"log"
	"net/http"

	"flowpay/payment-service/internal/api"
	"flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/infra"
	"flowpay/payment-service/internal/repository"
	"flowpay/payment-service/internal/service"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"

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
	// kafkaWriter := infra.InitKafka()
	db := infra.InitDB()
	paymentRepository := repository.NewPaymentRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	paymentIdempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	accountRepository := repository.NewAccountRepository(db)

	paymentService := service.NewPaymentService(db, paymentRepository, transactionRepository, paymentIdempotencyRepository, accountRepository)
	handler := api.NewHandler(paymentService)

	// defer kafkaWriter.Close()
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", getHealthCheck)
	mux.HandleFunc("/payments", handler.HandlePayment)
	mux.HandleFunc("/metrics", handleMetrics)
	log.Println("Payment service running on :8001")
	log.Fatal(http.ListenAndServe(":8001", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
