package main

import (
	"log"
	"net/http"
	"strings"

	"flowpay/payment-service/internal/api"
	paymentServiceConstants "flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/infra"
	"flowpay/payment-service/internal/repository"
	"flowpay/payment-service/internal/service"
	"flowpay/pkg/notifications"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"flowpay/pkg/utils"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("payment service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	metrics.InitMetrics()
	port := utils.GetEnv("PORT", "8001")
	if port == "" {
		log.Fatalf("Could not fetch env variables: " + port)
	}

	db := infra.InitDB()
	paymentRepository := repository.NewPaymentRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	paymentIdempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	accountRepository := repository.NewAccountRepository(db)
	outboxEventRepository := repository.NewOutboxEventRepository(db)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	notificationTimelineKafkaTopic := utils.GetEnv("NOTIFICATION_TIMELINE_KAFKA_TOPIC", "notification.timeline")
	timelinePublisher := notifications.NewTimelinePublisher(strings.Split(kafkaBroker, ","), notificationTimelineKafkaTopic)
	defer timelinePublisher.Close()

	paymentService := service.NewPaymentService(db, paymentRepository, transactionRepository, paymentIdempotencyRepository, accountRepository, outboxEventRepository, timelinePublisher)
	handler := api.NewHandler(paymentService)

	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/payments/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)
	mux.HandleFunc("/payments", handler.HandlePayment)
	mux.HandleFunc("/payments/{paymentID}", handler.HandlePaymentByID)
	log.Println("Payment service running on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, tracing.TracingMiddleware(paymentServiceConstants.ServiceName, mux)))
}
