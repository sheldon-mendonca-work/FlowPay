package main

import (
	"flowpay/notification-service/internal/api"
	"flowpay/notification-service/internal/constants"
	"flowpay/notification-service/internal/infra"
	"flowpay/notification-service/internal/kafka"
	"flowpay/notification-service/internal/repository"
	"flowpay/notification-service/internal/service"
	"flowpay/pkg/observability/tracing"
	"flowpay/pkg/utils"
	"log"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("notification service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	db := infra.InitDB()

	defer db.Close()

	notificationRepository := repository.NewNotificationRepository(db)

	notificationService := service.NewNotificationService(db,
		notificationRepository)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	notificationTimelineKafkaTopic := utils.GetEnv("NOTIFICATION_TIMELINE_KAFKA_TOPIC", "notification.timeline")
	notificationUserKafkaTopic := utils.GetEnv("NOTIFICATION_USER_KAFKA_TOPIC", "notification.user")
	reconciliationEventsKafkaTopic := utils.GetEnv("RECONCILIATION_EVENTS_KAFKA_TOPIC", "reconciliation.events")
	kafkaGroupID := utils.GetEnv("KAFKA_GROUP_ID", "notification-service-group")

	handler := api.NewKafkaNotificationHandler(notificationService)

	notificationTimelineKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), notificationTimelineKafkaTopic, kafkaGroupID, handler.HandleNotificationTimeline)
	defer notificationTimelineKafkaConsumer.Close()

	notificationUserKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), notificationUserKafkaTopic, kafkaGroupID, handler.HandleNotificationUser)
	defer notificationUserKafkaConsumer.Close()

	reconciliationEventsKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), reconciliationEventsKafkaTopic, kafkaGroupID, handler.HandleReconciliationEvents)
	defer reconciliationEventsKafkaConsumer.Close()

	httpHandler := api.NewHandler(notificationService)

	mux := http.NewServeMux()
	mux.HandleFunc("/notification/health", getHealthCheck)
	mux.HandleFunc("/notification/metrics", handleMetrics)
	mux.HandleFunc("/notification/timeline/{payment_id}", httpHandler.HandleNotificationTimelineStream)
	log.Println("Notification service running on :8010")
	log.Fatal(http.ListenAndServe(":8010", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
