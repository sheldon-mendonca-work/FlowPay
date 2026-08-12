package main

import (
	"context"
	"flowpay/notification-service/internal/api"
	"flowpay/notification-service/internal/constants"
	"flowpay/notification-service/internal/infra"
	"flowpay/notification-service/internal/kafka"
	"flowpay/notification-service/internal/repository"
	"flowpay/notification-service/internal/service"
	"flowpay/pkg/healthcheck"
	"flowpay/pkg/httpx"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"flowpay/pkg/utils"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("notification service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	healthcheck.RunIfRequested("http://localhost:8008/notification/health")

	metrics.InitMetrics()
	metrics.InitNotificationMetrics()

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

	// Each topic gets its own consumer group ID. segmentio/kafka-go's client-side
	// group assignment does not reliably split partitions across readers that share
	// one group ID but subscribe to different topics - members join the group fine
	// but end up with zero partitions assigned, so nothing is ever consumed.
	notificationTimelineKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), notificationTimelineKafkaTopic, kafkaGroupID+"-timeline", handler.HandleNotificationTimeline)
	defer notificationTimelineKafkaConsumer.Close()

	notificationUserKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), notificationUserKafkaTopic, kafkaGroupID+"-user", handler.HandleNotificationUser)
	defer notificationUserKafkaConsumer.Close()

	reconciliationEventsKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), reconciliationEventsKafkaTopic, kafkaGroupID+"-reconciliation", handler.HandleReconciliationEvents)
	defer reconciliationEventsKafkaConsumer.Close()

	consumerCtx, stopConsumers := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopConsumers()

	for _, consumer := range []*kafka.KafkaConsumer{
		notificationTimelineKafkaConsumer,
		notificationUserKafkaConsumer,
		reconciliationEventsKafkaConsumer,
	} {
		go func(c *kafka.KafkaConsumer) {
			if err := c.Start(consumerCtx); err != nil {
				log.Printf("notification-service kafka consumer stopped: %v", err)
			}
		}(consumer)
	}

	httpHandler := api.NewHandler(notificationService)

	router := httpx.NewRouter()
	router.HandleFunc("/notification/health", getHealthCheck)
	router.HandleFunc("/notification/metrics", handleMetrics)
	router.HandleFunc("/notification/timeline/payment/{paymentID}", httpHandler.HandleGetTimelineByPaymentID)
	router.HandleFunc("/notification/timeline/{trace_id}", httpHandler.HandleNotificationTimelineStream)
	router.HandleFunc("/notification/fp/metrics", httpHandler.HandleFlowpayMetricsStream)
	log.Println("Notification service running on :8008")
	log.Fatal(http.ListenAndServe(":8008", tracing.TracingMiddleware(constants.ServiceName, router)))
}
