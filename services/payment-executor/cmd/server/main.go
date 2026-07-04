package main

import (
	"context"
	"flowpay/payment-executor/internal/handler"
	"flowpay/payment-executor/internal/infra"
	"flowpay/payment-executor/internal/kafka"
	"flowpay/payment-executor/internal/repository"
	"flowpay/payment-executor/internal/service"
	"flowpay/pkg/notifications"
	"flowpay/pkg/utils"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	db := infra.InitDB()
	defer db.Close()

	paymentRepository := repository.NewPaymentRepository(db)
	accountRepository := repository.NewAccountRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	idempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	outboxEventsRepository := repository.NewOutboxEventsRepository(db)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	notificationTimelineKafkaTopic := utils.GetEnv("NOTIFICATION_TIMELINE_KAFKA_TOPIC", "notification.timeline")
	timelinePublisher := notifications.NewTimelinePublisher(strings.Split(kafkaBroker, ","), notificationTimelineKafkaTopic)
	defer timelinePublisher.Close()

	paymentExecutorService := service.NewPaymentExecutorService(
		db,
		accountRepository,
		paymentRepository,
		transactionRepository,
		idempotencyRepository,
		outboxEventsRepository,
		timelinePublisher,
	)

	paymentHandler := handler.NewPaymentHandler(paymentExecutorService)

	groupID := utils.GetEnv("KAFKA_GROUP_ID", "payment-executor-group")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Each topic gets its own consumer group ID. segmentio/kafka-go's client-side
	// group assignment does not reliably split partitions across readers that share
	// one group ID but subscribe to different topics - members join the group fine
	// but end up with zero partitions assigned, so nothing is ever consumed.
	paymentInitiatedConsumer := kafka.NewKafkaConsumer(
		strings.Split(kafkaBroker, ","),
		utils.GetEnv("PAYMENT_INITIATED_KAFKA_TOPIC", "payment.initiated"),
		groupID+"-payment-initiated",
		paymentHandler.HandlePaymentInitiated,
	)
	defer paymentInitiatedConsumer.Close()

	offerReservedConsumer := kafka.NewKafkaConsumer(
		strings.Split(kafkaBroker, ","),
		utils.GetEnv("OFFER_RESERVED_KAFKA_TOPIC", "offer.reserved"),
		groupID+"-offer-reserved",
		paymentHandler.HandleOfferReserved,
	)
	defer offerReservedConsumer.Close()

	offerRejectedConsumer := kafka.NewKafkaConsumer(
		strings.Split(kafkaBroker, ","),
		utils.GetEnv("OFFER_REJECTED_KAFKA_TOPIC", "offer.rejected"),
		groupID+"-offer-rejected",
		paymentHandler.HandleOfferRejected,
	)
	defer offerRejectedConsumer.Close()

	errCh := make(chan error, 3)

	go func() {
		errCh <- paymentInitiatedConsumer.Start(ctx)
	}()

	go func() {
		errCh <- offerReservedConsumer.Start(ctx)
	}()

	go func() {
		errCh <- offerRejectedConsumer.Start(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("consumer failed: %v", err)
		}
		stop()

	case <-ctx.Done():
	}

	log.Println("payment executor shutting down")
}
