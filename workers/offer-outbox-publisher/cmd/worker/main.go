package main

import (
	"context"
	"flowpay/offer-outbox-publisher/internal/db"
	"flowpay/offer-outbox-publisher/internal/kafka"
	"flowpay/offer-outbox-publisher/internal/repo"
	"flowpay/offer-outbox-publisher/internal/worker"
	"flowpay/pkg/utils"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db := db.InitDB()
	defer db.Close()

	outboxRepository := repo.NewOfferOutboxEventRepository(db)
	offerReservationentIdempotencyRepository := repo.OfferReservationentIdempotencyRepository(db)
	offerRedemptionentIdempotencyRepository := repo.OfferRedemptionentIdempotencyRepository(db)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	offerReservedKafkaTopic := utils.GetEnv("OFFER_RESERVED_KAFKA_TOPIC", "offer.reserved")
	offerRejectedKafkaTopic := utils.GetEnv("OFFER_REJECTED_KAFKA_TOPIC", "offer.rejected")

	offerReservedKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, offerReservedKafkaTopic)
	offerRejectedKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, offerRejectedKafkaTopic)

	outboxWorker := worker.NewOutboxWorker(db, *outboxRepository, *offerReservationentIdempotencyRepository, *offerRedemptionentIdempotencyRepository, *offerReservedKafkaProducer, *offerRejectedKafkaProducer)

	log.Printf("transaction processor worker starting broker=%s topics=[%s , %s]", kafkaBroker, offerReservedKafkaTopic, offerRejectedKafkaTopic)
	outboxWorker.Start(ctx)
}
