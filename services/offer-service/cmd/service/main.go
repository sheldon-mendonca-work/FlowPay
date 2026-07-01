package main

import (
	"flowpay/offer-service/internal/api"
	"flowpay/offer-service/internal/constants"
	"flowpay/offer-service/internal/infra"
	"flowpay/offer-service/internal/kafka"
	"flowpay/offer-service/internal/repository"
	"flowpay/offer-service/internal/service"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"flowpay/pkg/utils"
	"log"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("offer service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	metrics.InitOfferMetrics()
	db := infra.InitDB()

	defer db.Close()

	offerRepository := repository.NewOfferRepository(db)
	companyRepository := repository.NewCompanyRepository(db)
	offerEventsRepository := repository.NewOfferEventsRepository(db)
	offerIdempotencyRepository := repository.NewOfferIdempotencyRepository(db)
	offerReservationIdempotencyRepository := repository.NewOfferReservationIdempotencyRepository(db)
	offerRedemptionIdempotencyRepository := repository.NewOfferRedemptionIdempotencyRepository(db)
	offerRedemptionRepository := repository.NewOfferRedemptionsRepository(db)
	offerReservationRepository := repository.NewOfferReservationsRepository(db)
	offerOutboxRepository := repository.NewOfferOutboxRepository(db)
	usersRepository := repository.NewUserRepository(db)
	accountsRepository := repository.NewAccountsRepository(db)

	offerService := service.NewOfferService(db,
		offerRepository,
		companyRepository,
		offerEventsRepository,
		offerIdempotencyRepository,
		offerRedemptionRepository,
		offerReservationRepository,
		usersRepository,
		offerReservationIdempotencyRepository,
		offerRedemptionIdempotencyRepository,
		accountsRepository,
		offerOutboxRepository,
	)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	offerInitiatedKafkaTopic := utils.GetEnv("OFFER_INITIATED_KAFKA_TOPIC", "offer.initiated")
	paymentSuccessKafkaTopic := utils.GetEnv("PAYMENT_SUCCESS_KAFKA_TOPIC", "payment.succeeded")
	kafkaGroupID := utils.GetEnv("KAFKA_GROUP_ID", "offer-service-group")

	handler := api.NewKafkaOfferHandler(offerService)

	offerInitiatedKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), offerInitiatedKafkaTopic, kafkaGroupID, handler.HandlePaymentInitiated)
	defer offerInitiatedKafkaConsumer.Close()

	paymentSuccessKafkaConsumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), paymentSuccessKafkaTopic, kafkaGroupID, handler.HandlePaymentSuccess)
	defer paymentSuccessKafkaConsumer.Close()

	httpHandler := api.NewHandler(offerService)

	mux := http.NewServeMux()
	mux.HandleFunc("/offers/health", getHealthCheck)
	mux.HandleFunc("/offers/metrics", handleMetrics)
	mux.HandleFunc("/offers", httpHandler.HandleOfferTransactions)
	mux.HandleFunc("/offers/list", httpHandler.HandleListOffers)
	mux.HandleFunc("/offers/{offer_id}/reserve", httpHandler.HandleOfferIdReserveTransactions)
	mux.HandleFunc("/offers/{offer_id}/redeem", httpHandler.HandleOfferIdRedeemTransactions)
	mux.HandleFunc("/offers/companies", httpHandler.HandleGetCompanyOffers)
	mux.HandleFunc("/offers/companies/summary", httpHandler.HandleGetCompanyOffersSummary)
	log.Println("Offer service running on :8010")
	log.Fatal(http.ListenAndServe(":8010", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
