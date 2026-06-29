package config

import (
	"flowpay/pkg/utils"
)

type Config struct {
	PaymentServiceURL        string
	AuthServiceURL           string
	OfferServiceURL          string
	ReconciliationServiceURL string
	JWTSecret                string
	Port                     string
}

func Load() Config {
	return Config{
		PaymentServiceURL:        utils.GetEnv("PAYMENT_SERVICE_URL", "http://localhost:8001"),
		AuthServiceURL:           utils.GetEnv("AUTH_SERVICE_URL", "http://localhost:8001"),
		OfferServiceURL:          utils.GetEnv("OFFER_SERVICE_URL", "http://localhost:8005"),
		ReconciliationServiceURL: utils.GetEnv("RECONCILIATION_SERVICE_URL", "http://localhost:8004"),
		JWTSecret:                utils.GetEnv("JWT_SECRET", ""),
		Port:                     utils.GetEnv("PORT", "8006"),
	}
}
