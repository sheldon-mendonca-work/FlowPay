package config

import (
	"flowpay/pkg/utils"
)

type Config struct {
	PaymentServiceURL        string
	AuthServiceURL           string
	AccountServiceURL        string
	OfferServiceURL          string
	ReconciliationServiceURL string
	NotificationServiceURL   string
	DeploymentControllerURL  string
	JWTSecret                string
	Port                     string
	AllowedOrigins           []string
}

func Load() Config {
	return Config{
		PaymentServiceURL:        utils.GetEnv("PAYMENT_SERVICE_URL", "http://localhost:8001"),
		AuthServiceURL:           utils.GetEnv("AUTH_SERVICE_URL", "http://localhost:8007"),
		AccountServiceURL:        utils.GetEnv("ACCOUNT_SERVICE_URL", "http://localhost:8005"),
		OfferServiceURL:          utils.GetEnv("OFFER_SERVICE_URL", "http://localhost:8010"),
		ReconciliationServiceURL: utils.GetEnv("RECONCILIATION_SERVICE_URL", "http://localhost:8013"),
		NotificationServiceURL:   utils.GetEnv("NOTIFICATION_SERVICE_URL", "http://localhost:8008"),
		DeploymentControllerURL:  utils.GetEnv("DEPLOYMENT_CONTROLLER_URL", "http://localhost:8016"),
		JWTSecret:                utils.GetEnv("JWT_SECRET", ""),
		Port:                     utils.GetEnv("PORT", "8000"),
		AllowedOrigins:           utils.GetEnvSlice("ALLOWED_ORIGINS", []string{"http://localhost:5173"}),
	}
}
