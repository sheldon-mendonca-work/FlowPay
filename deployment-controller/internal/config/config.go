package config

import "flowpay/deployment-controller/internal/utils"

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
		AllowedOrigins: utils.GetEnvSlice("ALLOWED_ORIGINS", []string{"http://localhost:5173", "https://flowpay-ui.netlify.app/"}),
	}
}
