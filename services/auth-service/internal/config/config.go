package config

import "flowpay/pkg/utils"

type Config struct {
	AccountServiceURL string
	JWTSecret         string
	Port              string
}

func Load() Config {
	return Config{
		AccountServiceURL: utils.GetEnv("ACCOUNT_SERVICE_URL", "http://localhost:8002"),
		JWTSecret:         utils.GetEnv("JWT_SECRET", ""),
		Port:              utils.GetEnv("PORT", "8007"),
	}
}
