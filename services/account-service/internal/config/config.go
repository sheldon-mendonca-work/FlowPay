package config

import "flowpay/pkg/utils"

type Config struct {
	Port string
}

func Load() Config {
	return Config{
		Port: utils.GetEnv("PORT", "8002"),
	}
}
