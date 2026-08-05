package infra

import (
	"flowpay/pkg/utils"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func InitRedis() *redis.Client {
	host := utils.GetEnv("REDIS_HOST", "localhost")
	port := utils.GetEnv("REDIS_PORT", "6379")
	password := utils.GetEnv("REDIS_PASSWORD", "")
	dbType, _ := strconv.Atoi(utils.GetEnv("REDIS_DB", "0"))

	address := host + ":" + port

	return redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       dbType,
	})
}
