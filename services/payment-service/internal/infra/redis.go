package infra

import (
	"context"
	"flowpay/pkg/utils"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func InitRedis() *redis.Client {
	addr := utils.GetEnv("REDIS_ADDR", "localhost:6379")
	password := os.Getenv("REDIS_PASSWORD")
	db := 0

	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("failed to connect to Redis at %s: %v", addr, err)
	}

	log.Printf("Redis client configured for addr=%s", addr)
	return redisClient
}
