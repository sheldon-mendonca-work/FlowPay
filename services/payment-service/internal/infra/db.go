package infra

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"flowpay/pkg/utils"

	_ "github.com/lib/pq"
)

func InitDB() *sql.DB {
	dsn := utils.GetEnv("POSTGRES_DSN", "")
	if dsn == "" {
		host := utils.GetEnv("POSTGRES_HOST", "localhost")
		port := utils.GetEnv("POSTGRES_PORT", "5432")
		user := utils.GetEnv("POSTGRES_USER", "postgres")
		password := utils.GetEnv("POSTGRES_PASSWORD", "postgres")
		dbName := utils.GetEnv("POSTGRES_DB", "payment_db")
		sslMode := utils.GetEnv("POSTGRES_SSLMODE", "disable")

		dsn = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			host,
			port,
			user,
			password,
			dbName,
			sslMode,
		)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		log.Fatalf("failed to connect to Postgres: %v", err)
	}

	log.Println("Postgres client configured")
	return db
}
