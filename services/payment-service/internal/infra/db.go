package infra

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"flowpay/pkg/utils"

	_ "github.com/lib/pq"
)

func InitDB() *sql.DB {
	dsn := utils.GetEnv("POSTGRES_DSN", "")
	host := ""
	port := ""
	dbName := ""
	maxOpenConnections, err := strconv.Atoi(utils.GetEnv("POSTGRES_MAX_OPEN_CONNECTIONS", "10"))
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}
	maxIdleConnections, err := strconv.Atoi(utils.GetEnv("POSTGRES_MAX_IDLE_CONNECTIONS", "5"))
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}
	maxConnectionLifetime, err := strconv.Atoi(utils.GetEnv("POSTGRES_MAX_CONNECTION_LIFETIME", "30"))
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}

	if dsn == "" {
		host = utils.GetEnv("POSTGRES_HOST", "localhost")
		port = utils.GetEnv("POSTGRES_PORT", "5432")
		user := utils.GetEnv("POSTGRES_USER", "postgres")
		password := utils.GetEnv("POSTGRES_PASSWORD", "postgres")
		dbName = utils.GetEnv("POSTGRES_DB", "payment_db")
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

	if host != "" || port != "" || dbName != "" {
		log.Printf("connecting to Postgres host=%s port=%s db=%s", host, port, dbName)
	} else {
		log.Println("connecting to Postgres using POSTGRES_DSN")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to open Postgres connection: %v", err)
	}

	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)
	db.SetConnMaxLifetime(time.Duration(maxConnectionLifetime) * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 16500*time.Millisecond)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		log.Fatalf("failed to connect to Postgres: %v", err)
	}

	log.Println("Postgres client configured")
	return db
}
