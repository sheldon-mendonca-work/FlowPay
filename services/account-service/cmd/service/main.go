package main

import (
	"flowpay/account-service/internal/api"
	"flowpay/account-service/internal/config"
	"flowpay/account-service/internal/constants"
	"flowpay/account-service/internal/infra"
	"flowpay/account-service/internal/repository"
	"flowpay/account-service/internal/service"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	metrics.InitAccountMetrics()

	cfg := config.Load()
	db := infra.InitDB()
	defer db.Close()

	companyRepository := repository.NewCompanyRepository(db)
	userRepository := repository.NewUserRepository(db)
	accountsRepository := repository.NewAccountsRepository(db)

	accountService := service.NewAccountService(
		db,
		companyRepository,
		userRepository,
		accountsRepository,
	)

	httpHandler := api.NewHandler(accountService)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)
	mux.HandleFunc("/accounts", httpHandler.HandleCreateAccount)
	mux.HandleFunc("/companies", httpHandler.HandleCreateCompany)
	mux.HandleFunc("/users", httpHandler.HandleCreateUser)

	log.Printf("Account service running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, tracing.TracingMiddleware(constants.ServiceName, mux)))
}
