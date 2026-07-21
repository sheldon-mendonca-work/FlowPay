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
	w.Write([]byte("account service ok"))
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
	defaultCredentialsRepository := repository.NewDefaultCredentialsRepository(db)
	transactionsRepository := repository.NewTransactionsRepository(db)

	accountService := service.NewAccountService(
		db,
		companyRepository,
		userRepository,
		accountsRepository,
		defaultCredentialsRepository,
		transactionsRepository,
	)

	httpHandler := api.NewHandler(accountService)

	mux := http.NewServeMux()
	mux.HandleFunc("/accounts/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)
	mux.HandleFunc("/accounts", httpHandler.HandleCreateAccount)
	mux.HandleFunc("/accounts/companies", httpHandler.HandleCreateCompany)
	mux.HandleFunc("/accounts/users", httpHandler.HandleCreateUser)
	mux.HandleFunc("/accounts/defaults/accounts", httpHandler.HandleGetDefaultAccounts)
	mux.HandleFunc("/accounts/defaults/users", httpHandler.HandleGetDefaultUsers)
	mux.HandleFunc("/accounts/defaults/companies", httpHandler.HandleGetDefaultCompanies)
	mux.HandleFunc("/accounts/defaults/list", httpHandler.HandleGetDefaultList)
	mux.HandleFunc("/accounts/list", httpHandler.HandleListUserAccounts)
	mux.HandleFunc("/accounts/userinfo", httpHandler.HandleGetUserInfo)
	mux.HandleFunc("/accounts/balance/{id}", httpHandler.HandleGetBalance)
	mux.HandleFunc("/accounts/user/{id}", httpHandler.HandleGetUser)
	mux.HandleFunc("/accounts/transactions/payment/{paymentID}", httpHandler.HandleGetTransactionsByPaymentID)
	mux.HandleFunc("/accounts/transactions/{accountID}", httpHandler.HandleGetTransactions)

	log.Printf("Account service running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, tracing.TracingMiddleware(constants.ServiceName, mux)))
}
