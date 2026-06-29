package main

import (
	"flowpay/auth-service/internal/api"
	"flowpay/auth-service/internal/constants"
	"flowpay/auth-service/internal/infra"
	"flowpay/auth-service/internal/repository"
	"flowpay/auth-service/internal/service"
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
	metrics.InitAuthMetrics()
	db := infra.InitDB()

	defer db.Close()

	accountRepository := repository.NewAccountsRepository(db)
	refreshTokenRepository := repository.NewRefreshTokenRepository(db)
	credentialsRepository := repository.NewCredentialsRepository(db)

	authService := service.NewAuthService(db,
		accountRepository,
		refreshTokenRepository,
		credentialsRepository,
	)

	httpHandler := api.NewHandler(authService)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)
	mux.HandleFunc("/login", httpHandler.HandleAuthLoginRoute)
	mux.HandleFunc("/register", httpHandler.HandleAuthRegisterRoute)
	mux.HandleFunc("/refresh", httpHandler.HandleAuthRefreshRoute)
	mux.HandleFunc("/logout", httpHandler.HandleAuthLogoutRoute)
	log.Println("Auth service running on :8007")
	log.Fatal(http.ListenAndServe(":8007", tracing.TracingMiddleware(constants.ServiceName, mux)))
}
