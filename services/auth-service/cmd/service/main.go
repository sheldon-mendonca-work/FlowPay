package main

import (
	"flowpay/auth-service/internal/api"
	"flowpay/auth-service/internal/client"
	"flowpay/auth-service/internal/config"
	"flowpay/auth-service/internal/constants"
	"flowpay/auth-service/internal/infra"
	"flowpay/auth-service/internal/jwt"
	"flowpay/auth-service/internal/repository"
	"flowpay/auth-service/internal/service"
	"flowpay/pkg/healthcheck"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("auth service ok"))
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	healthcheck.RunIfRequested("http://localhost:8007/auth/health")

	metrics.InitAuthMetrics()

	cfg := config.Load()
	db := infra.InitDB()
	defer db.Close()

	credentialsRepository := repository.NewCredentialsRepository(db)
	refreshTokenRepository := repository.NewRefreshTokenRepository(db)
	defaultCredentialsRepository := repository.NewDefaultCredentialsRepository(db)
	accountClient := client.NewAccountServiceClient(cfg.AccountServiceURL)
	jwtManager := jwt.NewManager(cfg.JWTSecret)

	authService := service.NewAuthService(
		db,
		credentialsRepository,
		refreshTokenRepository,
		defaultCredentialsRepository,
		accountClient,
		jwtManager,
	)

	httpHandler := api.NewHandler(authService)

	mux := http.NewServeMux()
	mux.HandleFunc("/auth/health", getHealthCheck)
	mux.HandleFunc("/metrics", handleMetrics)
	mux.HandleFunc("/auth/login", httpHandler.HandleAuthLoginRoute)
	mux.HandleFunc("/auth/register", httpHandler.HandleAuthRegisterRoute)
	mux.HandleFunc("/auth/refresh", httpHandler.HandleAuthRefreshRoute)
	mux.HandleFunc("/auth/logout", httpHandler.HandleAuthLogoutRoute)
	mux.HandleFunc("/auth/default-login", httpHandler.HandleAuthDefaultLoginRoute)
	mux.HandleFunc("/auth/defaultLoginAccount", httpHandler.HandleAuthDefaultLoginAccountRoute)
	mux.HandleFunc("/auth/defaultLoginUser", httpHandler.HandleAuthDefaultLoginUserRoute)

	log.Printf("Auth service running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, tracing.TracingMiddleware(constants.ServiceName, mux)))
}
