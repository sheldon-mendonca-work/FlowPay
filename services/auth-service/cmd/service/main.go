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
	"flowpay/pkg/httpx"
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

	metrics.InitMetrics()
	metrics.InitAuthMetrics()

	cfg := config.Load()
	db := infra.InitDB()
	defer db.Close()

	redisClient := infra.InitRedis()
	defer redisClient.Close()

	credentialsRepository := repository.NewCredentialsRepository(db)
	refreshTokenRepository := repository.NewRefreshTokenRepository(db)
	defaultCredentialsRepository := repository.NewDefaultCredentialsRepository(db)
	accountClient := client.NewAccountServiceClient(cfg.AccountServiceURL)
	jwtManager := jwt.NewManager(cfg.JWTSecret)

	authService := service.NewAuthService(
		db,
		redisClient,
		credentialsRepository,
		refreshTokenRepository,
		defaultCredentialsRepository,
		accountClient,
		jwtManager,
	)

	httpHandler := api.NewHandler(authService)

	router := httpx.NewRouter()

	router.HandleFunc("/auth/health", getHealthCheck)
	router.HandleFunc("/metrics", handleMetrics)
	router.HandleFunc("/auth/login", httpHandler.HandleAuthLoginRoute)
	router.HandleFunc("/auth/register", httpHandler.HandleAuthRegisterRoute)
	router.HandleFunc("/auth/refresh", httpHandler.HandleAuthRefreshRoute)
	router.HandleFunc("/auth/logout", httpHandler.HandleAuthLogoutRoute)
	router.HandleFunc("/auth/default-login", httpHandler.HandleAuthDefaultLoginRoute)
	router.HandleFunc("/auth/defaultLoginAccount", httpHandler.HandleAuthDefaultLoginAccountRoute)
	router.HandleFunc("/auth/defaultLoginUser", httpHandler.HandleAuthDefaultLoginUserRoute)

	log.Printf("Auth service running on :%s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, tracing.TracingMiddleware(constants.ServiceName, router)))
}
