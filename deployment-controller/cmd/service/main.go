package main

import (
	"context"
	handler "flowpay/deployment-controller/internal/api"
	awsec2 "flowpay/deployment-controller/internal/aws"
	"flowpay/deployment-controller/internal/config"
	"flowpay/deployment-controller/internal/middleware"
	"flowpay/deployment-controller/internal/proxy"
	"flowpay/deployment-controller/internal/service"
	"flowpay/deployment-controller/internal/state"
	utils "flowpay/deployment-controller/internal/utils"
	"log"
	"net/http"
	"strconv"
	"time"
)

func getHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("deployment controller ok"))
}

func main() {
	cfg := config.Load()
	port := utils.GetEnv("PORT", "8016")
	region := utils.GetEnv("AWS_REGION", "")
	instanceID := utils.GetEnv("FLOWPAY_INSTANCE_ID", "")

	idleTimeoutMinutes, err := strconv.Atoi(utils.GetEnv("IDLE_TIMEOUT_MINUTES", "60"))
	if err != nil {
		log.Fatalf("invalid IDLE_TIMEOUT_MINUTES: %v", err)
	}

	pollIntervalSeconds, err := strconv.Atoi(utils.GetEnv("POLL_INTERVAL_SECONDS", "7"))
	if err != nil {
		log.Fatalf("invalid POLL_INTERVAL_SECONDS: %v", err)
	}

	healthPort := utils.GetEnv("FLOWPAY_APP_HEALTH_PORT", "8000")

	ctx := context.Background()

	ec2Client, err := awsec2.NewEC2Client(ctx, region, instanceID)
	if err != nil {
		log.Fatalf("failed to initialize ec2 client: %v", err)
	}

	deploymentState := state.New()
	deploymentService := service.NewDeploymentService(
		ec2Client,
		deploymentState,
		time.Duration(idleTimeoutMinutes)*time.Minute,
		time.Duration(pollIntervalSeconds)*time.Second,
		healthPort,
	)

	go deploymentService.MonitorIdle(ctx, time.Minute)

	httpHandler := handler.NewHandler(deploymentService)
	mux := http.NewServeMux()

	mux.HandleFunc("/deployment/health", getHealthCheck)
	mux.HandleFunc("/deployment/status", httpHandler.GetStatus)
	mux.HandleFunc("POST /deployment/start", httpHandler.StartInstance)
	mux.HandleFunc("POST /deployment/stop", httpHandler.StopInstance)
	mux.HandleFunc("POST /deployment/heartbeat", httpHandler.Heartbeat)

	apiProxy := proxy.New(deploymentService.ProxyTarget)
	mux.Handle("/", apiProxy)

	corsMiddleware := middleware.CORS(cfg.AllowedOrigins)

	log.Println("Deployment Controller service running on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, corsMiddleware(mux)))

}
