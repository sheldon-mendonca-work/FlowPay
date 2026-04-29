COMPOSE_FILE := infra/docker-compose.yml
PAYMENT_SERVICE_DIR := services/payment-service

.PHONY: up down logs test

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

test:
	cd $(PAYMENT_SERVICE_DIR) && go test ./...
