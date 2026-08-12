COMPOSE_FILE := infra/docker-compose.yml
WORKER_COMPOSE_FILE := infra/docker-compose.workers.yml
MIN_COMPOSE_FILE := infra/docker-compose.infra.min.yml
SERVICE_COMPOSE_FILE := infra/docker-compose.services.yml
PAYMENT_SERVICE_DIR := services/payment-service

.PHONY: up down logs test

build:
	docker compose -f $(COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) build

build-up:
	docker compose -f $(COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) up --build -d

up:
	docker compose -f $(COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) down

min_up:
	docker compose -f $(MIN_COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) up -d

min_down:
	docker compose -f $(MIN_COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE)  -f $(WORKER_COMPOSE_FILE) -f $(SERVICE_COMPOSE_FILE) logs -f

test:
	cd $(PAYMENT_SERVICE_DIR) && go test ./...
