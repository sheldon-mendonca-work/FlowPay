# FlowPay

> A production-grade distributed payment platform built to explore fintech architecture, event-driven systems, reliability engineering, and financial consistency.

FlowPay simulates how modern payment platforms process payments, apply promotional offers, publish domain events, recover from failures, and maintain financial correctness under retries and concurrent workloads.

---

# Architecture

```text
                 +----------------------+
                 |   React Frontend     |
                 +----------+-----------+
                            |
                      API Gateway
                            |
        +-------------------+-------------------+
        |                   |                   |
 Payment Service     Account/Auth Service   Offer Service
        |                   |                   |
        +-------------------+-------------------+
                            |
                       PostgreSQL
                            |
                 Transactional Outbox
                            |
                          Kafka
                            |
                 Payment Executor Service
```

---

# Highlights

- Production-grade distributed microservice architecture
- Event-driven payment processing using Kafka
- Transactional Outbox Pattern
- Idempotent REST APIs
- Replay-safe Kafka consumers
- Optimistic locking for concurrent offer redemption
- Lease-based worker coordination
- Financial consistency through immutable transaction history
- Structured logging, distributed tracing, and Prometheus metrics
- Failure-first system design

---

# Tech Stack

### Backend

- Go
- PostgreSQL
- Kafka (KRaft)
- Redis

### Infrastructure

- Docker Compose
- Prometheus
- Grafana
- Jaeger

### Frontend

- React
- TypeScript
- Tailwind CSS

---

# Services

| Service | Purpose |
|----------|---------|
| API Gateway | Entry point for frontend requests and routing |
| Account Service | Account, company, and user management |
| Auth Service | JWT authentication, refresh tokens, demo login |
| Payment Service | Payment APIs with idempotent request handling |
| Payment Executor | Kafka consumer that executes payments asynchronously |
| Offer Service | Offer creation, reservation, redemption, and expiry |
| Reconciliation Service | Detects inconsistencies across payments, transactions, idempotency records, and outbox events |

---

# Distributed Systems Concepts

## Reliability

- Idempotency Keys
- Retry-safe APIs
- Crash Recovery
- Replay-safe Consumers
- Optimistic Locking

## Event-Driven Architecture

- Kafka Event Streaming
- Transactional Outbox Pattern
- At-Least-Once Delivery
- Consumer Deduplication

## Financial Consistency

- Atomic payment execution
- Immutable transaction history
- Controlled payment state transitions

## Scalability

- Asynchronous processing
- Lease-based worker coordination
- Eventual consistency
- Service isolation

## Observability

- Structured JSON logging
- Distributed tracing
- RED metrics
- Prometheus + Grafana dashboards

---

# Features

## Payment Processing

- Payment creation API
- Idempotent payment execution
- Asynchronous execution via Kafka
- Transaction history
- Failure recovery
- Duplicate request protection
- Replay-safe event handling

---

## Offer Engine

Supports:

- Fixed discount offers
- Percentage discount offers
- Fixed cashback offers
- Percentage cashback offers

Features:

- Offer creation
- Offer listing
- Offer reservation
- Offer redemption
- Automatic offer expiry
- Global redemption limits
- Per-user redemption limits
- Payment eligibility rules
- Optimistic locking
- Inventory-safe redemption workflow

---

## Authentication

- User registration
- JWT access tokens
- Refresh tokens
- Logout
- Demo account login
- Company login

---

## Account Management

- Account management
- Company management
- User management
- Seeded demo accounts

---

## Reconciliation

Detects inconsistencies between:

- Payments
- Transactions
- Idempotency records
- Outbox events

Provides both individual and bulk reconciliation endpoints.

---

# Reliability Guarantees

FlowPay is designed assuming failures are normal.

The system safely handles:

- Duplicate HTTP requests
- Duplicate Kafka events
- Service restarts
- Worker crashes
- Event replay
- Partial failures
- Concurrent offer redemption
- Network interruptions

---

# Observability

Every service emits:

- Structured JSON logs
- Request IDs
- Trace IDs
- Prometheus metrics

Metrics follow the RED methodology:

- **Rate**
- **Errors**
- **Duration**

---

# Running Locally

```bash
docker compose up -d

# Start individual services
go run payment-service
go run payment-executor
go run account-service
go run auth-service
go run offer-service
go run reconciliation-service
```

---

# Roadmap

## Completed

- Payment Service
- Kafka Event Pipeline
- Transactional Outbox Pattern
- Payment Executor
- Offer Engine
- Account Service
- Authentication
- Reconciliation Service

## In Progress

- Frontend Integration
- API Gateway Routing

## Planned

- Dedicated Ledger Service
- Dedicated Wallet Service
- Fraud Detection Service
- Scheduler Service
- Load Testing
- Multi-region Architecture

---

# Engineering Focus

FlowPay is designed as a hands-on study of modern distributed payment systems, with emphasis on:

- Event-driven architectures
- Distributed transaction patterns
- Financial consistency guarantees
- Reliability engineering
- Failure recovery
- High-concurrency systems
- Production-grade observability
- System evolution and scalability

---

# Future Enhancements

- Dedicated Ledger microservice
- Dedicated Wallet microservice
- Fraud detection pipeline
- Scheduled financial workflows
- Advanced reconciliation automation
- Chaos testing
- Horizontal scaling
- Multi-region deployment
- Exactly-once effect patterns
- Kubernetes deployment