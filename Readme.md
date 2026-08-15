# FlowPay

> A production-grade distributed payment platform built to explore fintech architecture, event-driven systems, reliability engineering, and financial consistency.

FlowPay simulates how modern payment platforms process payments, apply promotional offers, publish domain events, recover from failures, and maintain financial correctness under retries and concurrent workloads.

## Deployment

FlowPay is currently deployed using an **on-demand EC2 instance** rather than running continuously.

The deployment controller starts the application infrastructure when it is needed and can stop the instance after a period of inactivity. This keeps the infrastructure cost low while still providing a real cloud deployment for the system.

Because the application is started on demand, the first request after a period of inactivity may take some time while the EC2 instance and application services start.

The current deployment intentionally does **not use Kubernetes/EKS**. In a production environment at larger scale, Kubernetes would be a natural choice for container orchestration, service scheduling, health management, scaling, and self-healing. For this project, the simpler EC2-based deployment provides a better cost/complexity tradeoff while allowing the focus to remain on distributed systems, concurrency, reliability, and financial consistency.

The services themselves remain containerized, so the architecture can be migrated to a container orchestration platform without fundamentally changing the application architecture.

---
## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React, TypeScript |
| Backend | Go |
| API | HTTP/REST, Server-Sent Events (SSE) |
| Messaging | Apache Kafka |
| Database | PostgreSQL |
| Cache / Distributed State | Redis |
| Observability | Prometheus, Grafana, Loki, Promtail |
| Containerization | Docker, Docker Compose |
| Cloud | AWS EC2, Elastic IP |
| Reverse Proxy | Nginx |
| CI/CD | GitHub Actions |
| Deployment | EC2-based on-demand deployment |

---

# Architecture

```text
                         +----------------------+
                         |    React Frontend    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |     API Gateway      |
                         +----------+-----------+
                                    |
             +----------------------+----------------------+
             |                      |                      |
             v                      v                      v
    +----------------+     +----------------+     +----------------+
    | Payment        |     | Account/Auth   |     | Offer          |
    | Service        |     | Service        |     | Service        |
    +-------+--------+     +-------+--------+     +-------+--------+
            |                      |                      |
            +----------------------+----------------------+
                                   |
                                   v
                          Transactional Outbox
                                    |
                                    v
                         +----------------------+
                         |        Kafka         |
                         |   Domain Events      |
                         +----------+-----------+
                                    |
                                    v
                +-------------------+-------------------+
                |                   |                   |
                v                   v                   v
       +----------------+  +----------------+  +----------------+
       | Payment        |  | Notification   |  | Other          |
       | Executor       |  | Service / SSE  |  | Consumers      |
       +----------------+  +----------------+  +----------------+
                |                   |                   |
                v                   v                   v
                +-------------------+-------------------+
                         +----------------------+
                         |     PostgreSQL       |
                         |   Financial State    |
                         +----------+-----------+
                                    |
                +--------------------------------------+
                |              Workers                  |
                |--------------------------------------|
                | Outbox Publisher                      |
                | Offer Outbox Publisher                |
                | Offer Expiry Worker                   |
                +--------------------------------------+

                         +----------------------+
                         |        Redis         |
                         | Cache / Idempotency  |
                         | Rate Limiting        |
                         +----------------------+

                         +----------------------+
                         |    Observability     |
                         |----------------------|
                         | Prometheus           |
                         | Grafana              |
                         | Loki / Promtail      |
                         +----------------------+

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

# Services

## API Gateway
The entry point for client requests. Handles routing, request-level concerns, and provides a single API surface to the frontend.

## Payment Service
Owns payment creation and payment lifecycle operations, including idempotency and transactional persistence.

## Account / Auth Services
Manage account-related operations and authentication/authorization concerns.

## Offer Service
Handles promotional offer reservation, redemption, idempotency, and expiry.

## PostgreSQL
The source of truth for strongly consistent financial state including payments, accounts, transactions and offer state.

## Redis
Used for low-latency distributed state such as caching, idempotency/rate-limiting use cases and other data that does not require PostgreSQL-level durability.

## Kafka
Provides asynchronous communication between services and decouples payment processing from downstream consumers.

## Transactional Outbox
Ensures domain events are persisted atomically with the corresponding database transaction before being published to Kafka.

## Workers
Background workers handle asynchronous processing such as:

Outbox event publishing
Offer outbox publishing
Offer expiry
Other scheduled/background operations

## Payment Executor
Consumes payment events and performs downstream payment execution and financial processing.

## Notification Service
Consumes domain events and maintains payment timelines. The frontend can subscribe through Server-Sent Events (SSE) to observe payment progress in real time.

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

--

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
