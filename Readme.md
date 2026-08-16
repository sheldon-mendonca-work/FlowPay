# [FlowPay](https://flowpay-ui.netlify.app)

> A production-grade distributed payment platform built to explore fintech architecture, event-driven systems, reliability engineering and financial consistency.

FlowPay simulates how modern payment platforms process payments, apply promotional offers, publish domain events, recover from failures and maintain financial correctness under retries and concurrent workloads.

---
# Use of AI

AI was used as an engineering assistant throughout the project for research, debugging, documentation and exploring implementation approaches. **All coding, architectural decisions, implementation, testing and validation were reviewed and driven by me.**

> 🤖 **Fun fact:** This README was also generated with AI 😃 

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

```
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
- Structured logging, distributed tracing and Prometheus metrics
- Failure-first system design

---

## Deployment

FlowPay is currently deployed using an **on-demand EC2 instance** rather than running continuously.

The deployment controller starts the application infrastructure when it is needed and can stop the instance after a period of inactivity. This keeps the infrastructure cost low while still providing a real cloud deployment for the system.

Because the application is started on demand, the first request after a period of inactivity may take some time while the EC2 instance and application services start.

The current deployment intentionally does **not use Kubernetes/EKS**. In a production environment at larger scale, Kubernetes would be a natural choice for container orchestration, service scheduling, health management, scaling and self-healing. For this project, the simpler EC2-based deployment provides a better cost/complexity tradeoff while allowing the focus to remain on distributed systems, concurrency, reliability and financial consistency.

The services themselves remain containerized, so the architecture can be migrated to a container orchestration platform without fundamentally changing the application architecture.

---

# Services

## API Gateway

The entry point for client requests. Handles routing, request-level concerns, authentication-related middleware and provides a unified API surface to the frontend.

## Payment Service

Owns payment creation and lifecycle operations, including idempotency, validation, transactional persistence and payment state management.

## Account Service

Manages user and company accounts and account-related operations.

## Auth Service

Handles authentication and authorization, including JWT access tokens, refresh tokens, logout and demo authentication flows.

## Offer Service

Manages promotional offers, including creation, reservation, redemption, eligibility, redemption limits and expiry.

## Payment Executor

Consumes payment events from Kafka and performs asynchronous payment execution and downstream financial processing.

## Notification Service

Consumes domain events and maintains payment timelines. The frontend can subscribe through Server-Sent Events (SSE) to observe payment progress in real time.

## Workers

Background workers handle asynchronous and scheduled processing:

- Outbox event publishing
- Offer outbox publishing
- Offer expiry
- Other background operations

## PostgreSQL

The primary source of truth for strongly consistent financial state, including payments, accounts, transactions, offers, idempotency records and outbox events.

## Redis

Provides low-latency distributed state for use cases such as caching, rate limiting and other ephemeral coordination that does not require PostgreSQL-level durability.

## Kafka

Provides asynchronous event streaming between services and decouples payment processing from downstream consumers.

## Observability Stack

FlowPay uses:

- Prometheus for metrics collection
- Grafana for dashboards
- Loki for log aggregation
- Promtail for log shipping
- Distributed tracing for request/event correlation

---

# Distributed Systems Concepts

## Reliability

 - Idempotency keys
 -  Retry-safe APIs
 -  Crash recovery
 -  Replay-safe consumers
 -  Consumer deduplication
 -  Optimistic locking
 -  Health checks
 -  Graceful service startup

## Event-Driven Architecture

- Kafka Event Streaming
- Transactional Outbox Pattern
- At-Least-Once Delivery
- Asynchronous consumers
- Consumer Deduplication

## Financial Consistency

- Atomic payment execution
- Immutable transaction history
- Controlled payment state transitions
- Transactional persistence
- Reconciliation of financial state

## Concurrency & Scalability

- Asynchronous processing
- Lease-based worker coordination
- Eventual consistency
- Distributed coordination
- Service isolation
- Redis-backed coordination
- Background workers

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
- Asynchronous execution through Kafka
- Transaction history
- Failure recovery
- Duplicate request protection
- Replay-safe event handling
- Payment processing timeline through SSE

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

## Authentication

- User registration
- JWT access tokens
- Refresh tokens
- Logout
- Demo account login
- Company login

## Account Management

- Account management
- Company management
- User management
- Seeded demo accounts

## Reconciliation

Detects inconsistencies between:

- Payments
- Transactions
- Idempotency records
- Outbox events

Provides individual and bulk reconciliation endpoints.

---

# Reliability Guarantees

FlowPay is designed under the assumption that **failures are normal rather than exceptional**.

The system is designed to safely handle:

- Duplicate HTTP requests
- Duplicate Kafka events
- Service restarts
- Worker crashes
- Event replay
- Partial failures
- Concurrent offer redemption
- Network interruptions

These guarantees are implemented through mechanisms such as idempotency keys, transactional outbox, consumer deduplication, optimistic locking, retries and reconciliation.

---

# Observability

Every service exposes operational signals including:

- Structured JSON logs
- Request IDs
- Trace IDs
- Prometheus metrics

Metrics follow the **RED methodology**:

- **Rate** — request/event throughput
- **Errors** — failures and error rates
- **Duration** — request and dependency latency

The observability stack provides dashboards for service health, business operations, dependencies, runtime behavior and recent errors.

---

# Engineering Focus

FlowPay is a hands-on exploration of the engineering problems behind distributed payment systems.

The project focuses on:

- Event-driven architecture
- Distributed transaction patterns
- Financial consistency
- Idempotency
- Concurrency
- Failure recovery
- Reliability engineering
- Production-grade observability
- System evolution and scalability

Rather than optimizing for infrastructure complexity, the project focuses on understanding the **distributed-system behavior and correctness guarantees** underneath a payment platform.

---

# Future Enhancements

- Dedicated Ledger microservice
- Dedicated Wallet microservice
- Fraud detection pipeline
- Scheduled financial workflows
- Advanced reconciliation automation
- Chaos testing
- Horizontal service scaling
- Multi-region deployment

- Exactly-once effect patterns
- Kubernetes deployment
