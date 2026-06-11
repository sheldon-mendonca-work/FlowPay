# 🔧 FlowPay — Distributed Fintech System

Production-grade distributed payment platform built to explore real-world fintech architecture, reliability engineering, and distributed systems design.

FlowPay simulates how modern payment platforms handle money movement, event processing, promotions, reconciliation, and failure recovery at scale.

---

## 🚀 Highlights

* Distributed microservice architecture
* Event-driven payment processing
* Double-entry ledger accounting
* Transactional Outbox Pattern
* Idempotent APIs and consumers
* Offer & promotion engine
* Concurrency-safe inventory reservation
* Replay-safe event handling
* Audit trail and reconciliation support
* End-to-end observability
* Failure-first system design

---

## 🏗️ Tech Stack

* Golang
* PostgreSQL
* Kafka (KRaft)
* Redis
* Docker Compose
* Prometheus
* Grafana
* Jaeger

---

## 🧩 Services

* API Gateway
* Payment Service
* Transaction Processor
* Ledger Service
* Wallet Service
* Offer Service

---

## ⚙️ Distributed Systems Concepts

### Reliability

* Idempotency Keys
* Retry Handling
* Crash Recovery
* Replay Protection
* Optimistic Locking

### Event-Driven Architecture

* Kafka Event Streaming
* Transactional Outbox
* At-Least-Once Delivery
* Consumer Deduplication

### Financial Consistency

* Double-Entry Ledger
* Immutable Ledger Records
* Atomic Balance Updates
* Wallet Projection Model

### Scalability

* Lease-Based Worker Coordination
* Async Processing Pipelines
* Eventual Consistency
* Service Isolation

### Observability

* Structured Logging
* Distributed Tracing
* RED Metrics
* Operational Monitoring

---

## ✅ Implemented Features

### Payment Processing

Implemented:

* Payment creation API
* Idempotent payment execution
* Async transaction orchestration
* Duplicate request protection
* Failure recovery workflows

Guarantees:

* Safe retries
* No duplicate processing
* Consistent payment state transitions

---

### Event Processing Platform

Implemented:

* Transactional Outbox Pattern
* Kafka publishing workers
* Lease-based coordination
* Replay-safe consumers
* End-to-end trace propagation

Guarantees:

* At-least-once delivery
* Crash recovery
* Event replay safety

---

### Ledger Service

Implemented:

* Double-entry accounting model
* Immutable ledger records
* Atomic transaction recording
* Financial audit support

Guarantees:

* Money is never created or destroyed outside the ledger
* Ledger remains the system of record

---

### Wallet Service

Implemented:

* Real-time balance projection
* Ledger-driven updates
* Event-based synchronization

Guarantees:

* Wallet balances derived from ledger activity
* Eventual consistency with financial source of truth

---

### Offer & Promotion Engine

#### Offer Management

Implemented:

* Admin-created offers
* Fixed-value discounts
* Percentage-based discounts
* Fixed cashback offers
* Percentage cashback offers

#### Offer Controls

Implemented:

* Global redemption limits
* Per-user redemption limits
* Payment amount eligibility rules
* Offer lifecycle management

#### Concurrency Protection

Implemented:

* Reservation-based allocation model
* Optimistic locking
* Oversubscription prevention
* Inventory-safe redemption workflow

#### Auditability

Implemented:

* Offer event history
* Redemption tracking
* Refund tracking
* Operational audit trail

---

## 📊 Observability

Every service includes:

* Structured JSON logging
* Request IDs
* Trace IDs
* Prometheus metrics
* Distributed tracing support

Metrics follow RED principles:

* Rate
* Errors
* Duration

---

## 🛡️ Reliability Guarantees

FlowPay is intentionally designed around failure scenarios.

The system tolerates:

* Duplicate API requests
* Duplicate Kafka events
* Worker crashes
* Service restarts
* Network interruptions
* Partial failures
* Event replay
* Concurrent access races

---

## 📚 What This Project Demonstrates

* Backend engineering
* Distributed systems design
* Event-driven architecture
* Fintech platform fundamentals
* Reliability engineering
* Financial consistency guarantees
* Observability and operations
* Concurrency control
* Failure recovery patterns

---

## 📈 Current Status

### Completed

* ✅ Payment Service
* ✅ Transaction Processor
* ✅ Kafka Event Pipeline
* ✅ Ledger Service
* ✅ Wallet Service
* ✅ Offer Creation Engine

### In Progress

* 🔄 Offer Reservation System
* 🔄 Offer Redemption Workflow
* 🔄 Offer Refund Processing

### Planned

* Fraud Detection Service
* Scheduler Service
* Reconciliation Service
* Load Testing & Capacity Validation
* Multi-Region Design Exploration

---

## 🎯 Engineering Focus

FlowPay is designed as a hands-on study of how large-scale fintech systems are built and operated, with particular emphasis on:

* Consistency vs Availability trade-offs
* Failure recovery and resiliency
* Event-driven system design
* Distributed transaction patterns
* Financial correctness guarantees
* Production-grade observability
* High-concurrency system behavior
