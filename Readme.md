# 🔧 FlowPay — Distributed Fintech System

Production-grade distributed payment system focused on reliability, idempotency, and fault tolerance.

Built to learn and demonstrate real-world backend and distributed systems engineering.

---

## 🚀 Highlights

* Distributed payment processing architecture
* Transactional Outbox Pattern
* Kafka-based async event processing
* Idempotent payment APIs
* Atomic money movement
* Replay-safe event handling
* Lease-based worker recovery
* Structured observability and tracing
* Failure-first system design

---

## 🏗️ Tech Stack

* Golang
* PostgreSQL
* Kafka (KRaft)
* Redis
* Docker Compose
* Prometheus + Grafana
* Jaeger

---

## 🧩 Services

* API Gateway
* Payment Service
* Transaction Processor
* Payment Executor

---

## ⚙️ Key Distributed Systems Concepts

* Transactional Outbox
* At-Least-Once Delivery
* Idempotency Keys
* Retry & Replay Handling
* Lease-Based Coordination
* Crash Recovery
* Event-Driven Architecture
* Distributed Tracing

---

# ✅ Progress

## Day 0 — Infrastructure

* Dockerized local distributed environment
* Kafka + PostgreSQL + Redis setup
* Observability stack setup
* API Gateway initialization

---

## Day 1 — Payment Service

Implemented:

* Payment creation API
* Idempotent request handling
* Concurrency-safe inserts
* Structured logging
* Metrics and tracing

Validated:

* Parallel request safety
* Duplicate request handling
* Conflict detection

---

## Day 2 — Async Processing Pipeline

Implemented:

* Transactional outbox pattern
* Kafka publishing worker
* Lease-based event claiming
* Retry handling
* Failure recovery
* Replay-safe processing

System now tolerates:

* worker crashes
* duplicate events
* partial failures
* Kafka redelivery

---

## Day 3 — Payment Executor

Implemented:

* Kafka consumer
* Atomic balance updates
* Payment execution workflow
* Ledger-style transaction recording
* Idempotent replay handling
* End-to-end trace propagation

Core guarantee:

* Money movement remains transactionally consistent even during retries and duplicate event delivery.

---

## 📚 What This Project Demonstrates

* Backend engineering at scale
* Distributed systems fundamentals
* Event-driven architecture
* Reliability engineering
* Failure-oriented system design
* Production-style observability
* Financial consistency guarantees
