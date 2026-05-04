# 🔧 FlowPay — Distributed Fintech System

## 🧠 Objective

Build a **production-grade distributed payment system** 

Focus areas:

* Strong consistency (Ledger-first design)
* Idempotent APIs
* Event-driven architecture (Kafka)
* Failure-first system design
* Observability (logs, metrics, tracing)

---

## 🏗️ Tech Stack

* **Golang** (clean architecture)
* **PostgreSQL** (source of truth)
* **Kafka (KRaft)** (async event processing)
* **Redis** (non-critical optimizations)
* **Docker Compose** (local infra)
* **Prometheus + Grafana** (metrics)
* **Jaeger** (tracing)

---

## 🧩 System Components

* API Gateway
* Payment Service ✅ (Day 1)
* Transaction Processor (planned)
* Ledger Service (planned)
* Wallet Service (planned)
* Fraud, Offers, Scheduler, Reconciliation (planned)

---

## ⚖️ Core Invariants

* Money correctness comes from **DB (Ledger later)**
* APIs must be **idempotent**
* Kafka = **at-least-once delivery**
* System must tolerate:

  * retries
  * duplicates
  * partial failures

---

# 🚀 Progress

---

## ✅ Day 0 — Infrastructure Setup

### ✔ Implemented

* Dockerized infra:

  * PostgreSQL
  * Redis
  * Kafka (KRaft mode)
  * Kafka UI
  * Prometheus
  * Grafana
  * Jaeger

* Kafka:

  * Topics created via config
  * Manual produce/consume verified
  * Persistence across restarts verified

* API Gateway:

  * `/health` endpoint working

---

## ✅ Day 1 — Payment Service (Idempotent API)

### 🎯 Goal

Build a **safe payment creation API** with:

* Idempotency
* DB persistence
* Concurrency safety

---

### 🧩 API

**POST /payments**

```json
{
  "user_id": "user_1",
  "amount": 100,
  "currency": "INR",
  "idempotency_key": "idem-123"
}
```

**Response (202 Accepted)**

```json
{
  "payment_id": "uuid",
  "status": "CREATED"
}
```

---

### 🗄️ Database

```sql
CREATE TABLE payments (
    payment_id UUID PRIMARY KEY,
    user_id VARCHAR(50),
    amount BIGINT,
    currency TEXT,
    status TEXT NOT NULL DEFAULT 'CREATED',
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

---

### 🔐 Idempotency Strategy

* Enforced via:

  ```text
  UNIQUE(idempotency_key)
  ```

* Flow:

  1. Try INSERT
  2. If success → return response
  3. If conflict:

     * Same payload → return existing payment
     * Different payload → return **409 Conflict**

---

### ⚔️ Concurrency Handling

Tested with parallel requests:

```bash
for i in {1..5}; do curl ... & done
```

✔ Result:

* Only **1 row inserted**
* All responses return same `payment_id`

---

### 🔍 Observability

* Structured logs:

  * service
  * endpoint
  * trace_id
  * request_id
  * latency
  * status

* Middleware:

  * Trace ID propagation
  * Request ID generation

* Prometheus metrics:

  * request_count
  * error_count
  * request_latency

---

### 🧠 Key Learnings

* DB-based idempotency > Redis-based idempotency
* Insert-first pattern avoids race conditions
* Unique constraints are concurrency primitives
* Observability must be built from day 1

---

### ⚠️ Edge Cases Handled

* Duplicate requests (same payload)
* Duplicate requests (different payload)
* Concurrent writes (race conditions)
* Invalid request body
* Partial failure recovery (DB conflict handling)

---

### 🧪 Validation

* Manual API testing via curl
* Parallel request simulation
* DB verification:

  ```sql
  SELECT COUNT(*) FROM payments WHERE idempotency_key = '...';
  ```

---

## 🚧 Next

* Day 2:

  * Transaction Processor service
  * Async consistency model

---
