-- Day 1

CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- psql -h localhost -U postgres -d payment_db -f migrations/Day1_PaymentTableCreate.sql