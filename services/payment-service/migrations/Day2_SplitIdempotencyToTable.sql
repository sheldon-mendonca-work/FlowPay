-- Day2_SplitIdempotencyToTable.sql

-- migration to uuid strategy:
-- 1. we do this in golang
-- 2. for each user_id in payments table, we create a set of user ids and assign a uuid for each user.
-- 3. create a tuple in accounts table and update payments where userid == userid from golang

-- however, since we dont have receiver_id, we'll just truncate the table
-- In production, this would require a backward-compatible migration instead of truncation
-- TRUNCATE TABLE payments CASCADE;


-- Change user id in payments to sender id. Also add receiver id
ALTER TABLE payments RENAME COLUMN user_id TO sender_id;
ALTER TABLE payments ALTER COLUMN sender_id TYPE UUID USING sender_id::uuid;
ALTER TABLE payments ADD receiver_id UUID NOT NULL;
ALTER TABLE payments RENAME COLUMN payment_id TO id;
ALTER TABLE payments ALTER COLUMN sender_id SET NOT NULL;
ALTER TABLE payments ALTER COLUMN status SET DEFAULT 'PENDING';
ALTER TABLE payments ALTER COLUMN idempotency_key SET NOT NULL;
ALTER TABLE payments
ADD CONSTRAINT chk_payment_status 
CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED'));
ALTER TABLE payments
ADD CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key);

-- create accouunts table
CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    currency TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- create idempotency key table
CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_body JSONB,
    status TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    error_code TEXT,
    error_message TEXT,
    owner_token TEXT,
    payment_id UUID,
    locked_until TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_idempotency_status ON idempotency_keys(status);

-- create transactions table
CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    payment_id UUID NOT NULL,
    account_id UUID NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_transaction_payment FOREIGN KEY (payment_id) REFERENCES payments(id),
    CONSTRAINT fk_transaction_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);
CREATE INDEX idx_transactions_payment_id ON transactions(payment_id);
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
-- connect payments to accounts

ALTER TABLE payments
ADD CONSTRAINT fk_sender FOREIGN KEY (sender_id) REFERENCES accounts(id);

ALTER TABLE payments
ADD CONSTRAINT fk_receiver FOREIGN KEY (receiver_id) REFERENCES accounts(id);

CREATE UNIQUE INDEX unique_payment_type
ON transactions (payment_id, type);
