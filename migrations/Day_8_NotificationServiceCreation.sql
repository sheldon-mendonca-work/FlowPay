CREATE TABLE payment_timeline_steps (
    id UUID PRIMARY KEY,

    trace_id TEXT NOT NULL,
    payment_id UUID NOT NULL,
    step_name VARCHAR(50) NOT NULL CHECK (
        step_name IN (
            'PAYMENT_INITIATED',
            'OFFER_EVALUATED',
            'OFFER_RESERVED',
            'PAYMENT_VALIDATED',
            'ACCOUNTS_UPDATED',
            'PAYMENT_PERSISTED',
            'OUTBOX_EVENT_CREATED',
            'KAFKA_PUBLISHED',
            'OFFER_REDEEMED',
            'PROMO_POOL_DEBITED',
            'CASHBACK_CREDITED',
            'PAYMENT_COMPLETED'
        )
    ),
    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'CREATED',
            'PROCESSING',
            'SUCCESS',
            'FAILED'
        )
    ),

    request_id TEXT,

    completed_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_payment_timeline_step UNIQUE (trace_id, step_name, status)
);

CREATE INDEX idx_payment_timeline_trace_id
ON payment_timeline_steps(trace_id, completed_time);
