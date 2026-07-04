CREATE TABLE payment_timeline_steps (
    id UUID PRIMARY KEY,

    payment_id UUID NOT NULL,
    step_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'CREATED',
            'PROCESSING',
            'SUCCESS',
            'FAILED'
        )
    ),

    trace_id TEXT,
    request_id TEXT,

    completed_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_payment_timeline_step UNIQUE (payment_id, step_name, status)
);

CREATE INDEX idx_payment_timeline_payment_id
ON payment_timeline_steps(payment_id, completed_time);
