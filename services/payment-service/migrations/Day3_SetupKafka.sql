-- Instead of directly pushing events to kafka, we will create a outbox events table that will handle event publishing in kafka

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,

    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,

    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL,

    payload JSONB NOT NULL,

    status TEXT CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED')) NOT NULL,
    locked_until TIMESTAMP,
    retry_count INT DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP
);

CREATE INDEX idx_outbox_status ON outbox_events(status);