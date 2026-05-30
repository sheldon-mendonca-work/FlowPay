-- Outbox health checks

-- 1. Stuck PROCESSING events whose lease has expired.
-- These should be picked up again by the transaction processor.
SELECT
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    idempotency_key,
    retry_count,
    locked_until,
    updated_at,
    NOW() - locked_until AS stuck_for
FROM outbox_events
WHERE status = 'PROCESSING'
AND locked_until < NOW()
ORDER BY locked_until ASC;

-- 2. Permanently FAILED events after retry exhaustion.
-- MaxKafkaRetryCount is currently 5 in transaction-processor constants.
SELECT
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    idempotency_key,
    retry_count,
    error_code,
    error_message,
    updated_at
FROM outbox_events
WHERE status = 'FAILED'
OR (status != 'PUBLISHED' AND retry_count >= 5)
ORDER BY updated_at ASC;

-- 3. Unprocessed backlog that is eligible for claiming now.
-- This excludes events that are currently leased and still being processed.
SELECT
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    idempotency_key,
    status,
    retry_count,
    created_at,
    updated_at,
    locked_until,
    NOW() - created_at AS age
FROM outbox_events
WHERE (
    status = 'PENDING'
    OR (status = 'PROCESSING' AND locked_until < NOW())
)
AND retry_count < 5
ORDER BY created_at ASC;

-- 4. Backlog summary by status and retry count.
SELECT
    status,
    retry_count,
    COUNT(*) AS event_count,
    MIN(created_at) AS oldest_event_at,
    MAX(created_at) AS newest_event_at
FROM outbox_events
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
GROUP BY status, retry_count
ORDER BY status, retry_count;

-- 5. Outbox events without idempotency rows.
SELECT
    o.id,
    o.aggregate_id,
    o.event_type,
    o.idempotency_key,
    o.status,
    o.retry_count,
    o.created_at
FROM outbox_events o
WHERE NOT EXISTS (
    SELECT 1
    FROM idempotency_keys i
    WHERE i.idempotency_key = o.idempotency_key
);

-- 6. Published outbox events whose aggregate payment does not exist.
SELECT
    o.id,
    o.aggregate_id,
    o.event_type,
    o.idempotency_key,
    o.status,
    o.published_at
FROM outbox_events o
WHERE o.status = 'PUBLISHED'
AND NOT EXISTS (
    SELECT 1
    FROM payments p
    WHERE p.id::text = o.aggregate_id
);
