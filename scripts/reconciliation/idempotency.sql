-- Idempotency consistency checks

-- 1. Completed idempotency rows without matching payments.
SELECT
    i.idempotency_key,
    i.payment_id,
    i.status,
    i.updated_at
FROM idempotency_keys i
WHERE i.status = 'COMPLETED'
AND NOT EXISTS (
    SELECT 1
    FROM payments p
    WHERE p.id = i.payment_id
);

-- 2. Payments missing completed idempotency.
SELECT
    p.id AS payment_id,
    p.idempotency_key,
    p.status AS payment_status,
    i.status AS idempotency_status,
    i.payment_id AS idempotency_payment_id
FROM payments p
LEFT JOIN idempotency_keys i
    ON i.idempotency_key = p.idempotency_key
WHERE i.idempotency_key IS NULL
OR i.status != 'COMPLETED'
OR i.payment_id IS DISTINCT FROM p.id;

-- 3. Idempotency rows missing canonical payment_id.
SELECT
    i.idempotency_key,
    i.status,
    i.owner_token,
    i.locked_until,
    i.created_at,
    i.updated_at
FROM idempotency_keys i
WHERE i.payment_id IS NULL;

-- 4. Duplicate canonical payment_id across idempotency rows.
SELECT
    i.payment_id,
    COUNT(*) AS idempotency_row_count,
    ARRAY_AGG(i.idempotency_key ORDER BY i.created_at) AS idempotency_keys
FROM idempotency_keys i
WHERE i.payment_id IS NOT NULL
GROUP BY i.payment_id
HAVING COUNT(*) > 1
ORDER BY idempotency_row_count DESC, i.payment_id;

-- 5. Expired IN_PROGRESS takeover candidates.
SELECT
    i.idempotency_key,
    i.payment_id,
    i.owner_token,
    i.locked_until,
    NOW() - i.locked_until AS expired_for,
    i.updated_at
FROM idempotency_keys i
WHERE i.status = 'IN_PROGRESS'
AND i.locked_until < NOW()
ORDER BY i.locked_until ASC;

-- 6. IN_PROGRESS rows with no outbox event.
SELECT
    i.idempotency_key,
    i.payment_id,
    i.owner_token,
    i.locked_until,
    i.created_at
FROM idempotency_keys i
WHERE i.status = 'IN_PROGRESS'
AND NOT EXISTS (
    SELECT 1
    FROM outbox_events o
    WHERE o.idempotency_key = i.idempotency_key
);

-- 7. Idempotency/outbox payment_id divergence.
SELECT
    i.idempotency_key,
    i.payment_id AS idempotency_payment_id,
    o.aggregate_id AS outbox_payment_id,
    o.id AS outbox_event_id,
    o.status AS outbox_status,
    o.retry_count
FROM idempotency_keys i
JOIN outbox_events o
    ON o.idempotency_key = i.idempotency_key
WHERE i.payment_id IS NOT NULL
AND o.aggregate_id IS DISTINCT FROM i.payment_id::text;

-- 8. Multiple outbox payment IDs for one idempotency key.
SELECT
    o.idempotency_key,
    COUNT(DISTINCT o.aggregate_id) AS outbox_payment_id_count,
    ARRAY_AGG(DISTINCT o.aggregate_id ORDER BY o.aggregate_id) AS outbox_payment_ids
FROM outbox_events o
GROUP BY o.idempotency_key
HAVING COUNT(DISTINCT o.aggregate_id) > 1
ORDER BY outbox_payment_id_count DESC, o.idempotency_key;
