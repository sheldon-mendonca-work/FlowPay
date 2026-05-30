-- Payment consistency checks

-- 1. Payments without ledger entries in transactions.
SELECT
    p.id AS payment_id,
    p.idempotency_key,
    p.status,
    p.created_at
FROM payments p
WHERE NOT EXISTS (
    SELECT 1
    FROM transactions t
    WHERE t.payment_id = p.id
);

-- 2. Successful payments without both debit and credit transactions.
SELECT
    p.id AS payment_id,
    p.idempotency_key,
    p.status,
    COUNT(t.id) AS transaction_count,
    COUNT(*) FILTER (WHERE t.type = 'DEBIT') AS debit_count,
    COUNT(*) FILTER (WHERE t.type = 'CREDIT') AS credit_count
FROM payments p
LEFT JOIN transactions t
    ON t.payment_id = p.id
WHERE p.status = 'SUCCESS'
GROUP BY p.id, p.idempotency_key, p.status
HAVING COUNT(*) FILTER (WHERE t.type = 'DEBIT') != 1
OR COUNT(*) FILTER (WHERE t.type = 'CREDIT') != 1;

-- 3. Payments without idempotency rows.
SELECT
    p.id AS payment_id,
    p.idempotency_key,
    p.status,
    p.created_at
FROM payments p
WHERE NOT EXISTS (
    SELECT 1
    FROM idempotency_keys i
    WHERE i.idempotency_key = p.idempotency_key
);

-- 4. Payments whose idempotency row points at a different payment_id.
SELECT
    p.id AS payment_id,
    p.idempotency_key,
    i.payment_id AS idempotency_payment_id,
    i.status AS idempotency_status
FROM payments p
JOIN idempotency_keys i
    ON i.idempotency_key = p.idempotency_key
WHERE i.payment_id IS DISTINCT FROM p.id;
