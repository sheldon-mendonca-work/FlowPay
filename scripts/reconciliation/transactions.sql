-- Transaction consistency checks

-- 1. Payments missing one side of the ledger.
SELECT
    t.payment_id,
    COUNT(*) AS transaction_count,
    COUNT(*) FILTER (WHERE t.type = 'DEBIT') AS debit_count,
    COUNT(*) FILTER (WHERE t.type = 'CREDIT') AS credit_count
FROM transactions t
GROUP BY t.payment_id
HAVING COUNT(*) FILTER (WHERE t.type = 'DEBIT') != 1
OR COUNT(*) FILTER (WHERE t.type = 'CREDIT') != 1;

-- 2. Debit/credit amount or currency imbalance.
SELECT
    debit.payment_id,
    debit.id AS debit_transaction_id,
    credit.id AS credit_transaction_id,
    debit.amount AS debit_amount,
    credit.amount AS credit_amount,
    debit.currency AS debit_currency,
    credit.currency AS credit_currency
FROM transactions debit
JOIN transactions credit
    ON credit.payment_id = debit.payment_id
    AND credit.type = 'CREDIT'
WHERE debit.type = 'DEBIT'
AND (
    debit.amount != credit.amount
    OR debit.currency != credit.currency
);

-- 3. Transactions pointing to missing payments.
SELECT
    t.id AS transaction_id,
    t.payment_id,
    t.type,
    t.amount,
    t.currency,
    t.status
FROM transactions t
WHERE NOT EXISTS (
    SELECT 1
    FROM payments p
    WHERE p.id = t.payment_id
);

-- 4. Transactions whose status differs from their payment status.
SELECT
    t.id AS transaction_id,
    t.payment_id,
    t.type,
    t.status AS transaction_status,
    p.status AS payment_status
FROM transactions t
JOIN payments p
    ON p.id = t.payment_id
WHERE t.status IS DISTINCT FROM p.status;
