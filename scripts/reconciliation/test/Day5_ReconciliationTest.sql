-- Day5_ReconciliationTest.sql
--
-- Seeds deterministic reconciliation anomalies for every query in:
--   scripts/reconciliation/payments.sql
--   scripts/reconciliation/transactions.sql
--   scripts/reconciliation/idempotency.sql
--   scripts/reconciliation/outbox.sql
--
-- Note:
-- Some reconciliation cases intentionally violate foreign-key expectations
-- (for example, transactions pointing to missing payments). Run this as a
-- local/test DB superuser such as postgres, because it temporarily disables
-- FK triggers with session_replication_role.

BEGIN;

SET LOCAL session_replication_role = replica;

-- Clean old Day 5 reconciliation fixtures.
DELETE FROM transactions
WHERE id IN (
    '50000000-0000-0000-0000-000000000101',
    '50000000-0000-0000-0000-000000000102',
    '50000000-0000-0000-0000-000000000201',
    '50000000-0000-0000-0000-000000000202',
    '50000000-0000-0000-0000-000000000301',
    '50000000-0000-0000-0000-000000000302',
    '50000000-0000-0000-0000-000000000401',
    '50000000-0000-0000-0000-000000000402',
    '50000000-0000-0000-0000-000000000501',
    '50000000-0000-0000-0000-000000000601',
    '50000000-0000-0000-0000-000000000602'
);

DELETE FROM outbox_events
WHERE idempotency_key LIKE 'recon-day5-%'
OR id IN (
    '60000000-0000-0000-0000-000000000101',
    '60000000-0000-0000-0000-000000000201',
    '60000000-0000-0000-0000-000000000301',
    '60000000-0000-0000-0000-000000000401',
    '60000000-0000-0000-0000-000000000501',
    '60000000-0000-0000-0000-000000000502',
    '60000000-0000-0000-0000-000000000601',
    '60000000-0000-0000-0000-000000000701',
    '60000000-0000-0000-0000-000000000801',
    '60000000-0000-0000-0000-000000000802'
);

DELETE FROM payments
WHERE id IN (
    '10000000-0000-0000-0000-000000000101',
    '10000000-0000-0000-0000-000000000201',
    '10000000-0000-0000-0000-000000000301',
    '10000000-0000-0000-0000-000000000401',
    '10000000-0000-0000-0000-000000000402',
    '10000000-0000-0000-0000-000000000501',
    '10000000-0000-0000-0000-000000000601',
    '10000000-0000-0000-0000-000000000701',
    '10000000-0000-0000-0000-000000000801',
    '10000000-0000-0000-0000-000000000901'
);

DELETE FROM idempotency_keys
WHERE idempotency_key LIKE 'recon-day5-%';

DELETE FROM accounts
WHERE id IN (
    '20000000-0000-0000-0000-000000000001',
    '20000000-0000-0000-0000-000000000002',
    '20000000-0000-0000-0000-000000000003',
    '20000000-0000-0000-0000-000000000004'
);

-- Shared accounts for payment and transaction rows.
INSERT INTO accounts (id, user_id, balance, currency, created_at, updated_at)
VALUES
    ('20000000-0000-0000-0000-000000000001', 'recon-day5-sender-a', 1000000, 'INR', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
    ('20000000-0000-0000-0000-000000000002', 'recon-day5-receiver-a', 1000000, 'INR', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
    ('20000000-0000-0000-0000-000000000003', 'recon-day5-sender-b', 1000000, 'USD', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day'),
    ('20000000-0000-0000-0000-000000000004', 'recon-day5-receiver-b', 1000000, 'USD', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')
ON CONFLICT (id) DO NOTHING;

-- Baseline good payment. Useful as an idempotency/outbox anchor.
INSERT INTO payments (
    id,
    idempotency_key,
    sender_id,
    receiver_id,
    amount,
    currency,
    status,
    created_at,
    updated_at
)
VALUES
    (
        '10000000-0000-0000-0000-000000000101',
        'recon-day5-good-payment',
        '20000000-0000-0000-0000-000000000001',
        '20000000-0000-0000-0000-000000000002',
        1000,
        'INR',
        'SUCCESS',
        NOW() - INTERVAL '2 hours',
        NOW() - INTERVAL '2 hours'
    );

INSERT INTO idempotency_keys (
    idempotency_key,
    request_hash,
    response_body,
    status,
    owner_token,
    payment_id,
    locked_until,
    created_at,
    updated_at
)
VALUES
    (
        'recon-day5-good-payment',
        'hash-good-payment',
        '{"payment_id":"10000000-0000-0000-0000-000000000101"}',
        'COMPLETED',
        'recon-day5-owner',
        '10000000-0000-0000-0000-000000000101',
        NOW() + INTERVAL '10 minutes',
        NOW() - INTERVAL '2 hours',
        NOW() - INTERVAL '2 hours'
    );

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000101', '10000000-0000-0000-0000-000000000101', '20000000-0000-0000-0000-000000000001', 'DEBIT', 1000, 'INR', 'SUCCESS', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'),
    ('50000000-0000-0000-0000-000000000102', '10000000-0000-0000-0000-000000000101', '20000000-0000-0000-0000-000000000002', 'CREDIT', 1000, 'INR', 'SUCCESS', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours');

-- payments.sql #1, #2:
-- Payment without any ledger entries and successful payment without debit/credit pair.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000201', 'recon-day5-payment-no-transactions', '20000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000002', 1500, 'INR', 'SUCCESS', NOW() - INTERVAL '90 minutes', NOW() - INTERVAL '90 minutes');

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-payment-no-transactions', 'hash-payment-no-transactions', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000201', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '90 minutes', NOW() - INTERVAL '90 minutes');

-- payments.sql #2 and transactions.sql #1:
-- Successful payment with only a DEBIT transaction.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000301', 'recon-day5-payment-debit-only', '20000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000002', 2000, 'INR', 'SUCCESS', NOW() - INTERVAL '80 minutes', NOW() - INTERVAL '80 minutes');

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-payment-debit-only', 'hash-payment-debit-only', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000301', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '80 minutes', NOW() - INTERVAL '80 minutes');

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000201', '10000000-0000-0000-0000-000000000301', '20000000-0000-0000-0000-000000000001', 'DEBIT', 2000, 'INR', 'SUCCESS', NOW() - INTERVAL '80 minutes', NOW() - INTERVAL '80 minutes');

-- payments.sql #3 and idempotency.sql #2:
-- Payment missing an idempotency row.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000401', 'recon-day5-payment-missing-idempotency', '20000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000002', 2500, 'INR', 'SUCCESS', NOW() - INTERVAL '70 minutes', NOW() - INTERVAL '70 minutes');

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000301', '10000000-0000-0000-0000-000000000401', '20000000-0000-0000-0000-000000000001', 'DEBIT', 2500, 'INR', 'SUCCESS', NOW() - INTERVAL '70 minutes', NOW() - INTERVAL '70 minutes');

-- payments.sql #4 and idempotency.sql #2:
-- Payment whose idempotency row points at a different payment_id.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000402', 'recon-day5-payment-idempotency-mismatch', '20000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000002', 2600, 'INR', 'SUCCESS', NOW() - INTERVAL '65 minutes', NOW() - INTERVAL '65 minutes');

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-payment-idempotency-mismatch', 'hash-payment-idempotency-mismatch', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '65 minutes', NOW() - INTERVAL '65 minutes');

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000401', '10000000-0000-0000-0000-000000000402', '20000000-0000-0000-0000-000000000001', 'DEBIT', 2600, 'INR', 'SUCCESS', NOW() - INTERVAL '65 minutes', NOW() - INTERVAL '65 minutes'),
    ('50000000-0000-0000-0000-000000000402', '10000000-0000-0000-0000-000000000402', '20000000-0000-0000-0000-000000000002', 'CREDIT', 2600, 'INR', 'SUCCESS', NOW() - INTERVAL '65 minutes', NOW() - INTERVAL '65 minutes');

-- transactions.sql #2:
-- Debit/credit imbalance for amount and currency.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000501', 'recon-day5-transaction-imbalance', '20000000-0000-0000-0000-000000000003', '20000000-0000-0000-0000-000000000004', 3000, 'USD', 'SUCCESS', NOW() - INTERVAL '60 minutes', NOW() - INTERVAL '60 minutes');

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-transaction-imbalance', 'hash-transaction-imbalance', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000501', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '60 minutes', NOW() - INTERVAL '60 minutes');

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000501', '10000000-0000-0000-0000-000000000501', '20000000-0000-0000-0000-000000000003', 'DEBIT', 3000, 'USD', 'SUCCESS', NOW() - INTERVAL '60 minutes', NOW() - INTERVAL '60 minutes'),
    ('50000000-0000-0000-0000-000000000202', '10000000-0000-0000-0000-000000000501', '20000000-0000-0000-0000-000000000004', 'CREDIT', 3500, 'EUR', 'SUCCESS', NOW() - INTERVAL '60 minutes', NOW() - INTERVAL '60 minutes');

-- transactions.sql #3:
-- Transaction pointing to a missing payment.
INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000302', '10000000-0000-0000-0000-000000009999', '20000000-0000-0000-0000-000000000001', 'DEBIT', 1000, 'INR', 'SUCCESS', NOW() - INTERVAL '55 minutes', NOW() - INTERVAL '55 minutes');

-- transactions.sql #4:
-- Transaction status differs from payment status.
INSERT INTO payments (id, idempotency_key, sender_id, receiver_id, amount, currency, status, created_at, updated_at)
VALUES
    ('10000000-0000-0000-0000-000000000601', 'recon-day5-transaction-status-mismatch', '20000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000002', 4000, 'INR', 'SUCCESS', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes');

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-transaction-status-mismatch', 'hash-transaction-status-mismatch', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000601', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes');

INSERT INTO transactions (id, payment_id, account_id, type, amount, currency, status, created_at, updated_at)
VALUES
    ('50000000-0000-0000-0000-000000000601', '10000000-0000-0000-0000-000000000601', '20000000-0000-0000-0000-000000000001', 'DEBIT', 4000, 'INR', 'FAILED', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes'),
    ('50000000-0000-0000-0000-000000000602', '10000000-0000-0000-0000-000000000601', '20000000-0000-0000-0000-000000000002', 'CREDIT', 4000, 'INR', 'SUCCESS', NOW() - INTERVAL '50 minutes', NOW() - INTERVAL '50 minutes');

-- idempotency.sql #1:
-- Completed idempotency row without matching payment.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-completed-missing-payment', 'hash-completed-missing-payment', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000008001', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '45 minutes', NOW() - INTERVAL '45 minutes');

-- idempotency.sql #3:
-- Idempotency row missing canonical payment_id.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-null-payment-id', 'hash-null-payment-id', '{}', 'COMPLETED', 'recon-day5-owner', NULL, NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '44 minutes', NOW() - INTERVAL '44 minutes');

-- idempotency.sql #4:
-- Duplicate canonical payment_id across idempotency rows.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-duplicate-payment-a', 'hash-duplicate-payment-a', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '43 minutes', NOW() - INTERVAL '43 minutes'),
    ('recon-day5-duplicate-payment-b', 'hash-duplicate-payment-b', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '42 minutes', NOW() - INTERVAL '42 minutes');

-- idempotency.sql #5:
-- Expired IN_PROGRESS takeover candidate.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-expired-in-progress', 'hash-expired-in-progress', NULL, 'IN_PROGRESS', 'recon-day5-stale-owner', '10000000-0000-0000-0000-000000000101', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '40 minutes', NOW() - INTERVAL '30 minutes');

-- idempotency.sql #6:
-- IN_PROGRESS row with no outbox event.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-in-progress-no-outbox', 'hash-in-progress-no-outbox', NULL, 'IN_PROGRESS', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '30 minutes', NOW() - INTERVAL '39 minutes', NOW() - INTERVAL '39 minutes');

-- idempotency.sql #7:
-- Idempotency/outbox payment_id divergence.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-idem-outbox-divergence', 'hash-idem-outbox-divergence', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '38 minutes', NOW() - INTERVAL '38 minutes');

INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000101', 'payment', '10000000-0000-0000-0000-000000000201', 'PaymentInitiated', 1, 'recon-day5-idem-outbox-divergence', '{}', 'PUBLISHED', NULL, 0, 'trace-recon-day5-divergence', 'request-recon-day5-divergence', NULL, NULL, NOW() - INTERVAL '38 minutes', NOW() - INTERVAL '38 minutes', NOW() - INTERVAL '37 minutes');

-- idempotency.sql #8:
-- Multiple outbox payment IDs for one idempotency key.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-multiple-outbox-ids', 'hash-multiple-outbox-ids', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '37 minutes', NOW() - INTERVAL '37 minutes');

INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000201', 'payment', '10000000-0000-0000-0000-000000000101', 'PaymentInitiated', 1, 'recon-day5-multiple-outbox-ids', '{}', 'PUBLISHED', NULL, 0, 'trace-recon-day5-multi-a', 'request-recon-day5-multi-a', NULL, NULL, NOW() - INTERVAL '37 minutes', NOW() - INTERVAL '37 minutes', NOW() - INTERVAL '36 minutes'),
    ('60000000-0000-0000-0000-000000000301', 'payment', '10000000-0000-0000-0000-000000000201', 'PaymentInitiated', 1, 'recon-day5-multiple-outbox-ids', '{}', 'PUBLISHED', NULL, 0, 'trace-recon-day5-multi-b', 'request-recon-day5-multi-b', NULL, NULL, NOW() - INTERVAL '36 minutes', NOW() - INTERVAL '36 minutes', NOW() - INTERVAL '35 minutes');

-- outbox.sql #1 and #3:
-- Stuck PROCESSING event whose lease has expired and is eligible for claiming.
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000401', 'payment', '10000000-0000-0000-0000-000000000101', 'PaymentInitiated', 1, 'recon-day5-stuck-processing', '{}', 'PROCESSING', NOW() - INTERVAL '20 minutes', 2, 'trace-recon-day5-stuck', 'request-recon-day5-stuck', NULL, NULL, NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '20 minutes', NULL);

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-stuck-processing', 'hash-stuck-processing', '{}', 'IN_PROGRESS', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '30 minutes', NOW() - INTERVAL '20 minutes');

-- outbox.sql #2 and #4:
-- Failed event after retry exhaustion.
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000501', 'payment', '10000000-0000-0000-0000-000000000101', 'PaymentInitiated', 1, 'recon-day5-failed-outbox', '{}', 'FAILED', NULL, 5, 'trace-recon-day5-failed', 'request-recon-day5-failed', 'KAFKA_PUBLISH_FAILURE', 'exhausted retries', NOW() - INTERVAL '25 minutes', NOW() - INTERVAL '10 minutes', NULL);

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-failed-outbox', 'hash-failed-outbox', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '25 minutes', NOW() - INTERVAL '10 minutes');

-- outbox.sql #3 and #4:
-- Pending backlog eligible for claiming now.
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000502', 'payment', '10000000-0000-0000-0000-000000000101', 'PaymentInitiated', 1, 'recon-day5-pending-outbox', '{}', 'PENDING', NULL, 1, 'trace-recon-day5-pending', 'request-recon-day5-pending', NULL, NULL, NOW() - INTERVAL '15 minutes', NOW() - INTERVAL '15 minutes', NULL);

INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-pending-outbox', 'hash-pending-outbox', '{}', 'IN_PROGRESS', 'recon-day5-owner', '10000000-0000-0000-0000-000000000101', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '15 minutes', NOW() - INTERVAL '15 minutes');

-- outbox.sql #5:
-- Outbox event without idempotency row.
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000601', 'payment', '10000000-0000-0000-0000-000000000101', 'PaymentInitiated', 1, 'recon-day5-outbox-missing-idempotency', '{}', 'PENDING', NULL, 0, 'trace-recon-day5-missing-idem', 'request-recon-day5-missing-idem', NULL, NULL, NOW() - INTERVAL '14 minutes', NOW() - INTERVAL '14 minutes', NULL);

-- outbox.sql #6:
-- Published outbox event whose aggregate payment does not exist.
INSERT INTO idempotency_keys (idempotency_key, request_hash, response_body, status, owner_token, payment_id, locked_until, created_at, updated_at)
VALUES
    ('recon-day5-published-missing-payment', 'hash-published-missing-payment', '{}', 'COMPLETED', 'recon-day5-owner', '10000000-0000-0000-0000-000000007001', NOW() + INTERVAL '10 minutes', NOW() - INTERVAL '13 minutes', NOW() - INTERVAL '13 minutes');

INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, idempotency_key, payload, status, locked_until, retry_count, trace_id, request_id, error_code, error_message, created_at, updated_at, published_at)
VALUES
    ('60000000-0000-0000-0000-000000000701', 'payment', '10000000-0000-0000-0000-000000007001', 'PaymentInitiated', 1, 'recon-day5-published-missing-payment', '{}', 'PUBLISHED', NULL, 0, 'trace-recon-day5-published-missing-payment', 'request-recon-day5-published-missing-payment', NULL, NULL, NOW() - INTERVAL '13 minutes', NOW() - INTERVAL '13 minutes', NOW() - INTERVAL '12 minutes');

COMMIT;
