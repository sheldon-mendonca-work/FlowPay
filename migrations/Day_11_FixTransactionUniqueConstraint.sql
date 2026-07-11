-- Day 11: fix transactions uniqueness for cashback payments.
--
-- unique_payment_type (payment_id, type) assumed exactly one DEBIT and one
-- CREDIT row per payment. A cashback payment inserts four rows for the same
-- payment_id: sender DEBIT / receiver CREDIT (category PAYMENT), plus a
-- promotion-pool DEBIT and a sender CREDIT (category CASHBACK) - the second
-- DEBIT and second CREDIT collide with the first pair and the whole batch
-- insert fails.
--
-- Replace it with a constraint scoped to the actual duplicate we want to
-- prevent: the same account being debited/credited twice for the same
-- payment under the same category.
-- unique_payment_type was created as a bare unique index (Day_2), not a
-- named table constraint, so it must be dropped as an index.
DROP INDEX IF EXISTS unique_payment_type;

CREATE UNIQUE INDEX unique_payment_account_type_category
ON transactions (payment_id, account_id, type, transaction_category);
