-- Day 10: support for the Payment Details page read APIs.

-- Bugfix: this constraint was added by mistake in Day_6 (attached to the
-- `status` column while adding the `offer_code` column). It requires status
-- to be one of ('NONE','RESERVED','REDEEMED','EXPIRED'), which directly
-- contradicts chk_payment_status ('PENDING','SUCCESS','FAILED'). Since the
-- two checks can never both hold, no row can currently be inserted into
-- payments at all.
ALTER TABLE payments
DROP CONSTRAINT IF EXISTS payments_status_check;

ALTER TABLE payments
ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'WALLET',
ADD COLUMN completed_at TIMESTAMP;

UPDATE payments
SET completed_at = updated_at
WHERE status = 'SUCCESS' AND completed_at IS NULL;

CREATE INDEX idx_payment_timeline_steps_payment_id
ON payment_timeline_steps(payment_id);
