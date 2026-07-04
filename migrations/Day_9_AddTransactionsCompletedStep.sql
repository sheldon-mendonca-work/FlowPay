ALTER TABLE payment_timeline_steps
DROP CONSTRAINT payment_timeline_steps_step_name_check;

ALTER TABLE payment_timeline_steps
ADD CONSTRAINT payment_timeline_steps_step_name_check CHECK (
    step_name IN (
        'PAYMENT_INITIATED',
        'OFFER_EVALUATED',
        'OFFER_RESERVED',
        'PAYMENT_VALIDATED',
        'ACCOUNTS_UPDATED',
        'PAYMENT_PERSISTED',
        'TRANSACTIONS_COMPLETED',
        'OUTBOX_EVENT_CREATED',
        'KAFKA_PUBLISHED',
        'OFFER_REDEEMED',
        'PROMO_POOL_DEBITED',
        'CASHBACK_CREDITED',
        'PAYMENT_COMPLETED'
    )
);
