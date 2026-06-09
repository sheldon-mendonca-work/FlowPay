-- interpretation of offer_type, offer_value, max_benefit_amount:
-- 1. ₹100 off

-- offer_type = FLAT_DISCOUNT
-- offer_value = 100
-- max_benefit_amount = NULL

-- 2. 10% off up to ₹500

-- offer_type = PERCENTAGE_DISCOUNT
-- offer_value = 10
-- max_benefit_amount = 500

-- 3. 5% cashback up to ₹200

-- offer_type = PERCENTAGE_CASHBACK
-- offer_value = 5
-- max_benefit_amount = 200

CREATE TABLE offers (
    id UUID PRIMARY KEY,

    offer_code VARCHAR(100) UNIQUE NOT NULL,

    offer_type VARCHAR(30) NOT NULL CHECK (
        offer_type IN (
            'DISCOUNT',
            'CASHBACK'
        )
    ),

    offer_amount BIGINT,
    offer_percentage INTEGER,
    max_benefit_amount BIGINT,

    minimum_payment_amount BIGINT,
    maximum_payment_amount BIGINT,

    max_redemptions INTEGER NOT NULL,
    redeemed_count INTEGER NOT NULL DEFAULT 0,

    max_redemptions_per_user INTEGER NOT NULL DEFAULT 1,
    idempotency_key VARCHAR(255),
    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'DRAFT',
            'ACTIVE',
            'PAUSED',
            'EXPIRED',
            'DELETED'
        )
    ),

    version BIGINT NOT NULL DEFAULT 1,

    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,

    created_by UUID NOT NULL,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_offer_creation
        FOREIGN KEY (created_by)
        REFERENCES users(id),

    CONSTRAINT fk_idempotency_key
        FOREIGN KEY (idempotency_key)
        REFERENCES offer_idempotency_keys(idempotency_key),

    CONSTRAINT chk_redemption_limit
        CHECK (redeemed_count <= max_redemptions),
    CONSTRAINT chk_offer_amount CHECK (
        (offer_amount IS NOT NULL AND offer_percentage IS NULL)
        OR
        (offer_amount IS NULL AND offer_percentage IS NOT NULL)
    ),
    CONSTRAINT chk_offer_window
        CHECK (start_time < end_time)
);

CREATE TABLE companies (
    id UUID PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    business_name VARCHAR(50) NOT NULL,
    email_id VARCHAR(50),
    phone_number VARCHAR(20),

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
    
);

CREATE TABLE users (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL,
    company_id UUID,
    role VARCHAR(20) NOT NULL CHECK (role IN ('ADMIN','USER')),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_account_id FOREIGN KEY(account_id) REFERENCES accounts(id),
    CONSTRAINT fk_company_id FOREIGN KEY(company_id) REFERENCES companies(id)
);

CREATE TABLE offer_redemptions (
    id UUID PRIMARY KEY,

    offer_id UUID NOT NULL,
    account_id UUID NOT NULL,

    payment_id UUID,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,

    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'IN_PROGRESS',
            'SUCCESS',
            'FAILED',
            'REFUNDED'
        )
    ),

    refund_reason TEXT,

    refunded_by_id UUID,
    refunded_by_type VARCHAR(20) CHECK (
        refunded_by_type IN (
            'ACCOUNT',
            'USER',
            'SYSTEM'
        )
    ),

    refund_source VARCHAR(30) CHECK (
        refund_source IN (
            'CUSTOMER_REQUEST',
            'FRAUD',
            'MANUAL_ADMIN',
            'SYSTEM'
        )
    ),

    created_at TIMESTAMP NOT NULL,
    redeemed_at TIMESTAMP,
    refunded_at TIMESTAMP,

    CONSTRAINT fk_offer_id
        FOREIGN KEY (offer_id) REFERENCES offers(id),

    CONSTRAINT fk_account_id
        FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE offer_events (
    id UUID PRIMARY KEY,

    offer_id UUID NOT NULL,

    event_type VARCHAR(50) NOT NULL,

    actor_id UUID,

    actor_type VARCHAR(20) NOT NULL CHECK (
        actor_type IN (
            'USER',
            'ACCOUNT',
            'SYSTEM'
        )
    ),

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_offer_event_offer
        FOREIGN KEY (offer_id)
        REFERENCES offers(id)
);

CREATE INDEX idx_offer_events_offer_id_created_at
ON offer_events(offer_id, created_at DESC);

CREATE TABLE offer_idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,

    offer_id UUID,

    request_hash VARCHAR(255) NOT NULL,

    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'IN_PROGRESS',
            'COMPLETED',
            'FAILED'
        )
    ),

    response_body JSONB,
    error_code TEXT,
    error_message TEXT,
    
    owner_token VARCHAR(255),

    locked_until TIMESTAMP NOT NULL,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE offer_reservations (
    id UUID PRIMARY KEY,

    offer_id UUID NOT NULL,
    account_id UUID NOT NULL,

    payment_id UUID,

    status VARCHAR(20) NOT NULL CHECK (
        status IN (
            'RESERVED',
            'REDEEMED',
            'CANCELLED',
            'EXPIRED'
        )
    ),

    expires_at TIMESTAMP NOT NULL,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_offer_reservation_offer
        FOREIGN KEY (offer_id)
        REFERENCES offers(id),

    CONSTRAINT fk_offer_reservation_account
        FOREIGN KEY (account_id)
        REFERENCES accounts(id)
);

CREATE INDEX idx_offer_reservations_offer_status
ON offer_reservations (offer_id, status);

CREATE INDEX idx_offer_reservations_account
ON offer_reservations (account_id);

CREATE INDEX idx_offer_reservations_payment
ON offer_reservations (payment_id);

CREATE INDEX idx_offer_reservations_expires_at
ON offer_reservations (expires_at);