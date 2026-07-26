CREATE TABLE credentials (
    account_id UUID PRIMARY KEY,

    email TEXT NOT NULL UNIQUE,

    password_hash TEXT NOT NULL,

    password_updated_at TIMESTAMP NOT NULL,

    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_account_id
        FOREIGN KEY(account_id)
        REFERENCES accounts(id)
);

CREATE TABLE refresh_tokens (
    id UUID PRIMARY KEY,

    account_id UUID NOT NULL,

    token_hash TEXT NOT NULL,

    expires_at TIMESTAMP NOT NULL,

    created_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_account_id
        FOREIGN KEY(account_id)
        REFERENCES accounts(id)
);

CREATE TABLE defaultcredentials (
    account_id UUID PRIMARY KEY,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_account_id
        FOREIGN KEY(account_id)
        REFERENCES accounts(id)
);


ALTER TABLE accounts
ADD COLUMN payment_handle VARCHAR(50);

UPDATE accounts
SET payment_handle = 'rahul@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000001';

UPDATE accounts
SET payment_handle = 'priya@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000002';

UPDATE accounts
SET payment_handle = 'amit@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000003';

UPDATE accounts
SET payment_handle = 'sneha@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000004';

UPDATE accounts
SET payment_handle = 'vikram@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000005';

UPDATE accounts
SET payment_handle = 'neha@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000006';

UPDATE accounts
SET payment_handle = 'arjun@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000007';

UPDATE accounts
SET payment_handle = 'kavya@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000008';

UPDATE accounts
SET payment_handle = 'rohan@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000009';

UPDATE accounts
SET payment_handle = 'meera@flowpay'
WHERE id = 'a1000000-0000-0000-0000-000000000010';

UPDATE accounts
SET payment_handle = 'amazon_amz100@flowpay'
WHERE id = 'b1000000-0000-0000-0000-000000000001';

UPDATE accounts
SET payment_handle = 'amazon_amz20@flowpay'
WHERE id = 'b1000000-0000-0000-0000-000000000002';

UPDATE accounts
SET payment_handle = 'flipkart_flip15@flowpay'
WHERE id = 'b1000000-0000-0000-0000-000000000003';

UPDATE accounts
SET payment_handle = 'flipkart_flip200@flowpay'
WHERE id = 'b1000000-0000-0000-0000-000000000004';


ALTER TABLE accounts
ALTER COLUMN payment_handle SET NOT NULL;

ALTER TABLE accounts
ADD CONSTRAINT accounts_payment_handle_unique
UNIQUE (payment_handle);