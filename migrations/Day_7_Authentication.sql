CREATE TABLE credentials (
    account_id UUID PRIMARY KEY,

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