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
