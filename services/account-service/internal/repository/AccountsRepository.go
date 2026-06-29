package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/domain"
)

type AccountsRepository struct {
	db *sql.DB
}

func NewAccountsRepository(db *sql.DB) *AccountsRepository {
	return &AccountsRepository{db: db}
}

func (r *AccountsRepository) Create(ctx context.Context, tx *sql.Tx, account domain.Account) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO accounts (id, account_name, balance, currency, account_type, allow_negative_balance, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
	`, account.ID, account.AccountName, account.Balance, account.Currency, account.AccountType, account.AllowNegativeBalance)
	return err
}

func (r *AccountsRepository) FindByID(ctx context.Context, id string) (*domain.Account, error) {
	var a domain.Account
	err := r.db.QueryRowContext(ctx, `
		SELECT id, account_name, balance, currency, account_type, allow_negative_balance, created_at, updated_at
		FROM accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.AccountName, &a.Balance, &a.Currency, &a.AccountType, &a.AllowNegativeBalance, &a.CreatedAt, &a.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}
