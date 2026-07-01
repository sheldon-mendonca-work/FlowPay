package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
)

type AccountsRepository struct {
	db *sql.DB
}

func NewAccountsRepository(db *sql.DB) *AccountsRepository {
	return &AccountsRepository{
		db: db,
	}
}

func (r *AccountsRepository) CreateNewAccountForOffer(ctx context.Context, tx *sql.Tx, account domain.Account) error {
	query := `
		INSERT INTO accounts (
			id,
			account_name,
			payment_handle,
			balance,
			currency,
			account_type,
			allow_negative_balance,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW());
	`

	_, err := tx.ExecContext(ctx,
		query,
		account.ID,
		account.AccountName,
		account.PaymentHandle,
		account.Balance,
		account.Currency,
		account.AccountType,
		account.AllowNegativeBalance,
	)

	if err != nil {
		return err // duplicate will error → good
	}

	return nil
}
