package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/domain"
	"flowpay/account-service/internal/dto"
)

type AccountsRepository struct {
	db *sql.DB
}

func NewAccountsRepository(db *sql.DB) *AccountsRepository {
	return &AccountsRepository{db: db}
}

func (r *AccountsRepository) Create(ctx context.Context, tx *sql.Tx, account domain.Account) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO accounts (id, account_name, payment_handle, balance, currency, account_type, allow_negative_balance, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
	`, account.ID, account.AccountName, account.PaymentHandle, account.Balance, account.Currency, account.AccountType, account.AllowNegativeBalance)
	return err
}

func (r *AccountsRepository) FindByID(ctx context.Context, id string) (*domain.Account, error) {
	var a domain.Account
	err := r.db.QueryRowContext(ctx, `
		SELECT id, account_name, payment_handle, balance, currency, account_type, allow_negative_balance, created_at, updated_at
		FROM accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.AccountName, &a.PaymentHandle, &a.Balance, &a.Currency, &a.AccountType, &a.AllowNegativeBalance, &a.CreatedAt, &a.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *AccountsRepository) FindByPaymentHandle(ctx context.Context, paymentHandle string) (*domain.Account, error) {
	var a domain.Account
	err := r.db.QueryRowContext(ctx, `
		SELECT id, account_name, payment_handle, balance, currency, account_type, allow_negative_balance, created_at, updated_at
		FROM accounts WHERE payment_handle = $1
	`, paymentHandle).Scan(&a.ID, &a.AccountName, &a.PaymentHandle, &a.Balance, &a.Currency, &a.AccountType, &a.AllowNegativeBalance, &a.CreatedAt, &a.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *AccountsRepository) ListPaged(ctx context.Context, excludeAccountID, search string, page, pageSize int) ([]dto.AccountListItem, int, error) {
	searchPattern := "%" + search + "%"
	offset := page * pageSize

	var total int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM accounts
		WHERE account_type = 'USER' AND id != $1 AND account_name ILIKE $2
	`, excludeAccountID, searchPattern).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, account_name, payment_handle, currency FROM accounts
		WHERE account_type = 'USER' AND id != $1 AND account_name ILIKE $2
		ORDER BY account_name
		LIMIT $3 OFFSET $4
	`, excludeAccountID, searchPattern, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []dto.AccountListItem{}
	for rows.Next() {
		var item dto.AccountListItem
		if err := rows.Scan(&item.AccountID, &item.AccountName, &item.PaymentHandle, &item.Currency); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}
