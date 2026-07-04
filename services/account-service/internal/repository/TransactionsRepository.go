package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/domain"
)

type TransactionsRepository struct {
	db *sql.DB
}

func NewTransactionsRepository(db *sql.DB) *TransactionsRepository {
	return &TransactionsRepository{db: db}
}

func (r *TransactionsRepository) ListPagedByAccountID(ctx context.Context, accountID string, page, pageSize int) ([]domain.Transaction, int, error) {
	offset := page * pageSize

	var total int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM transactions WHERE account_id = $1
	`, accountID).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, payment_id, account_id, type, transaction_category, amount, currency, status, created_at, updated_at
		FROM transactions
		WHERE account_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, accountID, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []domain.Transaction{}
	for rows.Next() {
		var t domain.Transaction
		if err := rows.Scan(&t.ID, &t.PaymentID, &t.AccountID, &t.Type, &t.TransactionCategory, &t.Amount, &t.Currency, &t.Status, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, 0, err
		}
		items = append(items, t)
	}
	return items, total, rows.Err()
}
