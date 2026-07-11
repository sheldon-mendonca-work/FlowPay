package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-executor/internal/domain"
	"fmt"
	"strings"
)

type TransactionRepository struct {
	db *sql.DB
}

func NewTransactionRepository(db *sql.DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) CreateTransactions(
	tx *sql.Tx,
	ctx context.Context,
	transactions []domain.Transaction,
) error {

	if len(transactions) == 0 {
		return nil
	}

	var (
		values []string
		args   []any
	)

	for i, t := range transactions {
		base := i*8 + 1

		values = append(values,
			fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,NOW(),NOW())",
				base,
				base+1,
				base+2,
				base+3,
				base+4,
				base+5,
				base+6,
				base+7,
			),
		)

		args = append(args,
			t.ID,
			t.PaymentID,
			t.AccountID,
			t.Type,
			t.TransactionCategory,
			t.Amount,
			t.Currency,
			t.Status,
		)
	}

	query := fmt.Sprintf(`
        INSERT INTO transactions (
            id,
            payment_id,
            account_id,
            type,
            transaction_category,
            amount,
            currency,
            status,
            created_at,
            updated_at
        )
        VALUES %s
    `, strings.Join(values, ","))

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != int64(len(transactions)) {
		return fmt.Errorf(
			"expected %d rows affected but got %d",
			len(transactions),
			rowsAffected,
		)
	}

	return nil
}
