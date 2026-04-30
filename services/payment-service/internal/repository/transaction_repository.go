package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
	"fmt"
)

type TransactionRepository struct {
	db *sql.DB
}

func NewTransactionRepository(db *sql.DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) CreateTransaction(ctx context.Context, transaction domain.Transaction) error {
	query := `
		INSERT INTO transactions (
			id,
			payment_id,
			account_id,
			type,
			amount,
			currency,
			status,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW());
	`

	res, err := r.db.ExecContext(ctx,
		query,
		transaction.ID,
		transaction.PaymentID,
		transaction.AccountID,
		transaction.Type,
		transaction.Amount,
		transaction.Currency,
		transaction.Status,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Transaction creation failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}

func (r *TransactionRepository) CreateTransactionsForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderTransaction domain.Transaction, receiverTransaction domain.Transaction) error {
	query :=
		`
		INSERT INTO transactions (
			id,
			payment_id,
			account_id,
			type,
			amount,
			currency,
			status,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW()), ($8, $9, $10, $11, $12, $13, $14, NOW(), NOW());
	`

	res, err := tx.ExecContext(ctx, query,
		senderTransaction.ID,
		senderTransaction.PaymentID,
		senderTransaction.AccountID,
		senderTransaction.Type,
		senderTransaction.Amount,
		senderTransaction.Currency,
		senderTransaction.Status,
		receiverTransaction.ID,
		receiverTransaction.PaymentID,
		receiverTransaction.AccountID,
		receiverTransaction.Type,
		receiverTransaction.Amount,
		receiverTransaction.Currency,
		receiverTransaction.Status,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 2 {
		return fmt.Errorf("Transaction creation failed: expected 2 rows affected, but got %d", rowsAffected)
	}
	return nil
}
