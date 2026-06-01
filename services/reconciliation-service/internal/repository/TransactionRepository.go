package repository

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/domain"
)

type TransactionRepository struct {
	db *sql.DB
}

func NewTransactionRepository(db *sql.DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) GetTransactionsMissingLedgerSide(ctx context.Context, tx *sql.Tx) ([]domain.TransactionLedgerCounts, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			t.payment_id::text,
			COUNT(*) AS transaction_count,
			COUNT(*) FILTER (WHERE t.type = 'DEBIT') AS debit_count,
			COUNT(*) FILTER (WHERE t.type = 'CREDIT') AS credit_count
		FROM transactions t
		GROUP BY t.payment_id
		HAVING COUNT(*) FILTER (WHERE t.type = 'DEBIT') != 1
		OR COUNT(*) FILTER (WHERE t.type = 'CREDIT') != 1;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ledgers []domain.TransactionLedgerCounts
	for rows.Next() {
		var ledger domain.TransactionLedgerCounts
		if err := rows.Scan(&ledger.PaymentID, &ledger.TransactionCount, &ledger.DebitCount, &ledger.CreditCount); err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, rows.Err()
}

func (r *TransactionRepository) GetTransactionsAmountCurrencyImbalance(ctx context.Context, tx *sql.Tx) ([]domain.TransactionAmountCurrencyImbalance, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			debit.payment_id::text,
			debit.id::text AS debit_transaction_id,
			credit.id::text AS credit_transaction_id,
			debit.amount AS debit_amount,
			credit.amount AS credit_amount,
			debit.currency AS debit_currency,
			credit.currency AS credit_currency
		FROM transactions debit
		JOIN transactions credit
			ON credit.payment_id = debit.payment_id
			AND credit.type = 'CREDIT'
		WHERE debit.type = 'DEBIT'
		AND (
			debit.amount != credit.amount
			OR debit.currency != credit.currency
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var imbalances []domain.TransactionAmountCurrencyImbalance
	for rows.Next() {
		var imbalance domain.TransactionAmountCurrencyImbalance
		if err := rows.Scan(&imbalance.PaymentID, &imbalance.DebitTransactionID, &imbalance.CreditTransactionID, &imbalance.DebitAmount, &imbalance.CreditAmount, &imbalance.DebitCurrency, &imbalance.CreditCurrency); err != nil {
			return nil, err
		}
		imbalances = append(imbalances, imbalance)
	}
	return imbalances, rows.Err()
}

func (r *TransactionRepository) GetTransactionsWithoutPayments(ctx context.Context, tx *sql.Tx) ([]domain.TransactionWithoutPayment, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT t.id::text, t.payment_id::text, t.type, t.amount, t.currency, t.status
		FROM transactions t
		WHERE NOT EXISTS (
			SELECT 1
			FROM payments p
			WHERE p.id = t.payment_id
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []domain.TransactionWithoutPayment
	for rows.Next() {
		var transaction domain.TransactionWithoutPayment
		if err := rows.Scan(&transaction.ID, &transaction.PaymentID, &transaction.Type, &transaction.Amount, &transaction.Currency, &transaction.Status); err != nil {
			return nil, err
		}
		transactions = append(transactions, transaction)
	}
	return transactions, rows.Err()
}

func (r *TransactionRepository) GetTransactionsStatusMismatch(ctx context.Context, tx *sql.Tx) ([]domain.TransactionStatusMismatch, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT t.id::text, t.payment_id::text, t.type, t.status AS transaction_status, p.status AS payment_status
		FROM transactions t
		JOIN payments p
			ON p.id = t.payment_id
		WHERE t.status IS DISTINCT FROM p.status;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mismatches []domain.TransactionStatusMismatch
	for rows.Next() {
		var mismatch domain.TransactionStatusMismatch
		if err := rows.Scan(&mismatch.ID, &mismatch.PaymentID, &mismatch.Type, &mismatch.TransactionStatus, &mismatch.PaymentStatus); err != nil {
			return nil, err
		}
		mismatches = append(mismatches, mismatch)
	}
	return mismatches, rows.Err()
}
