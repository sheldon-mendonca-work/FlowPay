package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
	"fmt"

	"github.com/lib/pq"
)

type AccountRepository struct {
	db *sql.DB
}

func NewAccountRepository(db *sql.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

func (r *AccountRepository) CreateAccount(ctx context.Context, account domain.Account) error {
	query := `
		INSERT INTO accounts (
			id,
			account_name,
			balance,
			allow_negative_balance,
			currency,
			account_type,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW());
	`

	_, err := r.db.ExecContext(ctx,
		query,
		account.ID,
		account.AccountName,
		account.Balance,
		account.AllowNegativeBalance,
		account.Currency,
		account.AccountType,
	)

	if err != nil {
		return err // duplicate will error → good
	}

	return nil
}

func (r *AccountRepository) GetAccountsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderId string, receiverId string) (map[string]domain.Account, error) {
	query := `
		SELECT id, account_name, balance, currency 
		FROM accounts
		WHERE id IN ($1, $2)
		ORDER BY id
		FOR UPDATE;
	`
	rows, err := tx.QueryContext(ctx, query, senderId, receiverId)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	accounts := make(map[string]domain.Account)

	for rows.Next() {
		var acc domain.Account
		err := rows.Scan(
			&acc.ID,
			&acc.AccountName,
			&acc.Balance,
			&acc.Currency,
		)
		if err != nil {
			return nil, err
		}

		accounts[acc.ID] = acc
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accounts, nil
}

func (r *AccountRepository) GetAccountsByIDs(ctx context.Context, ids []string) (map[string]domain.Account, error) {
	if len(ids) == 0 {
		return map[string]domain.Account{}, nil
	}

	query := `
		SELECT id, account_name, payment_handle, balance, currency
		FROM accounts
		WHERE id = ANY($1);
	`
	rows, err := r.db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	accounts := make(map[string]domain.Account)
	for rows.Next() {
		var acc domain.Account
		if err := rows.Scan(&acc.ID, &acc.AccountName, &acc.PaymentHandle, &acc.Balance, &acc.Currency); err != nil {
			return nil, err
		}
		accounts[acc.ID] = acc
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return accounts, nil
}

func (r *AccountRepository) UpdateBalanceForSenderAndReceiver(
	tx *sql.Tx,
	ctx context.Context,
	senderID string,
	receiverID string,
	amount int64,
	allowNegativeBalance bool,
) error {

	// 1. Debit sender (DB enforces balance >= amount)
	debitQuery := `
		UPDATE accounts
		SET balance = balance - $2,
			updated_at = NOW()
		WHERE id = $1 AND (allow_negative_balance = $2 OR balance >= $3);
	`

	debitRes, err := tx.ExecContext(ctx, debitQuery, senderID, allowNegativeBalance, amount)
	if err != nil {
		return err
	}

	debitRows, err := debitRes.RowsAffected()
	if err != nil {
		return err
	}

	if debitRows != 1 {
		return fmt.Errorf("insufficient funds or sender not found: %s", senderID)
	}

	// 2. Credit receiver
	creditQuery := `
		UPDATE accounts
		SET balance = balance + $2,
			updated_at = NOW()
		WHERE id = $1;
	`

	creditRes, err := tx.ExecContext(ctx, creditQuery, receiverID, amount)
	if err != nil {
		return err
	}

	creditRows, err := creditRes.RowsAffected()
	if err != nil {
		return err
	}

	if creditRows != 1 {
		return fmt.Errorf("receiver not found: %s", receiverID)
	}

	return nil
}
