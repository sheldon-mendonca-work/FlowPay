package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
	"fmt"
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
			user_id,
			balance,
			currency,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, NOW(), NOW());
	`

	_, err := r.db.ExecContext(ctx,
		query,
		account.ID,
		account.UserID,
		account.Balance,
		account.Currency,
	)

	if err != nil {
		return err // duplicate will error → good
	}

	return nil
}

func (r *AccountRepository) GetAccountsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderId string, receiverId string) (map[string]domain.Account, error) {
	query := `
		SELECT id, user_id, balance, currency 
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
			&acc.UserID,
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

func (r *AccountRepository) UpdateBalanceForSenderAndReceiver(
	tx *sql.Tx,
	ctx context.Context,
	senderID string,
	receiverID string,
	amount int64,
) error {

	// 1. Debit sender (DB enforces balance >= amount)
	debitQuery := `
		UPDATE accounts
		SET balance = balance - $2,
			updated_at = NOW()
		WHERE id = $1 AND balance >= $2;
	`

	debitRes, err := tx.ExecContext(ctx, debitQuery, senderID, amount)
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
