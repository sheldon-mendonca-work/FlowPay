package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/domain"
)

type UserRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, tx *sql.Tx, user domain.User) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO users (id, account_id, company_id, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
	`, user.ID, user.AccountID, nullableString(user.CompanyID), user.Role)
	return err
}

func (r *UserRepository) FindByAccountID(ctx context.Context, accountID string) (*domain.User, error) {
	var u domain.User
	var companyID sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, account_id, company_id, role, created_at, updated_at
		FROM users WHERE account_id = $1
	`, accountID).Scan(&u.ID, &u.AccountID, &companyID, &u.Role, &u.CreatedAt, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	u.CompanyID = companyID.String
	return &u, nil
}
