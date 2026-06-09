package repository

import (
	"context"
	"database/sql"
	"errors"
	"flowpay/offer-service/internal/domain"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
)

type UserRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{
		db: db,
	}
}

func (r *UserRepository) CreateUser(ctx context.Context, user domain.UsersEntity) error {
	query := `
		INSERT INTO users (
			id,
			company_id,
			account_id,
			role,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, NOW(), NOW());
	`

	_, err := r.db.ExecContext(ctx,
		query,
		user.ID,
		user.CompanyID,
		user.AccountID,
		user.Role,
	)

	if err != nil {
		return err // duplicate will error → good
	}

	return nil
}

func (r *UserRepository) GetUsersByUserAndCompanyId(ctx context.Context, tx *sql.Tx, companyId string, userId string) (domain.UsersEntity, error) {
	query := `
		SELECT id, account_id, company_id, role 
		FROM users
		WHERE id=$1 AND company_id=$2
		ORDER BY id
		FOR UPDATE;
	`
	var acc domain.UsersEntity
	err := tx.QueryRowContext(ctx, query, userId, companyId).Scan(
		&acc.ID,
		&acc.AccountID,
		&acc.CompanyID,
		&acc.Role,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.UsersEntity{}, flowpayOfferErrors.ErrUserNotFound // or ErrUserDoesNotExist
		}
		return domain.UsersEntity{}, err
	}

	return acc, nil
}
