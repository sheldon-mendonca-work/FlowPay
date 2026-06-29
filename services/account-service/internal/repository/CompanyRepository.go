package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/domain"
)

type CompanyRepository struct {
	db *sql.DB
}

func NewCompanyRepository(db *sql.DB) *CompanyRepository {
	return &CompanyRepository{db: db}
}

func (r *CompanyRepository) Create(ctx context.Context, tx *sql.Tx, company domain.Company) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO companies (id, name, business_name, email_id, phone_number, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
	`, company.ID, company.Name, company.BusinessName, nullableString(company.EmailID), nullableString(company.PhoneNumber))
	return err
}

func (r *CompanyRepository) FindByID(ctx context.Context, id string) (*domain.Company, error) {
	var c domain.Company
	var emailID, phoneNumber sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, business_name, email_id, phone_number, created_at, updated_at
		FROM companies WHERE id = $1
	`, id).Scan(&c.ID, &c.Name, &c.BusinessName, &emailID, &phoneNumber, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.EmailID = emailID.String
	c.PhoneNumber = phoneNumber.String
	return &c, nil
}

func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
