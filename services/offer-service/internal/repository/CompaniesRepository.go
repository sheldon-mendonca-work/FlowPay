package repository

import (
	"context"
	"database/sql"
)

type CompanyRepository struct {
	db *sql.DB
}

func NewCompanyRepository(db *sql.DB) *CompanyRepository {
	return &CompanyRepository{
		db: db,
	}
}

func (r *CompanyRepository) GetCompanyNameByID(ctx context.Context, tx *sql.Tx, companyID string) (string, error) {
	var name string
	err := tx.QueryRowContext(ctx, `SELECT name FROM companies WHERE id = $1`, companyID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}
