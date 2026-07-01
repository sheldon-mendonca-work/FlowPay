package repository

import (
	"context"
	"database/sql"

	"flowpay/account-service/internal/dto"
)

type DefaultCredentialsRepository struct {
	db *sql.DB
}

func NewDefaultCredentialsRepository(db *sql.DB) *DefaultCredentialsRepository {
	return &DefaultCredentialsRepository{db: db}
}

func (r *DefaultCredentialsRepository) ListAccounts(ctx context.Context) ([]dto.DefaultAccountItem, error) {
	const query = `
		SELECT a.id, a.account_name, a.payment_handle, a.currency,
		       CASE WHEN u.id IS NOT NULL THEN 'user' ELSE 'account' END AS account_type,
		       c.name,
		       COALESCE(dc.description, '')
		FROM defaultcredentials dc
		JOIN accounts a ON dc.account_id = a.id
		LEFT JOIN users u ON u.account_id = a.id
		LEFT JOIN companies c ON c.id = u.company_id
		ORDER BY dc.created_at
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []dto.DefaultAccountItem{}
	for rows.Next() {
		var item dto.DefaultAccountItem
		var companyName sql.NullString
		if err := rows.Scan(&item.AccountID, &item.AccountName, &item.PaymentHandle, &item.Currency, &item.AccountType, &companyName, &item.Description); err != nil {
			return nil, err
		}
		if companyName.Valid {
			c := companyName.String
			item.CompanyName = &c
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *DefaultCredentialsRepository) ListUsers(ctx context.Context) ([]dto.DefaultUserItem, error) {
	const query = `
		SELECT u.id, u.account_id, a.payment_handle, u.company_id, u.role,
		       COALESCE(dc.description, '')
		FROM defaultcredentials dc
		JOIN users u ON dc.account_id = u.account_id
		JOIN accounts a ON a.id = u.account_id
		ORDER BY dc.created_at
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []dto.DefaultUserItem{}
	for rows.Next() {
		var item dto.DefaultUserItem
		var companyID sql.NullString
		if err := rows.Scan(&item.UserID, &item.AccountID, &item.PaymentHandle, &companyID, &item.Role, &item.Description); err != nil {
			return nil, err
		}
		if companyID.Valid {
			c := companyID.String
			item.CompanyID = &c
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *DefaultCredentialsRepository) ListAccountsExcluding(ctx context.Context, excludeAccountID string) ([]dto.DefaultAccountItem, error) {
	const query = `
		SELECT a.id, a.account_name, a.payment_handle, a.currency,
		       CASE WHEN u.id IS NOT NULL THEN 'user' ELSE 'account' END AS account_type,
		       c.name,
		       COALESCE(dc.description, '')
		FROM defaultcredentials dc
		JOIN accounts a ON dc.account_id = a.id
		LEFT JOIN users u ON u.account_id = a.id
		LEFT JOIN companies c ON c.id = u.company_id
		WHERE a.account_type = 'USER' AND dc.account_id != $1
		ORDER BY dc.created_at
	`
	rows, err := r.db.QueryContext(ctx, query, excludeAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []dto.DefaultAccountItem{}
	for rows.Next() {
		var item dto.DefaultAccountItem
		var companyName sql.NullString
		if err := rows.Scan(&item.AccountID, &item.AccountName, &item.PaymentHandle, &item.Currency, &item.AccountType, &companyName, &item.Description); err != nil {
			return nil, err
		}
		if companyName.Valid {
			c := companyName.String
			item.CompanyName = &c
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *DefaultCredentialsRepository) ListSystemAccounts(ctx context.Context) ([]dto.DefaultSystemAccountItem, error) {
	const query = `
		SELECT a.id, a.account_name, a.payment_handle, a.account_type,
		       COALESCE(dc.description, '')
		FROM defaultcredentials dc
		JOIN accounts a ON dc.account_id = a.id
		WHERE a.account_type IN ('SYSTEM', 'PROMOTION_POOL')
		ORDER BY dc.created_at
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []dto.DefaultSystemAccountItem{}
	for rows.Next() {
		var item dto.DefaultSystemAccountItem
		if err := rows.Scan(&item.AccountID, &item.AccountName, &item.PaymentHandle, &item.AccountType, &item.Description); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *DefaultCredentialsRepository) ListCompanies(ctx context.Context) ([]dto.DefaultCompanyItem, error) {
	// DISTINCT ON (c.id) deduplicates companies when multiple default users belong to the same company.
	const query = `
		SELECT DISTINCT ON (c.id) c.id, c.name, c.business_name,
		       dc.account_id, a.payment_handle, COALESCE(dc.description, '')
		FROM defaultcredentials dc
		JOIN users u ON dc.account_id = u.account_id
		JOIN accounts a ON a.id = u.account_id
		JOIN companies c ON u.company_id = c.id
		ORDER BY c.id
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []dto.DefaultCompanyItem{}
	for rows.Next() {
		var item dto.DefaultCompanyItem
		if err := rows.Scan(&item.CompanyID, &item.Name, &item.BusinessName, &item.AccountID, &item.PaymentHandle, &item.Description); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
