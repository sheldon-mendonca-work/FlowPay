package accountErrors

import "errors"

var (
	ErrAccountNameRequired  = errors.New("account_name is required")
	ErrCurrencyRequired     = errors.New("currency is required")
	ErrInvalidAccountType   = errors.New("account_type must be USER, SYSTEM, or PROMOTION_POOL")
	ErrCompanyNameRequired  = errors.New("name is required")
	ErrBusinessNameRequired = errors.New("business_name is required")
	ErrRoleRequired         = errors.New("role is required")
	ErrInvalidRole          = errors.New("role must be ADMIN or USER")
	ErrAccountNotFound      = errors.New("account not found")
	ErrCompanyNotFound      = errors.New("company not found")
	ErrMethodNotAllowed     = errors.New("method not allowed")
	ErrInvalidRequestBody   = errors.New("invalid request body")
)
