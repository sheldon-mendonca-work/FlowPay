package authErrors

import "errors"

var (
	ErrEmailRequired    = errors.New("email is required")
	ErrPasswordRequired = errors.New("password is required")
	ErrPasswordTooShort = errors.New("password must be at least 8 characters")
	ErrNameRequired     = errors.New("account_name is required")
	ErrTypeRequired     = errors.New("account_type is required")
	ErrCurrencyRequired = errors.New("currency is required")

	ErrInvalidCredentials    = errors.New("invalid email or password")
	ErrEmailAlreadyExists    = errors.New("email already registered")
	ErrAccountCreationFailed = errors.New("failed to create account")

	ErrRefreshTokenRequired = errors.New("refresh_token is required")
	ErrInvalidRefreshToken  = errors.New("invalid refresh token")
	ErrExpiredRefreshToken  = errors.New("refresh token has expired")

	ErrMethodNotAllowed      = errors.New("method not allowed")
	ErrInvalidRequestBody    = errors.New("invalid request body")
	ErrAuthenticationRequired = errors.New("authentication required")

	ErrAccountIDRequired   = errors.New("account_id is required")
	ErrAccountTypeRequired = errors.New("account_type is required")
)
