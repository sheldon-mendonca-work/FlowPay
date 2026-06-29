package authErrors

import "errors"

const (
	ErrorTypeNone               = "NONE"
	ErrorTypeValidationError    = "VALIDATION_ERROR"
	ErrorTypeInvalidCredentials = "INVALID_CREDENTIALS"
	ErrorTypeTokenError         = "TOKEN_ERROR"
	ErrorTypeConflict           = "CONFLICT"
	ErrorTypeInternalError      = "INTERNAL_ERROR"
)

func ToAuthErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrEmailRequired),
		errors.Is(err, ErrPasswordRequired),
		errors.Is(err, ErrPasswordTooShort),
		errors.Is(err, ErrNameRequired),
		errors.Is(err, ErrTypeRequired),
		errors.Is(err, ErrCurrencyRequired),
		errors.Is(err, ErrRefreshTokenRequired),
		errors.Is(err, ErrMethodNotAllowed),
		errors.Is(err, ErrInvalidRequestBody):
		return ErrorTypeValidationError
	case errors.Is(err, ErrInvalidCredentials):
		return ErrorTypeInvalidCredentials
	case errors.Is(err, ErrEmailAlreadyExists):
		return ErrorTypeConflict
	case errors.Is(err, ErrInvalidRefreshToken),
		errors.Is(err, ErrExpiredRefreshToken):
		return ErrorTypeTokenError
	default:
		return ErrorTypeInternalError
	}
}
