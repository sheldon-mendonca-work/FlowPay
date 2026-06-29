package accountErrors

import "errors"

const (
	ErrorTypeNone            = "NONE"
	ErrorTypeValidationError = "VALIDATION_ERROR"
	ErrorTypeNotFound        = "NOT_FOUND"
	ErrorTypeInternalError   = "INTERNAL_ERROR"
)

func ToAccountErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrAccountNameRequired),
		errors.Is(err, ErrCurrencyRequired),
		errors.Is(err, ErrInvalidAccountType),
		errors.Is(err, ErrCompanyNameRequired),
		errors.Is(err, ErrBusinessNameRequired),
		errors.Is(err, ErrRoleRequired),
		errors.Is(err, ErrInvalidRole),
		errors.Is(err, ErrMethodNotAllowed),
		errors.Is(err, ErrInvalidRequestBody):
		return ErrorTypeValidationError
	case errors.Is(err, ErrAccountNotFound),
		errors.Is(err, ErrCompanyNotFound):
		return ErrorTypeNotFound
	default:
		return ErrorTypeInternalError
	}
}
