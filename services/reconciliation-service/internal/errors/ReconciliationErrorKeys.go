package flowpayReconciliationErrors

import (
	"context"
	"errors"
)

const (
	ErrorTypeNone            = "NONE"
	ErrorTypeValidationError = "VALIDATION_ERROR"
	ErrorTypeDBFailure       = "DB_FAILURE"
	ErrorTypeTimeout         = "TIMEOUT"
	ErrorTypeCanceled        = "CANCELED"
)

func ToReconciliationErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrMethodNotAllowed):
		return ErrorTypeValidationError
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorTypeTimeout
	case errors.Is(err, context.Canceled):
		return ErrorTypeCanceled
	default:
		return ErrorTypeDBFailure
	}
}
