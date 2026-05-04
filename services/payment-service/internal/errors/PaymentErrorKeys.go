package flowpayPaymentErrors

import (
	"context"
	"errors"
	"strings"
)

const (
	ErrorTypeNone                  = "NONE"
	ErrorTypeValidationError       = "VALIDATION_ERROR"
	ErrorTypeInsufficientBalance   = "INSUFFICIENT_BALANCE"
	ErrorTypeIdempotencyMismatch   = "IDEMPOTENCY_MISMATCH"
	ErrorTypeIdempotencyInProgress = "IDEMPOTENCY_IN_PROGRESS"
	ErrorTypeDBFailure             = "DB_FAILURE"
	ErrorTypeTimeout               = "TIMEOUT"
	ErrorTypeCanceled              = "CANCELED"
)

func ToPaymentErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrIdempotencyMismatch):
		return ErrorTypeIdempotencyMismatch
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorTypeTimeout
	case errors.Is(err, context.Canceled):
		return ErrorTypeCanceled
	case errors.Is(err, ErrSenderIDRequired),
		errors.Is(err, ErrReceiverIDRequired),
		errors.Is(err, ErrSenderReceiverIDMatching),
		errors.Is(err, ErrAmountMustBeGreaterThanZero),
		errors.Is(err, ErrCurrencyRequired),
		errors.Is(err, ErrIdempotencyKeyRequired),
		errors.Is(err, ErrInvalidRequestBody),
		errors.Is(err, ErrMethodNotAllowed):
		return ErrorTypeValidationError
	case errors.Is(err, ErrIdempotencyInProgress):
		return ErrorTypeIdempotencyInProgress
	case strings.Contains(strings.ToLower(err.Error()), "insufficient balance"):
		return ErrorTypeInsufficientBalance
	default:
		return ErrorTypeDBFailure
	}
}
