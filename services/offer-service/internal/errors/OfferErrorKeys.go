package flowpayOfferErrors

import (
	"context"
	"errors"
)

const (
	ErrorTypeNone                  = "NONE"
	ErrorTypeDBFailure             = "DB_FAILURE"
	ErrorTypeValidationError       = "VALIDATION_ERROR"
	ErrorTypeInsufficientBalance   = "INSUFFICIENT_BALANCE"
	ErrorTypeIdempotencyMismatch   = "IDEMPOTENCY_MISMATCH"
	ErrorTypeIdempotencyInProgress = "IDEMPOTENCY_IN_PROGRESS"
	ErrorTypeTimeout               = "TIMEOUT"
	ErrorTypeCanceled              = "CANCELED"
)

func ToOfferErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrIdempotencyMismatch):
		return ErrorTypeIdempotencyMismatch
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorTypeTimeout
	case errors.Is(err, context.Canceled):
		return ErrorTypeCanceled
	case errors.Is(err, ErrIdempotencyInProgress):
		return ErrorTypeIdempotencyInProgress

	case errors.Is(err, ErrOfferCodeRequired),
		errors.Is(err, ErrCreatedByRequired),
		errors.Is(err, ErrOfferTypeRequired),
		errors.Is(err, ErrOfferTypeInvalid),
		errors.Is(err, ErrCompanyIdRequired),
		errors.Is(err, ErrUserNotFound),
		errors.Is(err, ErrCompanyIdIsInvalid),
		errors.Is(err, ErrUserNotInCompany),
		errors.Is(err, ErrOfferAmountOrPercentageRequired),
		errors.Is(err, ErrOfferAmountInvalid),
		errors.Is(err, ErrOfferPercentageInvalid),
		errors.Is(err, ErrMaxBenefitAmountInvalid),
		errors.Is(err, ErrMaxRedemptionsPerUserInvalid),
		errors.Is(err, ErrMaxRedemptionsInvalid),
		errors.Is(err, ErrStartTimeRequired),
		errors.Is(err, ErrEndTimeRequired),
		errors.Is(err, ErrStartTimeAfterEndTime),
		errors.Is(err, ErrInvalidRequestBody),
		errors.Is(err, ErrOfferRequestTimedOut),
		errors.Is(err, ErrOfferRequestCanceled),
		errors.Is(err, ErrAccountIdRequired),
		errors.Is(err, ErrOfferIdRequired),
		errors.Is(err, ErrPaymentIdRequired),
		errors.Is(err, ErrReservationIdRequired),
		errors.Is(err, ErrRedemptionIdRequired),
		errors.Is(err, ErrCreateOfferFailed):
		return ErrorTypeValidationError
	default:
		return ErrorTypeDBFailure
	}
}
