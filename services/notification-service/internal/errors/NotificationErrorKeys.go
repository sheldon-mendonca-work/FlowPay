package flowpayNotificationErrors

import (
	"context"
	"errors"
)

const (
	ErrorTypeNone                 = "NONE"
	ErrorTypeKafkaMessageDecoding = "KAFKA_MESSAGE_DECODE_FAILED"
	ErrorTypeValidationError      = "VALIDATION_ERROR"
	ErrorTypeDBFailure            = "DB_FAILURE"
	ErrorTypeTimeout              = "TIMEOUT"
	ErrorTypeCanceled             = "CANCELED"
)

func ToNotificationErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorTypeTimeout
	case errors.Is(err, context.Canceled):
		return ErrorTypeCanceled
	case errors.Is(err, ErrInvalidEventPayload):
		return ErrorTypeKafkaMessageDecoding
	case errors.Is(err, ErrPaymentIDRequired),
		errors.Is(err, ErrStepNameRequired),
		errors.Is(err, ErrStatusRequired),
		errors.Is(err, ErrMethodNotAllowed):
		return ErrorTypeValidationError
	default:
		return ErrorTypeDBFailure
	}
}
