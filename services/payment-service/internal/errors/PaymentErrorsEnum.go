package flowpayPaymentErrors

import "errors"

var (
	ErrUserIDRequired              = errors.New("user_id is required")
	ErrAmountMustBeGreaterThanZero = errors.New("amount must be greater than 0")
	ErrCurrencyRequired            = errors.New("currency is required")
	ErrIdempotencyKeyRequired      = errors.New("idempotency_key is required")

	ErrMethodNotAllowed       = errors.New("method not allowed")
	ErrInvalidRequestBody     = errors.New("invalid request body")
	ErrPaymentRequestTimedOut = errors.New("payment request timed out")
	ErrPaymentRequestCanceled = errors.New("payment request canceled")
	ErrCreatePaymentFailed    = errors.New("failed to create payment")

	ErrIdempotencyMismatch = errors.New("idempotency key reused with different payload")
)
