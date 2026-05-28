package flowpayPaymentErrors

import "errors"

var (
	ErrSenderIDRequired            = errors.New("sender_id is required")
	ErrReceiverIDRequired          = errors.New("receiver_id is required")
	ErrSenderReceiverIDMatching    = errors.New("receiver_id and sender_id must not match")
	ErrAmountMustBeGreaterThanZero = errors.New("amount must be greater than 0")
	ErrCurrencyRequired            = errors.New("currency is required")
	ErrIdempotencyKeyRequired      = errors.New("idempotency_key is required")
	ErrSenderAccountNotFound       = errors.New("sender account is not present")
	ErrReceiverAccountNotFound     = errors.New("receiver account is not present")
	ErrSenderCurrencyMismatch      = errors.New("sender account currency not matching with request")
	ErrAccountCurrencyMismatch     = errors.New("sender or receiver account currency mismatch")
	ErrInsufficientBalance         = errors.New("insufficient balance")

	ErrMethodNotAllowed       = errors.New("method not allowed")
	ErrInvalidRequestBody     = errors.New("invalid request body")
	ErrPaymentNotFound        = errors.New("payment not found")
	ErrPaymentRequestTimedOut = errors.New("payment request timed out")
	ErrPaymentRequestCanceled = errors.New("payment request canceled")
	ErrCreatePaymentFailed    = errors.New("failed to create payment")

	ErrIdempotencyMismatch   = errors.New("idempotency key reused with different payload")
	ErrIdempotencyInProgress = errors.New("Payment is in progress")
)
