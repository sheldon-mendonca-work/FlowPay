package flowpayNotificationErrors

import "errors"

var (
	ErrPaymentIDRequired   = errors.New("payment_id is required")
	ErrStepNameRequired    = errors.New("step_name is required")
	ErrStatusRequired      = errors.New("status is required")
	ErrMethodNotAllowed    = errors.New("method not allowed")
	ErrInvalidEventPayload = errors.New("invalid kafka event payload")
)
