package flowpayReconciliationErrors

import "errors"

var (
	ErrMethodDoesntExist         = errors.New("method does not exist")
	ErrMethodNotAllowed          = errors.New("method not allowed")
	ErrReconciliationCheckFailed = errors.New("failed to run reconciliation check")
	ErrReconciliationTimedOut    = errors.New("reconciliation request timed out")
	ErrReconciliationCanceled    = errors.New("reconciliation request canceled")
)
