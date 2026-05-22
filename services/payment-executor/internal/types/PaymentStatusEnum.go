package types

type PaymentStatusEnum string

const (
	CREATED    PaymentStatusEnum = "CREATED"
	PROCESSING PaymentStatusEnum = "PROCESSING"
	SUCCESS    PaymentStatusEnum = "SUCCESS"
	FAILED     PaymentStatusEnum = "FAILED"
)
