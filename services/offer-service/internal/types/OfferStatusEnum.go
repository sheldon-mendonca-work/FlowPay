package types

type OfferStatusEnum string

const (
	CREATED    OfferStatusEnum = "CREATED"
	PROCESSING OfferStatusEnum = "PROCESSING"
	SUCCESS    OfferStatusEnum = "SUCCESS"
	FAILED     OfferStatusEnum = "FAILED"
)
