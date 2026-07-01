package domain

import "time"

type OfferEntity struct {
	ID                     string
	OfferCode              string
	OfferType              string
	OfferAmount            *int64
	OfferPercentage        *int16
	MaxBenefitAmount       int64
	MinimumPaymentAmount   int32
	MaximumPaymentAmount   *int32
	MaxRedemptions         int32
	MaxRedemptionsPerUser  int32
	RedeemedCount          int32
	ReservedCount          int32
	IdempotencyKey         string
	Status                 string
	Version                int
	CreatedBy              string
	PromotionPoolAccountId string
	BudgetAmount           *int32
	StartTime              time.Time
	EndTime                time.Time
	CreatedAt              time.Time
	UpdatedAt              time.Time
}
