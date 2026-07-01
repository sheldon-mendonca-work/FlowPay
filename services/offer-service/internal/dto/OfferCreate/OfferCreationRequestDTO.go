package offerCreateDTO

import "time"

type OfferCreationRequestDTO struct {
	OfferCode             string    `json:"offer_code"`
	OfferType             string    `json:"offer_type"`
	CreatedBy             string    `json:"created_by"`
	CompanyId             string    `json:"company_id"`
	OfferAmount           *int64    `json:"offer_amount"`
	OfferPercentage       *int16    `json:"offer_percentage"`
	MaxBenefitAmount      int64     `json:"max_benefit_amount"`
	MinimumPaymentAmount  int32     `json:"min_payment_amount"`
	MaximumPaymentAmount  *int32    `json:"max_payment_amount"`
	MaxRedemptionsPerUser int32     `json:"max_redemptions_per_user"`
	MaxRedemptions        int32     `json:"max_redemptions"`
	BudgetAmount          *int32    `json:"budget_amount"`
	StartTime             time.Time `json:"start_time"`
	EndTime               time.Time `json:"end_time"`
}
