package companyOffersDTO

import "time"

type CompanyOffersRequestDTO struct {
	CompanyID string `json:"company_id"`
}

type CompanyOfferDTO struct {
	ID                string    `json:"id"`
	Code              string    `json:"code"`
	Type              string    `json:"type"`
	BenefitAmount     int64     `json:"benefit_amount"`
	IsPercentage      bool      `json:"is_percentage"`
	MaxBenefit        int64     `json:"max_benefit"`
	MinPaymentAmount  int64     `json:"min_payment_amount"`
	MaxPaymentAmount  *int64    `json:"max_payment_amount"`
	MaxRedemptions    int32     `json:"max_redemptions"`
	PerUserLimit      int32     `json:"per_user_limit"`
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
	PromotionPoolName string    `json:"promotion_pool_name"`
	InitialBudget     *int64    `json:"initial_budget"`
	RemainingBudget   *int64    `json:"remaining_budget"`
	TotalRedemptions  int32     `json:"total_redemptions"`
	ConversionRate    float64   `json:"conversion_rate"`
	Status            string    `json:"status"`
	FundingStatus     string    `json:"funding_status"`
	CreatedAt         time.Time `json:"created_at"`
}

type CompanyOffersResponseDTO struct {
	Offers []CompanyOfferDTO `json:"offers"`
}

type CompanyOffersSummaryDTO struct {
	ActiveOffers      int64   `json:"active_offers"`
	TotalOffers       int64   `json:"total_offers"`
	TotalRedemptions  int64   `json:"total_redemptions"`
	BudgetRemaining   int64   `json:"budget_remaining"`
	InitialBudget     int64   `json:"initial_budget"`
	AvgConversionRate float64 `json:"avg_conversion_rate"`
}
