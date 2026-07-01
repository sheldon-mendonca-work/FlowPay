package domain

import "time"

type CompanyOfferRow struct {
	ID                    string
	OfferCode             string
	OfferType             string
	OfferAmount           *int64
	OfferPercentage       *int64
	MaxBenefitAmount      int64
	MinimumPaymentAmount  int64
	MaximumPaymentAmount  *int64
	MaxRedemptions        int32
	MaxRedemptionsPerUser int32
	RedeemedCount         int32
	Status                string
	BudgetAmount          *int64
	StartTime             time.Time
	EndTime               time.Time
	CreatedAt             time.Time
	PromotionPoolName     string
	RemainingBudget       *int64
}

type CompanyOffersSummaryRow struct {
	ActiveOffers      int64
	TotalOffers       int64
	TotalRedemptions  int64
	BudgetRemaining   int64
	InitialBudget     int64
	AvgConversionRate float64
}
