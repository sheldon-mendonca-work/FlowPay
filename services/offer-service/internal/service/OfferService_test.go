package service

import (
	"encoding/json"
	"errors"
	"flowpay/offer-service/internal/domain"
	offerCreateDTO "flowpay/offer-service/internal/dto/OfferCreate"
	offerRedeemDTO "flowpay/offer-service/internal/dto/OfferRedeem"
	offerReserveDTO "flowpay/offer-service/internal/dto/OfferReserve"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/offer-service/internal/types"
	"fmt"
	"testing"
	"time"
)

func TestCachedIdempotencyResult_Completed(t *testing.T) {
	response := offerCreateDTO.OfferCreationResponseDTO{OfferID: "offer-1", Status: types.SUCCESS}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("TestCachedIdempotencyResult_Completed: failed to marshal response: %v", err)
	}

	record := domain.OfferIdempotencyKey{
		IdempotencyKey: "key-1",
		Status:         "COMPLETED",
		ResponseBody:   string(responseBody),
	}

	got, err := cachedIdempotencyResult(record)

	if err != nil {
		t.Fatalf("TestCachedIdempotencyResult_Completed: expected nil error, got %v", err)
	}
	if got.OfferID != response.OfferID || got.Status != response.Status {
		t.Fatalf("TestCachedIdempotencyResult_Completed: expected %+v, got %+v", response, got)
	}
}

func TestCachedIdempotencyResult_CompletedInvalidJSON(t *testing.T) {
	record := domain.OfferIdempotencyKey{Status: "COMPLETED", ResponseBody: "not-json"}

	_, err := cachedIdempotencyResult(record)

	if err == nil {
		t.Fatal("TestCachedIdempotencyResult_CompletedInvalidJSON: expected error for invalid response body")
	}
}

func TestCachedIdempotencyResult_Failed(t *testing.T) {
	record := domain.OfferIdempotencyKey{Status: "FAILED", ErrorMessage: "boom"}

	_, err := cachedIdempotencyResult(record)

	if err == nil || err.Error() != "boom" {
		t.Fatalf("TestCachedIdempotencyResult_Failed: expected error 'boom', got %v", err)
	}
}

func TestCachedIdempotencyResult_InProgress(t *testing.T) {
	record := domain.OfferIdempotencyKey{IdempotencyKey: "key-1", Status: "IN_PROGRESS"}

	_, err := cachedIdempotencyResult(record)

	if !errors.Is(err, flowpayOfferErrors.ErrIdempotencyInProgress) {
		t.Fatalf("TestCachedIdempotencyResult_InProgress: expected ErrIdempotencyInProgress, got %v", err)
	}
}

func TestCachedReserveIdempotencyResult_Completed(t *testing.T) {
	response := offerReserveDTO.OfferReservationResponseDTO{ReservationID: "res-1", OfferID: "offer-1"}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("TestCachedReserveIdempotencyResult_Completed: failed to marshal response: %v", err)
	}

	record := domain.OfferReservationIdempotencyKeyEntity{Status: "COMPLETED", ResponseBody: string(responseBody)}

	got, err := cachedReserveIdempotencyResult(record)

	if err != nil {
		t.Fatalf("TestCachedReserveIdempotencyResult_Completed: expected nil error, got %v", err)
	}
	if got.ReservationID != response.ReservationID {
		t.Fatalf("TestCachedReserveIdempotencyResult_Completed: expected reservation id %s, got %s", response.ReservationID, got.ReservationID)
	}
}

func TestCachedReserveIdempotencyResult_CompletedInvalidJSON(t *testing.T) {
	record := domain.OfferReservationIdempotencyKeyEntity{Status: "COMPLETED", ResponseBody: "not-json"}

	_, err := cachedReserveIdempotencyResult(record)

	if err == nil {
		t.Fatal("TestCachedReserveIdempotencyResult_CompletedInvalidJSON: expected error for invalid response body")
	}
}

func TestCachedReserveIdempotencyResult_Failed(t *testing.T) {
	record := domain.OfferReservationIdempotencyKeyEntity{Status: "FAILED"}

	_, err := cachedReserveIdempotencyResult(record)

	if !errors.Is(err, flowpayOfferErrors.ErrReserveOfferFailed) {
		t.Fatalf("TestCachedReserveIdempotencyResult_Failed: expected ErrReserveOfferFailed, got %v", err)
	}
}

func TestCachedReserveIdempotencyResult_InProgress(t *testing.T) {
	record := domain.OfferReservationIdempotencyKeyEntity{IdempotencyKey: "key-1", Status: "IN_PROGRESS"}

	_, err := cachedReserveIdempotencyResult(record)

	if !errors.Is(err, flowpayOfferErrors.ErrIdempotencyInProgress) {
		t.Fatalf("TestCachedReserveIdempotencyResult_InProgress: expected ErrIdempotencyInProgress, got %v", err)
	}
}

func TestCachedRedeemIdempotencyResult_Completed(t *testing.T) {
	response := offerRedeemDTO.OfferRedemptionResponseDTO{RedemptionID: "redeem-1", OfferID: "offer-1"}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("TestCachedRedeemIdempotencyResult_Completed: failed to marshal response: %v", err)
	}

	record := domain.OfferRedemptionIdempotencyKeyEntity{Status: "COMPLETED", ResponseBody: string(responseBody)}

	got, err := cachedRedeemIdempotencyResult(record)

	if err != nil {
		t.Fatalf("TestCachedRedeemIdempotencyResult_Completed: expected nil error, got %v", err)
	}
	if got.RedemptionID != response.RedemptionID {
		t.Fatalf("TestCachedRedeemIdempotencyResult_Completed: expected redemption id %s, got %s", response.RedemptionID, got.RedemptionID)
	}
}

func TestCachedRedeemIdempotencyResult_CompletedInvalidJSON(t *testing.T) {
	record := domain.OfferRedemptionIdempotencyKeyEntity{Status: "COMPLETED", ResponseBody: "not-json"}

	_, err := cachedRedeemIdempotencyResult(record)

	if err == nil {
		t.Fatal("TestCachedRedeemIdempotencyResult_CompletedInvalidJSON: expected error for invalid response body")
	}
}

func TestCachedRedeemIdempotencyResult_Failed(t *testing.T) {
	record := domain.OfferRedemptionIdempotencyKeyEntity{Status: "FAILED"}

	_, err := cachedRedeemIdempotencyResult(record)

	if !errors.Is(err, flowpayOfferErrors.ErrRedeemOfferFailed) {
		t.Fatalf("TestCachedRedeemIdempotencyResult_Failed: expected ErrRedeemOfferFailed, got %v", err)
	}
}

func TestCachedRedeemIdempotencyResult_InProgress(t *testing.T) {
	record := domain.OfferRedemptionIdempotencyKeyEntity{IdempotencyKey: "key-1", Status: "IN_PROGRESS"}

	_, err := cachedRedeemIdempotencyResult(record)

	if !errors.Is(err, flowpayOfferErrors.ErrIdempotencyInProgress) {
		t.Fatalf("TestCachedRedeemIdempotencyResult_InProgress: expected ErrIdempotencyInProgress, got %v", err)
	}
}

func TestReplayableIdempotencyError_WithMessage(t *testing.T) {
	err := replayableIdempotencyError(domain.OfferIdempotencyKey{ErrorMessage: "boom"})

	if err == nil || err.Error() != "boom" {
		t.Fatalf("TestReplayableIdempotencyError_WithMessage: expected error 'boom', got %v", err)
	}
}

func TestReplayableIdempotencyError_NoMessage(t *testing.T) {
	err := replayableIdempotencyError(domain.OfferIdempotencyKey{})

	if !errors.Is(err, flowpayOfferErrors.ErrCreateOfferFailed) {
		t.Fatalf("TestReplayableIdempotencyError_NoMessage: expected ErrCreateOfferFailed, got %v", err)
	}
}

func TestReplayableReserveIdempotencyError_WithMessage(t *testing.T) {
	err := replayableReserveIdempotencyError(domain.OfferReservationIdempotencyKeyEntity{ErrorMessage: "boom"})

	if err == nil || err.Error() != "boom" {
		t.Fatalf("TestReplayableReserveIdempotencyError_WithMessage: expected error 'boom', got %v", err)
	}
}

func TestReplayableReserveIdempotencyError_NoMessage(t *testing.T) {
	err := replayableReserveIdempotencyError(domain.OfferReservationIdempotencyKeyEntity{})

	if !errors.Is(err, flowpayOfferErrors.ErrReserveOfferFailed) {
		t.Fatalf("TestReplayableReserveIdempotencyError_NoMessage: expected ErrReserveOfferFailed, got %v", err)
	}
}

func TestReplayableRedeemIdempotencyError_WithMessage(t *testing.T) {
	err := replayableRedeemIdempotencyError(domain.OfferRedemptionIdempotencyKeyEntity{ErrorMessage: "boom"})

	if err == nil || err.Error() != "boom" {
		t.Fatalf("TestReplayableRedeemIdempotencyError_WithMessage: expected error 'boom', got %v", err)
	}
}

func TestReplayableRedeemIdempotencyError_NoMessage(t *testing.T) {
	err := replayableRedeemIdempotencyError(domain.OfferRedemptionIdempotencyKeyEntity{})

	if !errors.Is(err, flowpayOfferErrors.ErrRedeemOfferFailed) {
		t.Fatalf("TestReplayableRedeemIdempotencyError_NoMessage: expected ErrRedeemOfferFailed, got %v", err)
	}
}

func TestShouldPersistFailedIdempotency(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"business failure", flowpayOfferErrors.ErrOfferNotActive, true},
		{"wrapped business failure", fmt.Errorf("context: %w", flowpayOfferErrors.ErrOfferExpired), true},
		{"technical failure", errors.New("connection refused"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldPersistFailedIdempotency(tc.err)
			if got != tc.want {
				t.Fatalf("shouldPersistFailedIdempotency(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsDeterministicBusinessFailure_MatchesShouldPersist(t *testing.T) {
	businessErr := flowpayOfferErrors.ErrOfferReservationLimitReached
	technicalErr := errors.New("db timeout")

	if !isDeterministicBusinessFailure(businessErr) {
		t.Fatal("TestIsDeterministicBusinessFailure_MatchesShouldPersist: expected true for business failure")
	}
	if isDeterministicBusinessFailure(technicalErr) {
		t.Fatal("TestIsDeterministicBusinessFailure_MatchesShouldPersist: expected false for technical failure")
	}
}

func TestLeaseExpiryFromNow_IsAboutFiveMinutesAhead(t *testing.T) {
	before := time.Now().UTC()
	expiry := leaseExpiryFromNow()
	after := time.Now().UTC()

	if expiry.Before(before.Add(5*time.Minute)) || expiry.After(after.Add(5*time.Minute)) {
		t.Fatalf("TestLeaseExpiryFromNow_IsAboutFiveMinutesAhead: expiry %v not within expected window", expiry)
	}
}

func TestReservationExpiryFromNow_IsAboutFifteenMinutesAhead(t *testing.T) {
	before := time.Now().UTC()
	expiry := reservationExpiryFromNow()
	after := time.Now().UTC()

	if expiry.Before(before.Add(15*time.Minute)) || expiry.After(after.Add(15*time.Minute)) {
		t.Fatalf("TestReservationExpiryFromNow_IsAboutFifteenMinutesAhead: expiry %v not within expected window", expiry)
	}
}

func TestToCompanyOfferDTO_PercentageOffer(t *testing.T) {
	percentage := int64(15)
	remainingBudget := int64(500)

	row := domain.CompanyOfferRow{
		ID:              "offer-1",
		OfferCode:       "SAVE15",
		OfferType:       "PERCENTAGE",
		OfferPercentage: &percentage,
		MaxRedemptions:  200,
		RedeemedCount:   50,
		RemainingBudget: &remainingBudget,
	}

	got := toCompanyOfferDTO(row)

	if !got.IsPercentage {
		t.Fatal("TestToCompanyOfferDTO_PercentageOffer: expected IsPercentage true")
	}
	if got.BenefitAmount != percentage {
		t.Fatalf("TestToCompanyOfferDTO_PercentageOffer: expected benefit amount %d, got %d", percentage, got.BenefitAmount)
	}
	if got.ConversionRate != 25.0 {
		t.Fatalf("TestToCompanyOfferDTO_PercentageOffer: expected conversion rate 25.0, got %v", got.ConversionRate)
	}
	if got.FundingStatus != "FUNDED" {
		t.Fatalf("TestToCompanyOfferDTO_PercentageOffer: expected FUNDED, got %s", got.FundingStatus)
	}
}

func TestToCompanyOfferDTO_FixedAmountOffer(t *testing.T) {
	amount := int64(100)

	row := domain.CompanyOfferRow{
		ID:          "offer-2",
		OfferType:   "FIXED",
		OfferAmount: &amount,
	}

	got := toCompanyOfferDTO(row)

	if got.IsPercentage {
		t.Fatal("TestToCompanyOfferDTO_FixedAmountOffer: expected IsPercentage false")
	}
	if got.BenefitAmount != amount {
		t.Fatalf("TestToCompanyOfferDTO_FixedAmountOffer: expected benefit amount %d, got %d", amount, got.BenefitAmount)
	}
}

func TestToCompanyOfferDTO_NoMaxRedemptionsSkipsConversionRate(t *testing.T) {
	row := domain.CompanyOfferRow{ID: "offer-3", MaxRedemptions: 0, RedeemedCount: 5}

	got := toCompanyOfferDTO(row)

	if got.ConversionRate != 0 {
		t.Fatalf("TestToCompanyOfferDTO_NoMaxRedemptionsSkipsConversionRate: expected conversion rate 0, got %v", got.ConversionRate)
	}
}

func TestToCompanyOfferDTO_DepletedBudget(t *testing.T) {
	zeroBudget := int64(0)
	row := domain.CompanyOfferRow{ID: "offer-4", RemainingBudget: &zeroBudget}

	got := toCompanyOfferDTO(row)

	if got.FundingStatus != "DEPLETED" {
		t.Fatalf("TestToCompanyOfferDTO_DepletedBudget: expected DEPLETED, got %s", got.FundingStatus)
	}
}

func TestToCompanyOfferDTO_NilBudgetIsDepleted(t *testing.T) {
	row := domain.CompanyOfferRow{ID: "offer-5"}

	got := toCompanyOfferDTO(row)

	if got.FundingStatus != "DEPLETED" {
		t.Fatalf("TestToCompanyOfferDTO_NilBudgetIsDepleted: expected DEPLETED, got %s", got.FundingStatus)
	}
}

func TestGenerateRandomId_GeneratesUUIDLikeValue(t *testing.T) {
	id, err := generateRandomId()

	if err != nil {
		t.Fatalf("TestGenerateRandomId_GeneratesUUIDLikeValue: expected nil error, got %v", err)
	}
	if len(id) != 36 {
		t.Fatalf("TestGenerateRandomId_GeneratesUUIDLikeValue: expected id length 36, got %d", len(id))
	}
}
