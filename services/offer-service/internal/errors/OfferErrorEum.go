package flowpayOfferErrors

import "errors"

var (
	ErrMethodNotAllowed    = errors.New("method_not_allowed")
	ErrOfferNotFound       = errors.New("offer_not_found")
	ErrInsufficientBalance = errors.New("insufficient_balance")

	ErrOfferCodeRequired               = errors.New("offer_code_missing")
	ErrCreatedByRequired               = errors.New("offer_creator_user_missing")
	ErrOfferTypeRequired               = errors.New("offer_type_missing")
	ErrOfferTypeInvalid                = errors.New("offer_type_is_invalid")
	ErrCompanyIdRequired               = errors.New("company_id_missing")
	ErrCompanyIdIsInvalid              = errors.New("company_does_not_exist")
	ErrUserNotFound                    = errors.New("user_does_not_exist")
	ErrUserNotInCompany                = errors.New("user_does_not_belong_in_company")
	ErrOfferAmountOrPercentageRequired = errors.New("need_offer_amount_or_percentage")
	ErrOfferAmountInvalid              = errors.New("offer_amount_invalid")
	ErrOfferPercentageInvalid          = errors.New("offer_percentage_invalid")
	ErrMaxBenefitAmountInvalid         = errors.New("max_benefit_amount_invalid")
	ErrMaxRedemptionsPerUserInvalid    = errors.New("max_redemptions_per_user_invalid")
	ErrMaxRedemptionsInvalid           = errors.New("max_redemptions_invalid")
	ErrStartTimeRequired               = errors.New("start_time_missing")
	ErrEndTimeRequired                 = errors.New("end_time_missing")
	ErrStartTimeAfterEndTime           = errors.New("start_time_after_end_time")
	ErrInvalidRequestBody              = errors.New("invalid request body")
	ErrOfferRequestTimedOut            = errors.New("offer request timed out")
	ErrOfferRequestCanceled            = errors.New("offer request canceled")
	ErrCreateOfferFailed               = errors.New("failed to create offer")
	ErrMinimumPaymentAmountNegative    = errors.New("minimum_payment_amount_is_negative")
	ErrMaximumPaymentAmountNegative    = errors.New("maximum_payment_amount_is_negative")
	ErrMinimumPaymentAmountInvalid     = errors.New("minimum_payment_amount_cannot_exceed_maximum")

	ErrIdempotencyRequired   = errors.New("idempotency key_is_missing")
	ErrIdempotencyMismatch   = errors.New("idempotency key reused with different payload")
	ErrIdempotencyInProgress = errors.New("Offer is in progress")
)
