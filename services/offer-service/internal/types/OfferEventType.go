package types

type OfferEventStatusEnum string

const (
	OFFER_CREATED           OfferEventStatusEnum = "OFFER_CREATED"
	OFFER_ACTIVATED         OfferEventStatusEnum = "OFFER_ACTIVATED"
	OFFER_PAUSED            OfferEventStatusEnum = "OFFER_PAUSED"
	OFFER_EXPIRED           OfferEventStatusEnum = "OFFER_EXPIRED"
	OFFER_RESERVED          OfferEventStatusEnum = "OFFER_RESERVED"
	OFFER_REDEEMED          OfferEventStatusEnum = "OFFER_REDEEMED"
	OFFER_REFUNDED          OfferEventStatusEnum = "OFFER_REFUNDED"
	OFFER_DELETED           OfferEventStatusEnum = "OFFER_DELETED"
	OFFER_REVERTED_TO_DRAFT OfferEventStatusEnum = "OFFER_REVERTED_TO_DRAFT"
)
