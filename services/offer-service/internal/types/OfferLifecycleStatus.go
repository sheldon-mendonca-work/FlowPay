package types

type OfferLifecycleStatus string

const (
	OfferStatusDraft   OfferLifecycleStatus = "DRAFT"
	OfferStatusActive  OfferLifecycleStatus = "ACTIVE"
	OfferStatusPaused  OfferLifecycleStatus = "PAUSED"
	OfferStatusExpired OfferLifecycleStatus = "EXPIRED"
	OfferStatusDeleted OfferLifecycleStatus = "DELETED"
)

func IsValidOfferLifecycleStatus(status string) bool {
	switch OfferLifecycleStatus(status) {
	case OfferStatusDraft, OfferStatusActive, OfferStatusPaused, OfferStatusExpired, OfferStatusDeleted:
		return true
	default:
		return false
	}
}

func OfferEventTypeForStatus(status string) OfferEventStatusEnum {
	switch OfferLifecycleStatus(status) {
	case OfferStatusActive:
		return OFFER_ACTIVATED
	case OfferStatusPaused:
		return OFFER_PAUSED
	case OfferStatusExpired:
		return OFFER_EXPIRED
	case OfferStatusDeleted:
		return OFFER_DELETED
	default:
		return OFFER_REVERTED_TO_DRAFT
	}
}
