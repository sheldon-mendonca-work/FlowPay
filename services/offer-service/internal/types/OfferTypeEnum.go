package types

type OfferType string

const (
	OfferTypeDiscount OfferType = "DISCOUNT"
	OfferTypeCashback OfferType = "CASHBACK"
)

func IsValidOfferType(offerType string) bool {
	return offerType == string(OfferTypeDiscount) || offerType == string(OfferTypeCashback)
}
