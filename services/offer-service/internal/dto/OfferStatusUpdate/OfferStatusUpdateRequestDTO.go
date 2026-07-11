package offerStatusUpdateDTO

type OfferStatusUpdateRequestDTO struct {
	Status    string `json:"status"`
	UpdatedBy string `json:"updated_by,omitempty"`
}
