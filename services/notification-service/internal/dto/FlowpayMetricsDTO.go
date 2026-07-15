package dto

type ConnectionStatus string

const (
	ConnectionStatusConnected    ConnectionStatus = "CONNECTED"
	ConnectionStatusDisconnected ConnectionStatus = "DISCONNECTED"
)

type FlowpayMetricsDTO struct {
	PaymentsTotal      int64 `json:"payments_total"`
	PaymentsSuccess    int64 `json:"payments_success"`
	PaymentsFailed     int64 `json:"payments_failed"`
	PaymentsProcessing int64 `json:"payments_processing"`

	OffersReserved int64 `json:"offers_reserved"`
	OffersRedeemed int64 `json:"offers_redeemed"`

	PaymentsToday int64 `json:"payments_today"`

	KafkaStatus          ConnectionStatus `json:"kafka_status"`
	OfferServiceStatus   ConnectionStatus `json:"offer_service_status"`
	PaymentServiceStatus ConnectionStatus `json:"payment_service_status"`
}
