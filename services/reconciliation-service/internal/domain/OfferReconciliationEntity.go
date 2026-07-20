package domain

import (
	"database/sql"
	"time"
)

type SuccessfulPaymentWithUnredeemedReservation struct {
	PaymentID         string
	ReservationID     string
	OfferID           string
	ReservationStatus string
	PaymentStatus     string
}

type RedeemedReservationWithoutSuccessfulPayment struct {
	ReservationID     string
	OfferID           string
	PaymentID         string
	ReservationStatus string
	PaymentStatus     string
}

type OfferReservationWithoutPayment struct {
	ID        string
	OfferID   string
	PaymentID string
	Status    string
}

type OfferRedemptionWithoutPayment struct {
	ID            string
	OfferID       string
	ReservationID string
	PaymentID     string
	Status        string
}

type ExpiredOfferReservation struct {
	ID        string
	OfferID   string
	PaymentID sql.NullString
	ExpiresAt time.Time
}

type OfferReservationRedemptionMismatch struct {
	ReservationID     string
	OfferID           string
	ReservationStatus string
	RedemptionID      sql.NullString
	RedemptionStatus  sql.NullString
}

type OfferRedeemedCountDrift struct {
	OfferID             string
	OfferCode           string
	RedeemedCount       int64
	ActualRedeemedCount int64
}
