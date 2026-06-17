package domain

import (
	"time"
)

type OfferOutboxEventType struct {
	ID             string
	AggregateType  string // payment/order/wallet
	AggregateID    string // for now idempotency then later paymentid
	IdempotencyKey string
	Payload        string
	EventType      string
	EventVersion   int8
	Status         string
	TraceID        string
	RequestID      string
	RetryCount     int8
	LockedUntil    time.Time
	CreatedAt      time.Time
	PublishedAt    time.Time
}

const (
	OfferOutboxEventPending    = "PENDING"
	OfferOutboxEventProcessing = "PROCESSING"
	OfferOutboxEventPublished  = "PUBLISHED"
)

type OfferReservedEvent struct {
	OfferID       string `json:"offer_id"`
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	AccountID     string `json:"account_id"`

	SenderID               string `json:"sender_id"`
	ReceiverID             string `json:"receiver_id"`
	Amount                 int64  `json:"amount"`
	Currency               string `json:"currency"`
	PaymentIdempotencyKey  string `json:"payment_idempotency_key"`
	PromotionPoolAccountId string `json:"promotional_pool_account_id"`

	OfferType            string `json:"offer_type"`
	OfferAmount          *int64 `json:"offer_amount"`
	OfferPercentage      *int16 `json:"offer_percentage"`
	MaxBenefitAmount     int64  `json:"max_benefit_amount"`
	MinimumPaymentAmount int32  `json:"minimum_payment_amount"`
	MaximumPaymentAmount int32  `json:"maximum_payment_amount"`

	PaymentOwnerToken string `json:"payment_owner_token"`
	IdempotencyKey    string `json:"idempotency_key"`
	TraceID           string `json:"trace_id"`
	RequestID         string `json:"request_id"`
}

type OfferRejectedEvent struct {
	OfferID   string `json:"offer_id"`
	PaymentID string `json:"payment_id"`
	AccountID string `json:"account_id"`

	SenderID   string `json:"sender_id"`
	ReceiverID string `json:"receiver_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`

	PaymentIdempotencyKey  string `json:"payment_idempotency_key"`
	PromotionPoolAccountId string `json:"promotion_pool_account_id"`
	TraceID                string `json:"trace_id"`
	RequestID              string `json:"request_id"`

	PaymentOwnerToken string `json:"payment_owner_token"`
	ErrorCode         string `json:"error_code"`
	ErrorText         string `json:"error_text"`
}
