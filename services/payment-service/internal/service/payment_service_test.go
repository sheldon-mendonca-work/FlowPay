package service

import (
	"encoding/json"
	"errors"
	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/types"
	"fmt"
	"testing"
	"time"
)

func TestValidateSenderAndReceiverAccounts_SameUser(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     5000,
			Currency:    "INR",
		},
		"receiver": {
			ID:          "sender",
			AccountName: "user2",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "sender",
		Amount:     50,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_SameUser : expected error for same sender and receiver")
	}
}

func TestValidateSenderAndReceiverAccounts_SenderAccountMissing(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     50,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_SenderAccountMissing: expected error for only sender")
	}
}

func TestValidateSenderAndReceiverAccounts_ReceiverAccountMissing(t *testing.T) {
	accounts := map[string]domain.Account{
		"receiver": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     50,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_ReceiverAccountMissing: expected error for only receiver")
	}
}

func TestValidateSenderAndReceiverAccounts_AccountCurrencyMismatch(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     5000,
			Currency:    "INR",
		},
		"receiver": {
			ID:          "receiver",
			AccountName: "user2",
			Balance:     5000,
			Currency:    "USD",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     50,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_AccountCurrencyMismatch : expected error for different currency", err)
	}
}

func TestValidateSenderAndReceiverAccounts_RequestCurrencyMismatch(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     5000,
			Currency:    "INR",
		},
		"receiver": {
			ID:          "receiver",
			AccountName: "user2",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     50,
		Currency:   "USD",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_RequestCurrencyMismatch : expected error for different currency", err)
	}
}

func TestValidateSenderAndReceiverAccounts_SenderBalanceInsufficient(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     50,
			Currency:    "INR",
		},
		"receiver": {
			ID:          "receiver",
			AccountName: "user2",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     5000,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err == nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_SenderBalanceInsufficient : expected error for different currency", err)
	}
}

func TestValidateSenderAndReceiverAccounts_ValidAccounts(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:          "sender",
			AccountName: "user1",
			Balance:     10000,
			Currency:    "INR",
		},
		"receiver": {
			ID:          "receiver",
			AccountName: "user2",
			Balance:     5000,
			Currency:    "INR",
		},
	}

	req := dto.PaymentRequestDTO{
		SenderID:   "sender",
		ReceiverID: "receiver",
		Amount:     50,
		Currency:   "INR",
	}

	err := validateSenderAndReceiverAccounts(accounts, req, 5000)

	if err != nil {
		t.Fatal("TestValidateSenderAndReceiverAccounts_ValidAccounts: expected nil error for valid accounts")
	}
}

func TestToPaymentResponse_MapsPaymentFields(t *testing.T) {
	payment := domain.Payment{
		ID:     "payment-123",
		Status: string(types.SUCCESS),
	}

	response := toPaymentResponse(payment)

	if response.PaymentID != payment.ID {
		t.Fatalf("TestToPaymentResponse_MapsPaymentFields: expected payment id %s, got %s", payment.ID, response.PaymentID)
	}

	if response.Status != types.SUCCESS {
		t.Fatalf("TestToPaymentResponse_MapsPaymentFields: expected status %s, got %s", types.SUCCESS, response.Status)
	}
}

func TestNewPaymentID_GeneratesUUIDLikeValue(t *testing.T) {
	paymentID, err := newPaymentID()

	if err != nil {
		t.Fatalf("TestNewPaymentID_GeneratesUUIDLikeValue: expected nil error, got %v", err)
	}

	if paymentID == "" {
		t.Fatal("TestNewPaymentID_GeneratesUUIDLikeValue: expected non-empty payment id")
	}

	if len(paymentID) != 36 {
		t.Fatalf("TestNewPaymentID_GeneratesUUIDLikeValue: expected id length 36, got %d", len(paymentID))
	}
}

func TestGenerateSenderTransaction_BuildsDebitTransaction(t *testing.T) {
	payment := domain.Payment{
		ID:         "payment-123",
		SenderID:   "sender-1",
		ReceiverID: "receiver-1",
		Amount:     5500,
		Currency:   "INR",
	}

	transaction, err := generateSenderTransaction(payment)

	if err != nil {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected nil error, got %v", err)
	}

	if transaction.ID == "" {
		t.Fatal("TestGenerateSenderTransaction_BuildsDebitTransaction: expected transaction id to be generated")
	}

	if transaction.PaymentID != payment.ID {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected payment id %s, got %s", payment.ID, transaction.PaymentID)
	}

	if transaction.AccountID != payment.SenderID {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected account id %s, got %s", payment.SenderID, transaction.AccountID)
	}

	if transaction.Type != "DEBIT" {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected type DEBIT, got %s", transaction.Type)
	}

	if transaction.Amount != payment.Amount {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected amount %d, got %d", payment.Amount, transaction.Amount)
	}

	if transaction.Currency != payment.Currency {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected currency %s, got %s", payment.Currency, transaction.Currency)
	}

	if transaction.Status != "SUCCESS" {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected status SUCCESS, got %s", transaction.Status)
	}
}

func TestGenerateReceiverTransaction_BuildsCreditTransaction(t *testing.T) {
	payment := domain.Payment{
		ID:         "payment-123",
		SenderID:   "sender-1",
		ReceiverID: "receiver-1",
		Amount:     5500,
		Currency:   "INR",
	}

	transaction, err := generateReceiverTransaction(payment)

	if err != nil {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected nil error, got %v", err)
	}

	if transaction.ID == "" {
		t.Fatal("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected transaction id to be generated")
	}

	if transaction.PaymentID != payment.ID {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected payment id %s, got %s", payment.ID, transaction.PaymentID)
	}

	if transaction.AccountID != payment.ReceiverID {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected account id %s, got %s", payment.ReceiverID, transaction.AccountID)
	}

	if transaction.Type != "CREDIT" {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected type CREDIT, got %s", transaction.Type)
	}

	if transaction.Amount != payment.Amount {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected amount %d, got %d", payment.Amount, transaction.Amount)
	}

	if transaction.Currency != payment.Currency {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected currency %s, got %s", payment.Currency, transaction.Currency)
	}

	if transaction.Status != "SUCCESS" {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected status SUCCESS, got %s", transaction.Status)
	}
}

func TestCachedIdempotencyResult_Completed(t *testing.T) {
	response := dto.PaymentResponseDTO{PaymentID: "payment-123", Status: types.SUCCESS}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("TestCachedIdempotencyResult_Completed: failed to marshal response: %v", err)
	}

	record := domain.PaymentIdempotencyKey{
		IdempotencyKey: "key-1",
		Status:         "COMPLETED",
		ResponseBody:   string(responseBody),
	}

	got, err := cachedIdempotencyResult(record)

	if err != nil {
		t.Fatalf("TestCachedIdempotencyResult_Completed: expected nil error, got %v", err)
	}

	if got.PaymentID != response.PaymentID || got.Status != response.Status {
		t.Fatalf("TestCachedIdempotencyResult_Completed: expected %+v, got %+v", response, got)
	}
}

func TestCachedIdempotencyResult_CompletedInvalidJSON(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		IdempotencyKey: "key-1",
		Status:         "COMPLETED",
		ResponseBody:   "not-json",
	}

	_, err := cachedIdempotencyResult(record)

	if err == nil {
		t.Fatal("TestCachedIdempotencyResult_CompletedInvalidJSON: expected error for invalid response body")
	}
}

func TestCachedIdempotencyResult_Failed(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		IdempotencyKey: "key-1",
		Status:         "FAILED",
		ErrorCode:      flowpayPaymentErrors.ErrorTypeInsufficientBalance,
	}

	_, err := cachedIdempotencyResult(record)

	if !errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestCachedIdempotencyResult_Failed: expected ErrInsufficientBalance, got %v", err)
	}
}

func TestCachedIdempotencyResult_InProgress(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		IdempotencyKey: "key-1",
		Status:         "IN_PROGRESS",
	}

	_, err := cachedIdempotencyResult(record)

	if err == nil {
		t.Fatal("TestCachedIdempotencyResult_InProgress: expected error for in-progress record")
	}
}

func TestReplayableIdempotencyError_InsufficientBalanceWithMessage(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		ErrorCode:    flowpayPaymentErrors.ErrorTypeInsufficientBalance,
		ErrorMessage: "sender_id=sender-1",
	}

	err := replayableIdempotencyError(record)

	if !errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestReplayableIdempotencyError_InsufficientBalanceWithMessage: expected ErrInsufficientBalance, got %v", err)
	}
	if err.Error() == flowpayPaymentErrors.ErrInsufficientBalance.Error() {
		t.Fatal("TestReplayableIdempotencyError_InsufficientBalanceWithMessage: expected error message to include record detail")
	}
}

func TestReplayableIdempotencyError_InsufficientBalanceNoMessage(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		ErrorCode: flowpayPaymentErrors.ErrorTypeInsufficientBalance,
	}

	err := replayableIdempotencyError(record)

	if !errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestReplayableIdempotencyError_InsufficientBalanceNoMessage: expected ErrInsufficientBalance, got %v", err)
	}
}

func TestReplayableIdempotencyError_DefaultWithMessage(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		ErrorCode:    "SOME_OTHER_CODE",
		ErrorMessage: "boom",
	}

	err := replayableIdempotencyError(record)

	if err == nil || err.Error() != "boom" {
		t.Fatalf("TestReplayableIdempotencyError_DefaultWithMessage: expected error 'boom', got %v", err)
	}
}

func TestReplayableIdempotencyError_DefaultNoMessage(t *testing.T) {
	record := domain.PaymentIdempotencyKey{
		ErrorCode: "SOME_OTHER_CODE",
	}

	err := replayableIdempotencyError(record)

	if !errors.Is(err, flowpayPaymentErrors.ErrCreatePaymentFailed) {
		t.Fatalf("TestReplayableIdempotencyError_DefaultNoMessage: expected ErrCreatePaymentFailed, got %v", err)
	}
}

func TestShouldPersistFailedIdempotency(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"business failure", flowpayPaymentErrors.ErrInsufficientBalance, true},
		{"wrapped business failure", fmt.Errorf("context: %w", flowpayPaymentErrors.ErrSenderAccountNotFound), true},
		{"unrelated error with similar text", errors.New("insufficient balance"), false},
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

func TestShouldPersistFailedIdempotency_WrappedWithErrorsIs(t *testing.T) {
	wrapped := errors.Join(flowpayPaymentErrors.ErrSenderReceiverIDMatching)

	if !shouldPersistFailedIdempotency(wrapped) {
		t.Fatal("TestShouldPersistFailedIdempotency_WrappedWithErrorsIs: expected true for joined business error")
	}
}

func TestIsDeterministicBusinessFailure_MatchesShouldPersist(t *testing.T) {
	businessErr := flowpayPaymentErrors.ErrAccountCurrencyMismatch
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

	minExpected := before.Add(5 * time.Minute)
	maxExpected := after.Add(5 * time.Minute)

	if expiry.Before(minExpected) || expiry.After(maxExpected) {
		t.Fatalf("TestLeaseExpiryFromNow_IsAboutFiveMinutesAhead: expected expiry between %v and %v, got %v", minExpected, maxExpected, expiry)
	}
}

func TestMapPaymentInitiatedToOutbox_PaymentEvent(t *testing.T) {
	event := domain.PaymentInitiatedEvent{
		ID:             "payment-123",
		SenderID:       "sender-1",
		ReceiverID:     "receiver-1",
		IdempotencyKey: "idem-1",
		Amount:         500,
		Currency:       "INR",
	}

	outboxEvent, err := MapPaymentInitiatedToOutbox(event, 2, "trace-1", "request-1")

	if err != nil {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected nil error, got %v", err)
	}

	if outboxEvent.ID == "" {
		t.Fatal("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected generated outbox event id")
	}
	if outboxEvent.AggregateType != "payment" {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected aggregate type 'payment', got %s", outboxEvent.AggregateType)
	}
	if outboxEvent.AggregateID != event.ID {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected aggregate id %s, got %s", event.ID, outboxEvent.AggregateID)
	}
	if outboxEvent.EventType != "payment_initiated" {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected event type 'payment_initiated', got %s", outboxEvent.EventType)
	}
	if outboxEvent.Status != domain.OutboxEventPending {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected status pending, got %s", outboxEvent.Status)
	}
	if outboxEvent.RetryCount != 2 {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected retry count 2, got %d", outboxEvent.RetryCount)
	}
	if outboxEvent.TraceID != "trace-1" || outboxEvent.RequestID != "request-1" {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected trace/request id to be propagated, got %s/%s", outboxEvent.TraceID, outboxEvent.RequestID)
	}
	if outboxEvent.IdempotencyKey != event.IdempotencyKey {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected idempotency key %s, got %s", event.IdempotencyKey, outboxEvent.IdempotencyKey)
	}

	var decoded domain.PaymentInitiatedEvent
	if err := json.Unmarshal([]byte(outboxEvent.Payload), &decoded); err != nil {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected payload to decode, got error %v", err)
	}
	if decoded.ID != event.ID {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_PaymentEvent: expected decoded payload id %s, got %s", event.ID, decoded.ID)
	}
}

func TestMapPaymentInitiatedToOutbox_OfferEvent(t *testing.T) {
	event := domain.PaymentInitiatedEvent{
		ID:      "payment-123",
		OfferID: "offer-1",
	}

	outboxEvent, err := MapPaymentInitiatedToOutbox(event, 0, "trace-1", "request-1")

	if err != nil {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_OfferEvent: expected nil error, got %v", err)
	}

	if outboxEvent.EventType != "offer_initiated" {
		t.Fatalf("TestMapPaymentInitiatedToOutbox_OfferEvent: expected event type 'offer_initiated' when offer id is set, got %s", outboxEvent.EventType)
	}
}

func TestOfferSummaryFromPayment_NoOffer(t *testing.T) {
	summary := offerSummaryFromPayment(domain.Payment{OfferID: ""})

	if summary != nil {
		t.Fatalf("TestOfferSummaryFromPayment_NoOffer: expected nil summary, got %+v", summary)
	}
}

func TestOfferSummaryFromPayment_Cashback(t *testing.T) {
	payment := domain.Payment{
		OfferID:            "offer-1",
		OfferCode:          "CASH10",
		OfferType:          "CASHBACK",
		OfferBenefitAmount: 100,
	}

	summary := offerSummaryFromPayment(payment)

	if summary == nil {
		t.Fatal("TestOfferSummaryFromPayment_Cashback: expected non-nil summary")
	}
	if summary.CashbackAmount != 100 {
		t.Fatalf("TestOfferSummaryFromPayment_Cashback: expected cashback amount 100, got %d", summary.CashbackAmount)
	}
	if summary.DiscountAmount != 0 {
		t.Fatalf("TestOfferSummaryFromPayment_Cashback: expected discount amount 0, got %d", summary.DiscountAmount)
	}
}

func TestOfferSummaryFromPayment_Discount(t *testing.T) {
	payment := domain.Payment{
		OfferID:            "offer-1",
		OfferCode:          "DISC10",
		OfferType:          "PERCENTAGE",
		OfferBenefitAmount: 50,
	}

	summary := offerSummaryFromPayment(payment)

	if summary == nil {
		t.Fatal("TestOfferSummaryFromPayment_Discount: expected non-nil summary")
	}
	if summary.DiscountAmount != 50 {
		t.Fatalf("TestOfferSummaryFromPayment_Discount: expected discount amount 50, got %d", summary.DiscountAmount)
	}
	if summary.CashbackAmount != 0 {
		t.Fatalf("TestOfferSummaryFromPayment_Discount: expected cashback amount 0, got %d", summary.CashbackAmount)
	}
}

func TestPaymentStatusFromIdempotency(t *testing.T) {
	cases := []struct {
		status string
		want   string
	}{
		{"IN_PROGRESS", string(types.PROCESSING)},
		{"FAILED", string(types.FAILED)},
		{"COMPLETED", string(types.SUCCESS)},
		{"UNKNOWN_STATUS", "UNKNOWN_STATUS"},
	}

	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			got := paymentStatusFromIdempotency(tc.status)
			if got != tc.want {
				t.Fatalf("paymentStatusFromIdempotency(%s) = %s, want %s", tc.status, got, tc.want)
			}
		})
	}
}
