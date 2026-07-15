package service

import (
	"errors"
	"flowpay/payment-executor/internal/domain"
	flowpayPaymentErrors "flowpay/payment-executor/internal/errors"
	"fmt"
	"testing"
)

func TestShouldPersistFailedIdempotency(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"business failure", flowpayPaymentErrors.ErrInsufficientBalance, true},
		{"wrapped business failure", fmt.Errorf("context: %w", flowpayPaymentErrors.ErrSenderAccountNotFound), true},
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
	businessErr := flowpayPaymentErrors.ErrAccountCurrencyMismatch
	technicalErr := errors.New("db timeout")

	if !isDeterministicBusinessFailure(businessErr) {
		t.Fatal("TestIsDeterministicBusinessFailure_MatchesShouldPersist: expected true for business failure")
	}
	if isDeterministicBusinessFailure(technicalErr) {
		t.Fatal("TestIsDeterministicBusinessFailure_MatchesShouldPersist: expected false for technical failure")
	}
}

func TestValidateSenderAndReceiverAccounts_SameUser(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {ID: "sender", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "sender", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrSenderReceiverIDMatching) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_SameUser: expected ErrSenderReceiverIDMatching, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_SenderAccountMissing(t *testing.T) {
	accounts := map[string]domain.Account{
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrSenderAccountNotFound) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_SenderAccountMissing: expected ErrSenderAccountNotFound, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_ReceiverAccountMissing(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {ID: "sender", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrReceiverAccountNotFound) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_ReceiverAccountMissing: expected ErrReceiverAccountNotFound, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_SenderCurrencyMismatch(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 5000, Currency: "USD"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "USD"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrSenderCurrencyMismatch) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_SenderCurrencyMismatch: expected ErrSenderCurrencyMismatch, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_AccountCurrencyMismatch(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 5000, Currency: "INR"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "USD"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrAccountCurrencyMismatch) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_AccountCurrencyMismatch: expected ErrAccountCurrencyMismatch, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_InsufficientBalance(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 40, Currency: "INR"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if !errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_InsufficientBalance: expected ErrInsufficientBalance, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_InsufficientBalanceAfterDiscount(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 30, Currency: "INR"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	// amount 100, discount 60 => net 40 owed, still exceeds balance of 30
	err := validateSenderAndReceiverAccounts(accounts, event, 100, 60)

	if !errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_InsufficientBalanceAfterDiscount: expected ErrInsufficientBalance, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_DiscountCoversShortfall(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 30, Currency: "INR"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	// amount 100, discount 80 => net 20 owed, within balance of 30
	err := validateSenderAndReceiverAccounts(accounts, event, 100, 80)

	if err != nil {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_DiscountCoversShortfall: expected nil error, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_AllowsNegativeBalance(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 0, Currency: "INR", AllowNegativeBalance: true},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 5000, 0)

	if err != nil {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_AllowsNegativeBalance: expected nil error when sender allows negative balance, got %v", err)
	}
}

func TestValidateSenderAndReceiverAccounts_ValidAccounts(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender":   {ID: "sender", Balance: 10000, Currency: "INR"},
		"receiver": {ID: "receiver", Balance: 5000, Currency: "INR"},
	}
	event := domain.PaymentInitiatedEvent{SenderID: "sender", ReceiverID: "receiver", Currency: "INR"}

	err := validateSenderAndReceiverAccounts(accounts, event, 50, 0)

	if err != nil {
		t.Fatalf("TestValidateSenderAndReceiverAccounts_ValidAccounts: expected nil error, got %v", err)
	}
}

func TestGetOfferToBeRedeemed_NilOffer(t *testing.T) {
	if got := getOfferToBeRedeemed(1000, nil); got != 0 {
		t.Fatalf("TestGetOfferToBeRedeemed_NilOffer: expected 0, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_BelowMinimumAmount(t *testing.T) {
	offer := &domain.OfferReservedEvent{MinimumPaymentAmount: 500}

	if got := getOfferToBeRedeemed(100, offer); got != 0 {
		t.Fatalf("TestGetOfferToBeRedeemed_BelowMinimumAmount: expected 0, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_FixedAmountOffer(t *testing.T) {
	fixed := int64(200)
	offer := &domain.OfferReservedEvent{OfferAmount: &fixed}

	if got := getOfferToBeRedeemed(1000, offer); got != 200 {
		t.Fatalf("TestGetOfferToBeRedeemed_FixedAmountOffer: expected 200, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_FixedAmountOfferCappedByAmount(t *testing.T) {
	fixed := int64(2000)
	offer := &domain.OfferReservedEvent{OfferAmount: &fixed}

	if got := getOfferToBeRedeemed(500, offer); got != 500 {
		t.Fatalf("TestGetOfferToBeRedeemed_FixedAmountOfferCappedByAmount: expected 500, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_PercentageOffer(t *testing.T) {
	pct := int16(10)
	offer := &domain.OfferReservedEvent{OfferPercentage: &pct}

	if got := getOfferToBeRedeemed(1000, offer); got != 100 {
		t.Fatalf("TestGetOfferToBeRedeemed_PercentageOffer: expected 100, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_PercentageOfferCappedByMaxBenefit(t *testing.T) {
	pct := int16(50)
	offer := &domain.OfferReservedEvent{OfferPercentage: &pct, MaxBenefitAmount: 50}

	if got := getOfferToBeRedeemed(1000, offer); got != 50 {
		t.Fatalf("TestGetOfferToBeRedeemed_PercentageOfferCappedByMaxBenefit: expected 50, got %d", got)
	}
}

func TestGetOfferToBeRedeemed_NoOfferAmountOrPercentage(t *testing.T) {
	offer := &domain.OfferReservedEvent{}

	if got := getOfferToBeRedeemed(1000, offer); got != 0 {
		t.Fatalf("TestGetOfferToBeRedeemed_NoOfferAmountOrPercentage: expected 0, got %d", got)
	}
}

func TestMapPaymentSuccessToOutbox_NoOffer(t *testing.T) {
	event := domain.PaymentSuccessEvent{
		PaymentID:      "payment-1",
		IdempotencyKey: "idem-1",
	}

	outboxEvent, err := MapPaymentSuccessToOutbox(event, 1, "trace-1", "request-1")

	if err != nil {
		t.Fatalf("TestMapPaymentSuccessToOutbox_NoOffer: expected nil error, got %v", err)
	}
	if outboxEvent.EventType != "payment_success" {
		t.Fatalf("TestMapPaymentSuccessToOutbox_NoOffer: expected event type 'payment_success', got %s", outboxEvent.EventType)
	}
	if outboxEvent.AggregateID != event.PaymentID {
		t.Fatalf("TestMapPaymentSuccessToOutbox_NoOffer: expected aggregate id %s, got %s", event.PaymentID, outboxEvent.AggregateID)
	}
	if outboxEvent.RetryCount != 1 {
		t.Fatalf("TestMapPaymentSuccessToOutbox_NoOffer: expected retry count 1, got %d", outboxEvent.RetryCount)
	}
}

func TestMapPaymentSuccessToOutbox_WithOffer(t *testing.T) {
	offerID := "offer-1"
	event := domain.PaymentSuccessEvent{
		PaymentID: "payment-1",
		OfferID:   &offerID,
	}

	outboxEvent, err := MapPaymentSuccessToOutbox(event, 0, "trace-1", "request-1")

	if err != nil {
		t.Fatalf("TestMapPaymentSuccessToOutbox_WithOffer: expected nil error, got %v", err)
	}
	if outboxEvent.EventType != "offer_applicable_payment_success" {
		t.Fatalf("TestMapPaymentSuccessToOutbox_WithOffer: expected event type 'offer_applicable_payment_success', got %s", outboxEvent.EventType)
	}
}

func TestGenerateSenderTransaction_BuildsDebitTransaction(t *testing.T) {
	payment := domain.Payment{ID: "payment-1", SenderID: "sender-1", Amount: 500, Currency: "INR"}

	tx, err := generateSenderTransaction(payment, "PAYMENT")

	if err != nil {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected nil error, got %v", err)
	}
	if tx.AccountID != payment.SenderID {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected account id %s, got %s", payment.SenderID, tx.AccountID)
	}
	if tx.Type != "DEBIT" {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected type DEBIT, got %s", tx.Type)
	}
	if tx.TransactionCategory != "PAYMENT" {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected category PAYMENT, got %s", tx.TransactionCategory)
	}
	if tx.Amount != payment.Amount {
		t.Fatalf("TestGenerateSenderTransaction_BuildsDebitTransaction: expected amount %d, got %d", payment.Amount, tx.Amount)
	}
}

func TestGenerateReceiverTransaction_BuildsCreditTransaction(t *testing.T) {
	payment := domain.Payment{ID: "payment-1", ReceiverID: "receiver-1", Amount: 500, Currency: "INR"}

	tx, err := generateReceiverTransaction(payment, "PAYMENT")

	if err != nil {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected nil error, got %v", err)
	}
	if tx.AccountID != payment.ReceiverID {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected account id %s, got %s", payment.ReceiverID, tx.AccountID)
	}
	if tx.Type != "CREDIT" {
		t.Fatalf("TestGenerateReceiverTransaction_BuildsCreditTransaction: expected type CREDIT, got %s", tx.Type)
	}
}

func TestGeneratePromotionPoolDebitTransaction(t *testing.T) {
	payment := domain.Payment{ID: "payment-1", OfferAmount: 200, Currency: "INR"}

	tx, err := generatePromotionPoolDebitTransaction(payment, "pool-account-1")

	if err != nil {
		t.Fatalf("TestGeneratePromotionPoolDebitTransaction: expected nil error, got %v", err)
	}
	if tx.AccountID != "pool-account-1" {
		t.Fatalf("TestGeneratePromotionPoolDebitTransaction: expected account id pool-account-1, got %s", tx.AccountID)
	}
	if tx.Type != "DEBIT" {
		t.Fatalf("TestGeneratePromotionPoolDebitTransaction: expected type DEBIT, got %s", tx.Type)
	}
	if tx.TransactionCategory != "CASHBACK" {
		t.Fatalf("TestGeneratePromotionPoolDebitTransaction: expected category CASHBACK, got %s", tx.TransactionCategory)
	}
	if tx.Amount != payment.OfferAmount {
		t.Fatalf("TestGeneratePromotionPoolDebitTransaction: expected amount %d, got %d", payment.OfferAmount, tx.Amount)
	}
}

func TestGenerateCashbackCreditTransaction(t *testing.T) {
	payment := domain.Payment{ID: "payment-1", SenderID: "sender-1", OfferAmount: 200, Currency: "INR"}

	tx, err := generateCashbackCreditTransaction(payment)

	if err != nil {
		t.Fatalf("TestGenerateCashbackCreditTransaction: expected nil error, got %v", err)
	}
	if tx.AccountID != payment.SenderID {
		t.Fatalf("TestGenerateCashbackCreditTransaction: expected account id %s, got %s", payment.SenderID, tx.AccountID)
	}
	if tx.Type != "CREDIT" {
		t.Fatalf("TestGenerateCashbackCreditTransaction: expected type CREDIT, got %s", tx.Type)
	}
	if tx.TransactionCategory != "CASHBACK" {
		t.Fatalf("TestGenerateCashbackCreditTransaction: expected category CASHBACK, got %s", tx.TransactionCategory)
	}
	if tx.Amount != payment.OfferAmount {
		t.Fatalf("TestGenerateCashbackCreditTransaction: expected amount %d, got %d", payment.OfferAmount, tx.Amount)
	}
}

func TestNewPaymentID_GeneratesUUIDLikeValue(t *testing.T) {
	id, err := newPaymentID()

	if err != nil {
		t.Fatalf("TestNewPaymentID_GeneratesUUIDLikeValue: expected nil error, got %v", err)
	}
	if len(id) != 36 {
		t.Fatalf("TestNewPaymentID_GeneratesUUIDLikeValue: expected id length 36, got %d", len(id))
	}
}
