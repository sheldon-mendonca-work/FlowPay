package service

import (
	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	"flowpay/payment-service/internal/types"
	"testing"
)

func TestValidateSenderAndReceiverAccounts_SameUser(t *testing.T) {
	accounts := map[string]domain.Account{
		"sender": {
			ID:       "sender",
			UserID:   "user1",
			Balance:  5000,
			Currency: "INR",
		},
		"receiver": {
			ID:       "sender",
			UserID:   "user2",
			Balance:  5000,
			Currency: "INR",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  5000,
			Currency: "INR",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  5000,
			Currency: "INR",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  5000,
			Currency: "INR",
		},
		"receiver": {
			ID:       "receiver",
			UserID:   "user2",
			Balance:  5000,
			Currency: "USD",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  5000,
			Currency: "INR",
		},
		"receiver": {
			ID:       "receiver",
			UserID:   "user2",
			Balance:  5000,
			Currency: "INR",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  50,
			Currency: "INR",
		},
		"receiver": {
			ID:       "receiver",
			UserID:   "user2",
			Balance:  5000,
			Currency: "INR",
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
			ID:       "sender",
			UserID:   "user1",
			Balance:  10000,
			Currency: "INR",
		},
		"receiver": {
			ID:       "receiver",
			UserID:   "user2",
			Balance:  5000,
			Currency: "INR",
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
