package service

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/infra"
	"flowpay/payment-service/internal/repository"
	"flowpay/payment-service/internal/types"
)

var integrationDB *sql.DB

func TestMain(m *testing.M) {
	if os.Getenv("RUN_DB_TESTS") == "1" {
		integrationDB = infra.InitDB()
		defer integrationDB.Close()
	}

	os.Exit(m.Run())
}

func setupIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()

	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("set RUN_DB_TESTS=1 to run db integration tests")
	}

	if integrationDB == nil {
		t.Fatal("setupIntegrationDB: integration db was not initialized")
	}

	return integrationDB
}

func createPaymentService(db *sql.DB) *PaymentService {
	return NewPaymentService(
		db,
		repository.NewPaymentRepository(db),
		repository.NewTransactionRepository(db),
		repository.NewPaymentIdempotencyRepository(db),
		repository.NewAccountRepository(db),
	)
}

func createTwoAccounts(t *testing.T, db *sql.DB) (domain.Account, domain.Account) {
	t.Helper()

	accountRepository := repository.NewAccountRepository(db)

	senderAccount := domain.Account{
		ID:       "11111111-1111-1111-1111-111111111111",
		UserID:   "integration-sender",
		Balance:  100000,
		Currency: "INR",
	}

	receiverAccount := domain.Account{
		ID:       "22222222-2222-2222-2222-222222222222",
		UserID:   "integration-receiver",
		Balance:  25000,
		Currency: "INR",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cleanupPaymentArtifacts(t, db, senderAccount.ID, receiverAccount.ID)
	cleanupAccounts(t, db, senderAccount.ID, receiverAccount.ID)

	if err := accountRepository.CreateAccount(ctx, senderAccount); err != nil {
		t.Fatalf("createTwoAccounts: failed to create sender account: %v", err)
	}

	if err := accountRepository.CreateAccount(ctx, receiverAccount); err != nil {
		t.Fatalf("createTwoAccounts: failed to create receiver account: %v", err)
	}

	t.Cleanup(func() {
		cleanupPaymentArtifacts(t, db, senderAccount.ID, receiverAccount.ID)
		cleanupAccounts(t, db, senderAccount.ID, receiverAccount.ID)
	})

	return senderAccount, receiverAccount
}

func cleanupAccounts(t *testing.T, db *sql.DB, senderID string, receiverID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, "DELETE FROM accounts WHERE id IN ($1, $2)", senderID, receiverID); err != nil {
		t.Fatalf("cleanupAccounts: failed to delete test accounts: %v", err)
	}
}

func cleanupPaymentArtifacts(t *testing.T, db *sql.DB, senderID string, receiverID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT id FROM payments WHERE sender_id IN ($1, $2) OR receiver_id IN ($1, $2)", senderID, receiverID)
	if err != nil {
		t.Fatalf("cleanupPaymentArtifacts: failed to query payments: %v", err)
	}
	defer rows.Close()

	var paymentIDs []string
	for rows.Next() {
		var paymentID string
		if err := rows.Scan(&paymentID); err != nil {
			t.Fatalf("cleanupPaymentArtifacts: failed to scan payment id: %v", err)
		}
		paymentIDs = append(paymentIDs, paymentID)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("cleanupPaymentArtifacts: failed while iterating payments: %v", err)
	}

	for _, paymentID := range paymentIDs {
		if _, err := db.ExecContext(ctx, "DELETE FROM transactions WHERE payment_id = $1", paymentID); err != nil {
			t.Fatalf("cleanupPaymentArtifacts: failed to delete transactions: %v", err)
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM payments WHERE id = $1", paymentID); err != nil {
			t.Fatalf("cleanupPaymentArtifacts: failed to delete payments: %v", err)
		}
	}

	if _, err := db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE idempotency_key LIKE 'integration-%'"); err != nil {
		t.Fatalf("cleanupPaymentArtifacts: failed to delete idempotency keys: %v", err)
	}
}

func getAccountBalance(t *testing.T, db *sql.DB, accountID string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var balance int64
	if err := db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1", accountID).Scan(&balance); err != nil {
		t.Fatalf("getAccountBalance: failed to fetch account balance: %v", err)
	}

	return balance
}

func countPaymentRows(t *testing.T, db *sql.DB, senderID string, receiverID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM payments WHERE sender_id = $1 AND receiver_id = $2",
		senderID,
		receiverID,
	).Scan(&count); err != nil {
		t.Fatalf("countPaymentRows: failed to count payments: %v", err)
	}

	return count
}

func countTransactionRowsForPayment(t *testing.T, db *sql.DB, paymentID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE payment_id = $1", paymentID).Scan(&count); err != nil {
		t.Fatalf("countTransactionRowsForPayment: failed to count transactions: %v", err)
	}

	return count
}

func countIdempotencyRows(t *testing.T, db *sql.DB, key string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = $1", key).Scan(&count); err != nil {
		t.Fatalf("countIdempotencyRows: failed to count idempotency rows: %v", err)
	}

	return count
}

func getIdempotencyRecord(t *testing.T, db *sql.DB, key string) domain.PaymentIdempotencyKey {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var record domain.PaymentIdempotencyKey
	err := db.QueryRowContext(
		ctx,
		"SELECT idempotency_key, request_hash, COALESCE(response_body::text, ''), status, COALESCE(error_code, ''), COALESCE(error_message, '') FROM idempotency_keys WHERE idempotency_key = $1",
		key,
	).Scan(&record.IdempotencyKey, &record.RequestHash, &record.ResponseBody, &record.Status, &record.ErrorCode, &record.ErrorMessage)
	if err != nil {
		t.Fatalf("getIdempotencyRecord: failed to fetch idempotency record: %v", err)
	}

	return record
}

func TestIntegrationDBSetup_CreateTwoAccounts(t *testing.T) {
	db := setupIntegrationDB(t)
	senderAccount, receiverAccount := createTwoAccounts(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var accountCount int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM accounts WHERE id IN ($1, $2)",
		senderAccount.ID,
		receiverAccount.ID,
	).Scan(&accountCount)
	if err != nil {
		t.Fatalf("TestIntegrationDBSetup_CreateTwoAccounts: failed to count seeded accounts: %v", err)
	}

	if accountCount != 2 {
		t.Fatalf("TestIntegrationDBSetup_CreateTwoAccounts: expected 2 accounts, got %d", accountCount)
	}
}

func TestCreatePayment_Success(t *testing.T) {
	db := setupIntegrationDB(t)
	paymentService := createPaymentService(db)
	senderAccount, receiverAccount := createTwoAccounts(t, db)

	req := dto.PaymentRequestDTO{
		SenderID:   senderAccount.ID,
		ReceiverID: receiverAccount.ID,
		Amount:     12550,
		Currency:   "INR",
	}

	response, err := paymentService.CreatePayment(context.Background(), req, "integration-success")
	if err != nil {
		t.Fatalf("TestCreatePayment_Success: expected nil error, got %v", err)
	}

	if response.PaymentID == "" {
		t.Fatal("TestCreatePayment_Success: expected non-empty payment id")
	}

	if response.Status != types.SUCCESS {
		t.Fatalf("TestCreatePayment_Success: expected status %s, got %s", types.SUCCESS, response.Status)
	}

	senderBalance := getAccountBalance(t, db, senderAccount.ID)
	receiverBalance := getAccountBalance(t, db, receiverAccount.ID)

	if senderBalance != 87450 {
		t.Fatalf("TestCreatePayment_Success: expected sender balance 87450, got %d", senderBalance)
	}

	if receiverBalance != 37550 {
		t.Fatalf("TestCreatePayment_Success: expected receiver balance 37550, got %d", receiverBalance)
	}

	if countPaymentRows(t, db, senderAccount.ID, receiverAccount.ID) != 1 {
		t.Fatal("TestCreatePayment_Success: expected exactly one payment row")
	}

	if countTransactionRowsForPayment(t, db, response.PaymentID) != 2 {
		t.Fatal("TestCreatePayment_Success: expected exactly two transaction rows")
	}

	record := getIdempotencyRecord(t, db, "integration-success")
	if record.Status != "COMPLETED" {
		t.Fatalf("TestCreatePayment_Success: expected idempotency status COMPLETED, got %s", record.Status)
	}
}

func TestCreatePayment_Idempotent(t *testing.T) {
	db := setupIntegrationDB(t)
	paymentService := createPaymentService(db)
	senderAccount, receiverAccount := createTwoAccounts(t, db)

	req := dto.PaymentRequestDTO{
		SenderID:   senderAccount.ID,
		ReceiverID: receiverAccount.ID,
		Amount:     12550,
		Currency:   "INR",
	}

	firstResponse, err := paymentService.CreatePayment(context.Background(), req, "integration-idempotent")
	if err != nil {
		t.Fatalf("TestCreatePayment_Idempotent: expected first call to succeed, got %v", err)
	}

	secondResponse, err := paymentService.CreatePayment(context.Background(), req, "integration-idempotent")
	if err != nil {
		t.Fatalf("TestCreatePayment_Idempotent: expected second call to succeed, got %v", err)
	}

	if firstResponse.PaymentID != secondResponse.PaymentID {
		t.Fatalf("TestCreatePayment_Idempotent: expected same payment id, got %s and %s", firstResponse.PaymentID, secondResponse.PaymentID)
	}

	if countPaymentRows(t, db, senderAccount.ID, receiverAccount.ID) != 1 {
		t.Fatal("TestCreatePayment_Idempotent: expected exactly one payment row")
	}

	if countTransactionRowsForPayment(t, db, firstResponse.PaymentID) != 2 {
		t.Fatal("TestCreatePayment_Idempotent: expected exactly two transaction rows")
	}

	if countIdempotencyRows(t, db, "integration-idempotent") != 1 {
		t.Fatal("TestCreatePayment_Idempotent: expected exactly one idempotency row")
	}
}

func TestCreatePayment_IdempotencyMismatch(t *testing.T) {
	db := setupIntegrationDB(t)
	paymentService := createPaymentService(db)
	senderAccount, receiverAccount := createTwoAccounts(t, db)

	firstReq := dto.PaymentRequestDTO{
		SenderID:   senderAccount.ID,
		ReceiverID: receiverAccount.ID,
		Amount:     12550,
		Currency:   "INR",
	}

	secondReq := dto.PaymentRequestDTO{
		SenderID:   senderAccount.ID,
		ReceiverID: receiverAccount.ID,
		Amount:     15025,
		Currency:   "INR",
	}

	firstResponse, err := paymentService.CreatePayment(context.Background(), firstReq, "integration-mismatch")
	if err != nil {
		t.Fatalf("TestCreatePayment_IdempotencyMismatch: expected first call to succeed, got %v", err)
	}

	_, err = paymentService.CreatePayment(context.Background(), secondReq, "integration-mismatch")
	if err == nil {
		t.Fatal("TestCreatePayment_IdempotencyMismatch: expected error for mismatched idempotency request")
	}

	if !errors.Is(err, flowpayPaymentErrors.ErrIdempotencyMismatch) {
		t.Fatalf("TestCreatePayment_IdempotencyMismatch: expected ErrIdempotencyMismatch, got %v", err)
	}

	if countPaymentRows(t, db, senderAccount.ID, receiverAccount.ID) != 1 {
		t.Fatal("TestCreatePayment_IdempotencyMismatch: expected exactly one payment row")
	}

	if countTransactionRowsForPayment(t, db, firstResponse.PaymentID) != 2 {
		t.Fatal("TestCreatePayment_IdempotencyMismatch: expected exactly two transaction rows")
	}
}

func TestCreatePayment_InsufficientBalance(t *testing.T) {
	db := setupIntegrationDB(t)
	paymentService := createPaymentService(db)
	senderAccount, receiverAccount := createTwoAccounts(t, db)

	req := dto.PaymentRequestDTO{
		SenderID:   senderAccount.ID,
		ReceiverID: receiverAccount.ID,
		Amount:     100001,
		Currency:   "INR",
	}

	_, err := paymentService.CreatePayment(context.Background(), req, "integration-insufficient")
	if err == nil {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected insufficient balance error")
	}

	if getAccountBalance(t, db, senderAccount.ID) != senderAccount.Balance {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected sender balance to remain unchanged")
	}

	if getAccountBalance(t, db, receiverAccount.ID) != receiverAccount.Balance {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected receiver balance to remain unchanged")
	}

	if countPaymentRows(t, db, senderAccount.ID, receiverAccount.ID) != 0 {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected no payment rows")
	}

	if countIdempotencyRows(t, db, "integration-insufficient") != 1 {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected one idempotency row")
	}

	record := getIdempotencyRecord(t, db, "integration-insufficient")
	if record.Status != "FAILED" {
		t.Fatalf("TestCreatePayment_InsufficientBalance: expected idempotency status FAILED, got %s", record.Status)
	}

	if record.ResponseBody != "" {
		t.Fatalf("TestCreatePayment_InsufficientBalance: expected empty response body, got %s", record.ResponseBody)
	}

	if record.ErrorCode != flowpayPaymentErrors.ErrorTypeInsufficientBalance {
		t.Fatalf("TestCreatePayment_InsufficientBalance: expected error code %s, got %s", flowpayPaymentErrors.ErrorTypeInsufficientBalance, record.ErrorCode)
	}

	_, secondErr := paymentService.CreatePayment(context.Background(), req, "integration-insufficient")
	if secondErr == nil {
		t.Fatal("TestCreatePayment_InsufficientBalance: expected cached insufficient balance error on retry")
	}

	if !errors.Is(secondErr, flowpayPaymentErrors.ErrInsufficientBalance) {
		t.Fatalf("TestCreatePayment_InsufficientBalance: expected ErrInsufficientBalance on retry, got %v", secondErr)
	}
}
