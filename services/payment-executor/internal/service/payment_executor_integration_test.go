package service

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"flowpay/payment-executor/internal/domain"
	"flowpay/payment-executor/internal/infra"
	"flowpay/payment-executor/internal/repository"
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

func createPaymentExecutorService(db *sql.DB) *PaymentExecutorService {
	return NewPaymentExecutorService(
		db,
		repository.NewAccountRepository(db),
		repository.NewPaymentRepository(db),
		repository.NewTransactionRepository(db),
		repository.NewPaymentIdempotencyRepository(db),
		repository.NewOutboxEventsRepository(db),
	)
}

func seedAccountsAndIdempotency(t *testing.T, db *sql.DB, event domain.PaymentInitiatedEvent) (domain.Account, domain.Account) {
	t.Helper()

	senderAccount := domain.Account{
		ID:       event.SenderID,
		UserID:   "executor-integration-sender",
		Balance:  100000,
		Currency: event.Currency,
	}

	receiverAccount := domain.Account{
		ID:       event.ReceiverID,
		UserID:   "executor-integration-receiver",
		Balance:  25000,
		Currency: event.Currency,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cleanupExecutorArtifacts(t, db, event)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO accounts (id, user_id, balance, currency, created_at, updated_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW()), ($5, $6, $7, $8, NOW(), NOW())
	`, senderAccount.ID, senderAccount.UserID, senderAccount.Balance, senderAccount.Currency, receiverAccount.ID, receiverAccount.UserID, receiverAccount.Balance, receiverAccount.Currency); err != nil {
		t.Fatalf("seedAccountsAndIdempotency: failed to insert accounts: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO idempotency_keys (
			idempotency_key,
			request_hash,
			response_body,
			status,
			error_code,
			error_message,
			owner_token,
			payment_id,
			locked_until,
			created_at,
			updated_at
		) VALUES ($1, $2, NULL, 'IN_PROGRESS', NULL, NULL, $3, $4::uuid, NOW(), NOW(), NOW())
	`, event.IdempotencyKey, "executor-integration-hash", event.OwnerToken, event.ID); err != nil {
		t.Fatalf("seedAccountsAndIdempotency: failed to insert idempotency row: %v", err)
	}

	t.Cleanup(func() {
		cleanupExecutorArtifacts(t, db, event)
	})

	return senderAccount, receiverAccount
}

func cleanupExecutorArtifacts(t *testing.T, db *sql.DB, event domain.PaymentInitiatedEvent) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, "DELETE FROM transactions WHERE payment_id = $1::uuid", event.ID); err != nil {
		t.Fatalf("cleanupExecutorArtifacts: failed to delete transactions: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM payments WHERE id = $1::uuid", event.ID); err != nil {
		t.Fatalf("cleanupExecutorArtifacts: failed to delete payments: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE idempotency_key = $1", event.IdempotencyKey); err != nil {
		t.Fatalf("cleanupExecutorArtifacts: failed to delete idempotency row: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM accounts WHERE id IN ($1::uuid, $2::uuid)", event.SenderID, event.ReceiverID); err != nil {
		t.Fatalf("cleanupExecutorArtifacts: failed to delete accounts: %v", err)
	}
}

func getAccountBalance(t *testing.T, db *sql.DB, accountID string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var balance int64
	if err := db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1::uuid", accountID).Scan(&balance); err != nil {
		t.Fatalf("getAccountBalance: failed to fetch account balance: %v", err)
	}

	return balance
}

func countPayments(t *testing.T, db *sql.DB, paymentID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM payments WHERE id = $1::uuid", paymentID).Scan(&count); err != nil {
		t.Fatalf("countPayments: failed to count payments: %v", err)
	}

	return count
}

func countTransactions(t *testing.T, db *sql.DB, paymentID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transactions WHERE payment_id = $1::uuid", paymentID).Scan(&count); err != nil {
		t.Fatalf("countTransactions: failed to count transactions: %v", err)
	}

	return count
}

func idempotencyStatus(t *testing.T, db *sql.DB, key string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var status string
	if err := db.QueryRowContext(ctx, "SELECT status FROM idempotency_keys WHERE idempotency_key = $1", key).Scan(&status); err != nil {
		t.Fatalf("idempotencyStatus: failed to fetch idempotency status: %v", err)
	}

	return status
}

func TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances(t *testing.T) {
	db := setupIntegrationDB(t)
	paymentExecutorService := createPaymentExecutorService(db)

	event := domain.PaymentInitiatedEvent{
		ID:             "33333333-3333-4333-8333-333333333333",
		SenderID:       "11111111-1111-1111-1111-111111111111",
		ReceiverID:     "22222222-2222-2222-2222-222222222222",
		IdempotencyKey: "integration-executor-redelivery",
		OwnerToken:     "owner-token-redelivery",
		TraceID:        "trace-executor-redelivery",
		RequestID:      "request-executor-redelivery",
		RetryCount:     1,
		Amount:         12550,
		Currency:       "INR",
		CreatedAt:      time.Now(),
	}

	senderAccount, receiverAccount := seedAccountsAndIdempotency(t, db, event)

	firstResponse, err := paymentExecutorService.ExecutePayment(context.Background(), event)
	if err != nil {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: first execution failed: %v", err)
	}

	secondResponse, err := paymentExecutorService.ExecutePayment(context.Background(), event)
	if err != nil {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: replay execution failed: %v", err)
	}

	if firstResponse.PaymentID != event.ID {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected first payment id %s, got %s", event.ID, firstResponse.PaymentID)
	}

	if secondResponse.PaymentID != event.ID {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected second payment id %s, got %s", event.ID, secondResponse.PaymentID)
	}

	if got := getAccountBalance(t, db, senderAccount.ID); got != 87450 {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected sender balance 87450, got %d", got)
	}

	if got := getAccountBalance(t, db, receiverAccount.ID); got != 37550 {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected receiver balance 37550, got %d", got)
	}

	if got := countPayments(t, db, event.ID); got != 1 {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected 1 payment row, got %d", got)
	}

	if got := countTransactions(t, db, event.ID); got != 2 {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected 2 transaction rows, got %d", got)
	}

	if got := idempotencyStatus(t, db, event.IdempotencyKey); got != "COMPLETED" {
		t.Fatalf("TestExecutePayment_RedeliveryAfterCommit_DoesNotDoubleApplyBalances: expected idempotency status COMPLETED, got %s", got)
	}
}
