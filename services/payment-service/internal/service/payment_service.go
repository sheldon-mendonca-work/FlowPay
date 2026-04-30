package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/types"
	"flowpay/pkg/utils"
)

type PaymentRepository interface {
	CreatePayment(tx *sql.Tx, ctx context.Context, payment domain.Payment) error
}

type AccountRepository interface {
	GetAccountsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderId string, receiverId string) (map[string]domain.Account, error)
	UpdateBalanceForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderID string, receiverID string, amount int64) error
}

type TransactionRepository interface {
	CreateTransactionsForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderTransaction domain.Transaction, receiverTransaction domain.Transaction) error
}

type PaymentIdempotencyRepository interface {
	TryCreateOrGet(tx *sql.Tx, ctx context.Context, idempotency domain.PaymentIdempotencyKey) (domain.PaymentIdempotencyKey, bool, error)
	MarkCompleted(tx *sql.Tx, ctx context.Context, idempotencyKey string, responseBody string) error
}

type PaymentService struct {
	db                           *sql.DB
	paymentRepository            PaymentRepository
	transactionRepository        TransactionRepository
	paymentIdempotencyRepository PaymentIdempotencyRepository
	accountRepository            AccountRepository
}

func NewPaymentService(db *sql.DB, paymentRepository PaymentRepository, transactionRepository TransactionRepository, paymentIdempotencyRepository PaymentIdempotencyRepository, accountRepository AccountRepository) *PaymentService {
	return &PaymentService{
		db:                           db,
		paymentRepository:            paymentRepository,
		transactionRepository:        transactionRepository,
		paymentIdempotencyRepository: paymentIdempotencyRepository,
		accountRepository:            accountRepository,
	}
}

func validateSenderAndReceiverAccounts(accounts map[string]domain.Account, req dto.PaymentRequestDTO, amount int64) error {
	if req.SenderID == req.ReceiverID {
		return fmt.Errorf("sender and receiver cannot be same")
	}

	senderAccount, senderExists := accounts[req.SenderID]
	if !senderExists {
		return fmt.Errorf("sender account is not present: %s", req.SenderID)
	}

	receiverAccount, receiverExists := accounts[req.ReceiverID]
	if !receiverExists {
		return fmt.Errorf("receiver account is not present: %s", req.ReceiverID)
	}

	if senderAccount.Currency != req.Currency {
		return fmt.Errorf("sender account currency not matching with request: %s", req.SenderID)
	}

	if senderAccount.Currency != receiverAccount.Currency {
		return fmt.Errorf("sender or receiver account currency mismatch: %s", req.SenderID)
	}

	if senderAccount.Balance < amount {
		return fmt.Errorf("insufficient balance for sender account: %s", req.SenderID)
	}

	return nil
}

func (s *PaymentService) CreatePayment(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string) (dto.PaymentResponseDTO, error) {
	// If hashing fails or payment id generation fails, no need to go further for now
	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}
	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	paymentID, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	// Start Transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	defer tx.Rollback()

	// handle idempotency
	idempotencyPayload := domain.PaymentIdempotencyKey{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		Status:         "IN_PROGRESS",
	}
	existingIdempotency, idempotencyKeyCreated, err := s.paymentIdempotencyRepository.TryCreateOrGet(tx, ctx, idempotencyPayload)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	idempotencyKeyExists := !idempotencyKeyCreated
	if idempotencyKeyExists && existingIdempotency.RequestHash != payloadHash {
		return dto.PaymentResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyMismatch, idempotencyKey)
	}

	if idempotencyKeyExists && existingIdempotency.Status == "COMPLETED" {
		var cachedResponse dto.PaymentResponseDTO
		if err := json.Unmarshal([]byte(existingIdempotency.ResponseBody), &cachedResponse); err != nil {
			return dto.PaymentResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	}

	if idempotencyKeyExists {
		return dto.PaymentResponseDTO{}, fmt.Errorf("idempotency key already in progress: %s", idempotencyKey)
	}

	// handle account check and amount checks
	accounts, err := s.accountRepository.GetAccountsBySenderReceiverId(ctx, tx, req.SenderID, req.ReceiverID)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}
	senderUser := accounts[req.SenderID]
	receiverUser := accounts[req.ReceiverID]
	amount := int64(math.Round(req.Amount * 100))

	err = validateSenderAndReceiverAccounts(accounts, req, amount)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	// update entry in accounts table

	err = s.accountRepository.UpdateBalanceForSenderAndReceiver(tx, ctx, senderUser.ID, receiverUser.ID, amount)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	// update entry in payments table
	payment := domain.Payment{
		ID:         paymentID,
		SenderID:   senderUser.ID,
		ReceiverID: receiverUser.ID,
		Amount:     amount,
		Currency:   req.Currency,
		Status:     string(types.SUCCESS),
	}

	err = s.paymentRepository.CreatePayment(tx, ctx, payment)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	// update entries in transaction table
	senderTransaction, err := generateSenderTransaction(payment)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}
	receiverTransaction, err := generateReceiverTransaction(payment)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	err = s.transactionRepository.CreateTransactionsForSenderAndReceiver(tx, ctx, senderTransaction, receiverTransaction)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	response := toPaymentResponse(payment)
	responseBody, err := json.Marshal(response)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("encode idempotency response: %w", err)
	}
	if err := s.paymentIdempotencyRepository.MarkCompleted(tx, ctx, idempotencyKey, string(responseBody)); err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	if err := tx.Commit(); err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	return response, nil
}

func toPaymentResponse(payment domain.Payment) dto.PaymentResponseDTO {
	return dto.PaymentResponseDTO{
		PaymentID: payment.ID,
		Status:    types.PaymentStatusEnum(payment.Status),
	}
}

func newPaymentID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate payment id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func generateSenderTransaction(payment domain.Payment) (domain.Transaction, error) {
	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}
	return domain.Transaction{
		ID:        transactionID,
		PaymentID: payment.ID,
		AccountID: payment.SenderID,
		Type:      "DEBIT",
		Amount:    payment.Amount,
		Currency:  payment.Currency,
		Status:    "SUCCESS",
	}, nil
}

func generateReceiverTransaction(payment domain.Payment) (domain.Transaction, error) {
	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}
	return domain.Transaction{
		ID:        transactionID,
		PaymentID: payment.ID,
		AccountID: payment.ReceiverID,
		Type:      "CREDIT",
		Amount:    payment.Amount,
		Currency:  payment.Currency,
		Status:    "SUCCESS",
	}, nil
}
