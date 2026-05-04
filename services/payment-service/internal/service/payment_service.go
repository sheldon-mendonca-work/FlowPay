package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/types"
	"flowpay/pkg/observability/logger"
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
	TryCreateOrGet(ctx context.Context, idempotency domain.PaymentIdempotencyKey) (domain.PaymentIdempotencyKey, bool, error)
	MarkCompleted(ctx context.Context, idempotencyKey string, responseBody string) error
	MarkFailed(ctx context.Context, idempotencyKey string, errorCode string, errorMessage string) error
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
		return flowpayPaymentErrors.ErrSenderReceiverIDMatching
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
		return fmt.Errorf("%w: sender_id=%s", flowpayPaymentErrors.ErrInsufficientBalance, req.SenderID)
	}

	return nil
}

func logPaymentStepFailure(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string, step string, err error) {
	logger.LogEvent(ctx, "ERROR", constants.ServiceName, "payment_step_failed", logger.Fields{
		"step":            step,
		"idempotency_key": idempotencyKey,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
		"error":           err.Error(),
	})
}

func cachedIdempotencyResult(record domain.PaymentIdempotencyKey) (dto.PaymentResponseDTO, error) {
	switch record.Status {
	case "COMPLETED":
		var cachedResponse dto.PaymentResponseDTO
		if err := json.Unmarshal([]byte(record.ResponseBody), &cachedResponse); err != nil {
			return dto.PaymentResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	case "FAILED":
		return dto.PaymentResponseDTO{}, replayableIdempotencyError(record)
	default:
		return dto.PaymentResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyInProgress, record.IdempotencyKey)
	}
}

func replayableIdempotencyError(record domain.PaymentIdempotencyKey) error {
	switch record.ErrorCode {
	case flowpayPaymentErrors.ErrorTypeInsufficientBalance:
		if record.ErrorMessage != "" {
			return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrInsufficientBalance, record.ErrorMessage)
		}
		return flowpayPaymentErrors.ErrInsufficientBalance
	default:
		if record.ErrorMessage != "" {
			return errors.New(record.ErrorMessage)
		}
		return flowpayPaymentErrors.ErrCreatePaymentFailed
	}
}

func shouldPersistFailedIdempotency(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func (s *PaymentService) CreatePayment(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string) (dto.PaymentResponseDTO, error) {
	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}
	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	idempotencyPayload := domain.PaymentIdempotencyKey{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		Status:         "IN_PROGRESS",
	}
	existingIdempotency, idempotencyKeyCreated, err := s.paymentIdempotencyRepository.TryCreateOrGet(ctx, idempotencyPayload)
	if err != nil {
		logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_create_or_get", err)
		return dto.PaymentResponseDTO{}, err
	}

	if !idempotencyKeyCreated {
		if existingIdempotency.RequestHash != payloadHash {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "payment_idempotency_mismatch", logger.Fields{
				"idempotency_key": idempotencyKey,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeIdempotencyMismatch,
			})
			return dto.PaymentResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyInProgress, idempotencyKey)
			logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_in_progress", err)
			return dto.PaymentResponseDTO{}, err
		}

		cachedResponse, err := cachedIdempotencyResult(existingIdempotency)
		if err != nil {
			logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_cached_result", err)
			return dto.PaymentResponseDTO{}, err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"status":          existingIdempotency.Status,
			"payment_id":      cachedResponse.PaymentID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayPaymentErrors.ErrorTypeNone,
		})
		logger.LogPlain(ctx, constants.ServiceName, "served cached idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, nil
	}

	paymentID, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}
	txClosed := false
	rollbackDueToError := false
	logger.LogPlain(ctx, constants.ServiceName, "started payment transaction idempotency_key=%s sender_id=%s receiver_id=%s", idempotencyKey, req.SenderID, req.ReceiverID)

	defer func() {
		if txClosed {
			return
		}

		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	markFailed := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
			})
		}
		txClosed = true

		if shouldPersistFailedIdempotency(err) {
			if persistErr := s.paymentIdempotencyRepository.MarkFailed(ctx, idempotencyKey, flowpayPaymentErrors.ToPaymentErrorType(err), err.Error()); persistErr != nil {
				logger.LogEvent(ctx, "ERROR", constants.ServiceName, "idempotency_mark_failed_failed", logger.Fields{
					"idempotency_key": idempotencyKey,
					"sender_id":       req.SenderID,
					"receiver_id":     req.ReceiverID,
					"amount":          req.Amount,
					"currency":        req.Currency,
					"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
					"error":           persistErr.Error(),
				})
			}
		}

		return dto.PaymentResponseDTO{}, err
	}

	accounts, err := s.accountRepository.GetAccountsBySenderReceiverId(ctx, tx, req.SenderID, req.ReceiverID)
	if err != nil {
		return markFailed("account_lock_and_load", err)
	}
	senderUser := accounts[req.SenderID]
	receiverUser := accounts[req.ReceiverID]
	amount := req.Amount

	err = validateSenderAndReceiverAccounts(accounts, req, amount)
	if err != nil {
		return markFailed("account_validation", err)
	}

	err = s.accountRepository.UpdateBalanceForSenderAndReceiver(tx, ctx, senderUser.ID, receiverUser.ID, amount)
	if err != nil {
		return markFailed("account_balance_update", err)
	}
	logger.LogPlain(ctx, constants.ServiceName, "updated balances sender_id=%s receiver_id=%s amount=%d", senderUser.ID, receiverUser.ID, amount)

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
		return markFailed("payment_insert", err)
	}
	logger.LogPlain(ctx, constants.ServiceName, "inserted payment row payment_id=%s sender_id=%s receiver_id=%s amount=%d", payment.ID, payment.SenderID, payment.ReceiverID, payment.Amount)

	senderTransaction, err := generateSenderTransaction(payment)
	if err != nil {
		return markFailed("sender_transaction_generate", err)
	}
	receiverTransaction, err := generateReceiverTransaction(payment)
	if err != nil {
		return markFailed("receiver_transaction_generate", err)
	}

	err = s.transactionRepository.CreateTransactionsForSenderAndReceiver(tx, ctx, senderTransaction, receiverTransaction)
	if err != nil {
		return markFailed("transaction_insert", err)
	}
	logger.LogPlain(ctx, constants.ServiceName, "added transaction entries payment_id=%s sender_tx_id=%s receiver_tx_id=%s", payment.ID, senderTransaction.ID, receiverTransaction.ID)

	response := toPaymentResponse(payment)
	responseBody, err := json.Marshal(response)
	if err != nil {
		return markFailed("idempotency_response_encode", fmt.Errorf("encode idempotency response: %w", err))
	}

	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, "tx_commit", err)
		return dto.PaymentResponseDTO{}, err
	}
	txClosed = true
	logger.LogEvent(ctx, "INFO", constants.ServiceName, "payment_tx_committed", logger.Fields{
		"idempotency_key": idempotencyKey,
		"payment_id":      payment.ID,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
	})
	logger.LogPlain(ctx, constants.ServiceName, "committed payment transaction payment_id=%s idempotency_key=%s", payment.ID, idempotencyKey)

	if err := s.paymentIdempotencyRepository.MarkCompleted(ctx, idempotencyKey, string(responseBody)); err != nil {
		logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_mark_completed", err)
		return dto.PaymentResponseDTO{}, err
	}
	logger.LogPlain(ctx, constants.ServiceName, "marked idempotency completed idempotency_key=%s payment_id=%s", idempotencyKey, payment.ID)

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
