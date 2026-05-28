package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flowpay/payment-executor/internal/constants"
	"flowpay/payment-executor/internal/domain"
	"flowpay/payment-executor/internal/dto"
	flowpayPaymentErrors "flowpay/payment-executor/internal/errors"
	"flowpay/payment-executor/internal/types"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"
	"fmt"
)

type AccountRepository interface {
	GetAccounsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderID string, receiverID string) (map[string]domain.Account, error)
	UpdateBalanceForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderID string, receiverID string, amount int64) error
}

type PaymentRepository interface {
	CreatePayment(tx *sql.Tx, ctx context.Context, payment domain.Payment) error
	GetByPaymentIdAndIdempotencyKey(ctx context.Context, paymentID string, idempotencyKey string) (domain.Payment, error)
}

type TransactionRepository interface {
	CreateTransactionsForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderTransaction domain.Transaction, receiverTransaction domain.Transaction) error
}

type IdempotencyRepository interface {
	GetByKey(
		ctx context.Context,
		key string,
	) (domain.PaymentIdempotencyKey, error)
	MarkCompleted(tx *sql.Tx, ctx context.Context, idempotencyKey string, responseBody string, paymentID string, ownerToken string) error
	MarkFailed(tx *sql.Tx, ctx context.Context, idempotencyKey string, errorCode string, errorMessage string, ownerToken string) error
}

type OutboxEventsRepository interface {
}

type PaymentExecutorService struct {
	db                     *sql.DB
	accountRepository      AccountRepository
	paymentRepository      PaymentRepository
	transactionRepository  TransactionRepository
	outboxEventsRepository OutboxEventsRepository
	idempotencyRepository  IdempotencyRepository
}

func NewPaymentExecutorService(db *sql.DB, accountRepository AccountRepository, paymentRepository PaymentRepository, transactionRepository TransactionRepository, idempotencyRepository IdempotencyRepository, outboxEventsRepository OutboxEventsRepository) *PaymentExecutorService {
	return &PaymentExecutorService{
		db:                     db,
		paymentRepository:      paymentRepository,
		transactionRepository:  transactionRepository,
		accountRepository:      accountRepository,
		idempotencyRepository:  idempotencyRepository,
		outboxEventsRepository: outboxEventsRepository,
	}
}

func shouldPersistFailedIdempotency(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, flowpayPaymentErrors.ErrSenderIDRequired),
		errors.Is(err, flowpayPaymentErrors.ErrReceiverIDRequired),
		errors.Is(err, flowpayPaymentErrors.ErrSenderReceiverIDMatching),
		errors.Is(err, flowpayPaymentErrors.ErrAmountMustBeGreaterThanZero),
		errors.Is(err, flowpayPaymentErrors.ErrCurrencyRequired),
		errors.Is(err, flowpayPaymentErrors.ErrIdempotencyKeyRequired),
		errors.Is(err, flowpayPaymentErrors.ErrSenderAccountNotFound),
		errors.Is(err, flowpayPaymentErrors.ErrReceiverAccountNotFound),
		errors.Is(err, flowpayPaymentErrors.ErrSenderCurrencyMismatch),
		errors.Is(err, flowpayPaymentErrors.ErrAccountCurrencyMismatch),
		errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance):
		return true
	default:
		return false
	}
}

func isDeterministicBusinessFailure(err error) bool {
	return shouldPersistFailedIdempotency(err)
}

func logPaymentStepFailure(ctx context.Context, req domain.PaymentInitiatedEvent, step string, err error) {
	logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_step_failed", logger.Fields{
		"step":            step,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"idempotency_key": req.IdempotencyKey,
		"trace_id":        req.TraceID,
		"request_id":      req.RequestID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"retry_count":     req.RetryCount,
		"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
		"error":           err.Error(),
	})
}

func validateSenderAndReceiverAccounts(accounts map[string]domain.Account, event domain.PaymentInitiatedEvent, amount int64) error {
	if event.SenderID == event.ReceiverID {
		return flowpayPaymentErrors.ErrSenderReceiverIDMatching
	}

	senderAccount, senderExists := accounts[event.SenderID]
	if !senderExists {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrSenderAccountNotFound, event.SenderID)
	}

	receiverAccount, receiverExists := accounts[event.ReceiverID]
	if !receiverExists {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrReceiverAccountNotFound, event.ReceiverID)
	}

	if senderAccount.Currency != event.Currency {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrSenderCurrencyMismatch, event.SenderID)
	}

	if senderAccount.Currency != receiverAccount.Currency {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrAccountCurrencyMismatch, event.SenderID)
	}

	if senderAccount.Balance < amount {
		return fmt.Errorf("%w: sender_id=%s", flowpayPaymentErrors.ErrInsufficientBalance, event.SenderID)
	}
	return nil
}

func (r *PaymentExecutorService) ExecutePayment(ctx context.Context, event domain.PaymentInitiatedEvent) (dto.PaymentResponseDTO, error) {
	traceID := event.TraceID
	requestID := event.RequestID

	eventCtx := tracing.WithTraceAndRequestIDs(ctx, traceID, requestID)
	logger.LogEvent(eventCtx, "INFO", constants.PaymentExecutorServiceName, "process_received", logger.Fields{
		"ID":              event.ID,
		"sender_id":       event.SenderID,
		"receiver_id":     event.ReceiverID,
		"idempotency_key": event.IdempotencyKey,
		"amount":          event.Amount,
		"currency":        event.Currency,
		"retry_count":     event.RetryCount,
		"error_type":      "NONE",
	})

	// Get PaymentId from idempotency table
	idempotencyItem, err := r.idempotencyRepository.GetByKey(ctx, event.IdempotencyKey)
	if err != nil {
		logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "failed_to_get_payment_id_from_idempotency", logger.Fields{
			"error": err.Error(),
		})

		return dto.PaymentResponseDTO{}, err
	}

	paymentId := idempotencyItem.PaymentID

	// check for existing payment
	existingPayment, err := r.paymentRepository.GetByPaymentIdAndIdempotencyKey(ctx, paymentId, event.IdempotencyKey)

	if err == nil {
		logger.LogEvent(eventCtx, "INFO", constants.PaymentExecutorServiceName, "existing_payment_found", logger.Fields{
			"ID":              event.ID,
			"sender_id":       event.SenderID,
			"receiver_id":     event.ReceiverID,
			"idempotency_key": event.IdempotencyKey,
			"payment_id":      existingPayment.ID,
			"status":          types.PaymentStatusEnum(existingPayment.Status),
			"amount":          event.Amount,
			"currency":        event.Currency,
			"retry_count":     event.RetryCount,
			"error_type":      "NONE",
		})
		return dto.PaymentResponseDTO{
			PaymentID: existingPayment.ID,
			Status:    types.PaymentStatusEnum(existingPayment.Status),
		}, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		// Actual DB error
		logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "failed_to_get_payment_from_payments", logger.Fields{
			"error": err.Error(),
		})
		return dto.PaymentResponseDTO{}, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "failed_to_start_tx", logger.Fields{
			"error": err.Error(),
		})
		return dto.PaymentResponseDTO{}, err
	}

	txClosed := false
	rollbackDueToError := false

	logger.LogPlain(ctx, constants.PaymentExecutorServiceName, "started payment executor transaction id=%s sender_id=%s receiver_id=%s amount=%d", event.ID, event.SenderID, event.ReceiverID, event.Amount)

	defer func() {
		if txClosed {
			return
		}
		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(eventCtx, "WARN", constants.PaymentExecutorServiceName, "payment_executor_tx_rolled_back", logger.Fields{
				"id":              event.ID,
				"sender_id":       event.SenderID,
				"receiver_id":     event.ReceiverID,
				"idempotency_key": event.IdempotencyKey,
				"amount":          event.Amount,
				"currency":        event.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "payment_executor_tx_rollback_failed", logger.Fields{
				"id":              event.ID,
				"sender_id":       event.SenderID,
				"receiver_id":     event.ReceiverID,
				"idempotency_key": event.IdempotencyKey,
				"amount":          event.Amount,
				"currency":        event.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	rollbackTechnicalFailure := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true

		logPaymentStepFailure(eventCtx, event, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": event.IdempotencyKey,
				"sender_id":       event.SenderID,
				"receiver_id":     event.ReceiverID,
				"amount":          event.Amount,
				"currency":        event.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(eventCtx, "WARN", constants.PaymentExecutorServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": event.IdempotencyKey,
				"sender_id":       event.SenderID,
				"receiver_id":     event.ReceiverID,
				"amount":          event.Amount,
				"currency":        event.Currency,
				"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
			})
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true
		logPaymentStepFailure(eventCtx, event, step, err)
		if markErr := r.idempotencyRepository.MarkFailed(tx, eventCtx, event.IdempotencyKey, flowpayPaymentErrors.ToPaymentErrorType(err), err.Error(), event.OwnerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, err
	}

	senderID := event.SenderID
	receiverID := event.ReceiverID
	amount := event.Amount

	// Validate sender and receiver accounts
	accounts, err := r.accountRepository.GetAccounsBySenderReceiverId(eventCtx, tx, senderID, receiverID)

	if err != nil {
		return rollbackTechnicalFailure("account_lock_and_load", err)
	}

	err = validateSenderAndReceiverAccounts(accounts, event, amount)

	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_verification", err)
		}

		return rollbackTechnicalFailure("account_validation", err)
	}

	// Update balances

	err = r.accountRepository.UpdateBalanceForSenderAndReceiver(tx, eventCtx, senderID, receiverID, amount)

	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_verification", err)
		}

		return rollbackTechnicalFailure("account_validation", err)
	}

	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "updated balances sender_id=%s receiver_id=%s amount=%d", senderID, receiverID, amount)

	payment := domain.Payment{
		ID:             paymentId,
		IdempotencyKey: event.IdempotencyKey,
		SenderID:       senderID,
		ReceiverID:     receiverID,
		Amount:         amount,
		Currency:       event.Currency,
		Status:         string(types.SUCCESS),
	}

	// Create Payment Entries in payment table
	err = r.paymentRepository.CreatePayment(tx, eventCtx, payment)
	if err != nil {
		return rollbackTechnicalFailure("payment_insert", err)
	}
	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "inserted payment row payment_id=%s sender_id=%s receiver_id=%s amount=%d", payment.ID, payment.SenderID, payment.ReceiverID, payment.Amount)

	// Create Transacion entries for both sender and reciever
	senderTransaction, err := generateSenderTransaction(payment)
	if err != nil {
		return rollbackTechnicalFailure("sender_transaction_generate", err)
	}
	receiverTransaction, err := generateReceiverTransaction(payment)
	if err != nil {
		return rollbackTechnicalFailure("receiver_transaction_generate", err)
	}

	err = r.transactionRepository.CreateTransactionsForSenderAndReceiver(tx, eventCtx, senderTransaction, receiverTransaction)
	if err != nil {
		return rollbackTechnicalFailure("transaction_insert", err)
	}

	// Mark Idempotency Completed
	response := dto.PaymentResponseDTO{
		PaymentID: paymentId,
		Status:    "Payment Accepted",
	}
	responseBody, err := json.Marshal(response)
	if err != nil {
		return rollbackTechnicalFailure("idempotency_response_encode", fmt.Errorf("encode idempotency response: %w", err))
	}

	if err := r.idempotencyRepository.MarkCompleted(tx, eventCtx, event.IdempotencyKey, string(responseBody), event.ID, event.OwnerToken); err != nil {
		return rollbackTechnicalFailure("idempotency_mark_completed", err)
	}

	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "added transaction entries payment_id=%s sender_tx_id=%s receiver_tx_id=%s", payment.ID, senderTransaction.ID, receiverTransaction.ID)

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logPaymentStepFailure(eventCtx, event, "tx_commit", err)
		return dto.PaymentResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(eventCtx, "INFO", constants.PaymentExecutorServiceName, "payment_executor_tx_committed", logger.Fields{
		"idempotency_key": event.IdempotencyKey,
		"payment_id":      event.ID,
		"sender_id":       event.SenderID,
		"receiver_id":     event.ReceiverID,
		"amount":          event.Amount,
		"currency":        event.Currency,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
	})
	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "committed payment transaction outboxEvent_id=%s idempotency_key=%s", event.ID, event.IdempotencyKey)

	return response, nil
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

func newPaymentID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate payment id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
