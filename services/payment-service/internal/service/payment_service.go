package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	paymentServiceConstants "flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/types"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/utils"
)

type PaymentRepository interface {
	CreatePayment(tx *sql.Tx, ctx context.Context, payment domain.Payment) error
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error)
	GetPaymentByID(ctx context.Context, paymentID string) (domain.Payment, error)
}

type AccountRepository interface {
	GetAccountsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderId string, receiverId string) (map[string]domain.Account, error)
}

type TransactionRepository interface {
	CreateTransactionsForSenderAndReceiver(tx *sql.Tx, ctx context.Context, senderTransaction domain.Transaction, receiverTransaction domain.Transaction) error
}

type PaymentIdempotencyRepository interface {
	ClaimOrGet(ctx context.Context, idempotency domain.PaymentIdempotencyKey) (domain.PaymentIdempotencyKey, bool, error)
	MarkCompleted(tx *sql.Tx, ctx context.Context, idempotencyKey string, responseBody string, paymentID string, ownerToken string) error
	MarkFailed(tx *sql.Tx, ctx context.Context, idempotencyKey string, errorCode string, errorMessage string, ownerToken string) error
	GetByKey(ctx context.Context, key string) (domain.PaymentIdempotencyKey, error)
	GetByPaymentID(ctx context.Context, paymentID string) (domain.PaymentIdempotencyKey, error)
}

type OutboxEventRepository interface {
	InsertOutboxEvent(tx *sql.Tx, ctx context.Context, payload domain.OutboxEventType) error
	GetLatestByAggregateID(ctx context.Context, aggregateID string) (domain.OutboxEventType, error)
}

type PaymentService struct {
	db                           *sql.DB
	paymentRepository            PaymentRepository
	transactionRepository        TransactionRepository
	paymentIdempotencyRepository PaymentIdempotencyRepository
	accountRepository            AccountRepository
	outboxEventRepository        OutboxEventRepository
}

func NewPaymentService(db *sql.DB,
	paymentRepository PaymentRepository,
	transactionRepository TransactionRepository,
	paymentIdempotencyRepository PaymentIdempotencyRepository,
	accountRepository AccountRepository,
	outboxEventRepository OutboxEventRepository,
) *PaymentService {
	return &PaymentService{
		db:                           db,
		paymentRepository:            paymentRepository,
		transactionRepository:        transactionRepository,
		paymentIdempotencyRepository: paymentIdempotencyRepository,
		accountRepository:            accountRepository,
		outboxEventRepository:        outboxEventRepository,
	}
}

func validateSenderAndReceiverAccounts(accounts map[string]domain.Account, req dto.PaymentRequestDTO, amount int64) error {
	if req.SenderID == req.ReceiverID {
		return flowpayPaymentErrors.ErrSenderReceiverIDMatching
	}

	senderAccount, senderExists := accounts[req.SenderID]
	if !senderExists {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrSenderAccountNotFound, req.SenderID)
	}

	receiverAccount, receiverExists := accounts[req.ReceiverID]
	if !receiverExists {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrReceiverAccountNotFound, req.ReceiverID)
	}

	if senderAccount.Currency != req.Currency {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrSenderCurrencyMismatch, req.SenderID)
	}

	if senderAccount.Currency != receiverAccount.Currency {
		return fmt.Errorf("%w: %s", flowpayPaymentErrors.ErrAccountCurrencyMismatch, req.SenderID)
	}

	if senderAccount.Balance < amount {
		return fmt.Errorf("%w: sender_id=%s", flowpayPaymentErrors.ErrInsufficientBalance, req.SenderID)
	}

	return nil
}

func logPaymentStepFailure(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string, step string, err error) {
	logger.LogEvent(ctx, "ERROR", paymentServiceConstants.ServiceName, "payment_step_failed", logger.Fields{
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

func leaseExpiryFromNow() time.Time {
	return time.Now().UTC().Add(5 * time.Minute)
}

func MapPaymentInitiatedToOutbox(event domain.PaymentInitiatedEvent, retryCount int8, traceID string, requestID string) (domain.OutboxEventType, error) {
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return domain.OutboxEventType{}, err
	}

	eventId, err := newPaymentID()
	if err != nil {
		return domain.OutboxEventType{}, err
	}

	return domain.OutboxEventType{
		ID:             eventId,
		AggregateType:  "payment",
		AggregateID:    event.ID,
		EventType:      "payment_initiated",
		EventVersion:   1,
		Status:         domain.OutboxEventPending,
		Payload:        string(payloadBytes),
		CreatedAt:      time.Now(),
		TraceID:        traceID,
		RequestID:      requestID,
		RetryCount:     retryCount,
		IdempotencyKey: event.IdempotencyKey,
	}, nil
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

func (s *PaymentService) CreatePayment(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string, traceId string, requestId string) (dto.PaymentResponseDTO, error) {
	// Compute Request Hash
	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}
	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return dto.PaymentResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	ownerToken, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	paymentID, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	idempotencyPayload := domain.PaymentIdempotencyKey{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		PaymentID:      paymentID,
		Status:         "IN_PROGRESS",
		OwnerToken:     ownerToken,
		LockedUntil:    leaseExpiryFromNow(),
	}

	// Claim or get idempotency key
	existingIdempotency, idempotencyClaimed, err := s.paymentIdempotencyRepository.ClaimOrGet(ctx, idempotencyPayload)
	if err != nil {
		logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_claim_or_get", err)
		return dto.PaymentResponseDTO{}, err
	}

	if !idempotencyClaimed {
		if existingIdempotency.RequestHash != payloadHash {
			logger.LogEvent(ctx, "WARN", paymentServiceConstants.ServiceName, "payment_idempotency_mismatch", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
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

		logger.LogEvent(ctx, "INFO", paymentServiceConstants.ServiceName, "idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"trace_id":        traceId,
			"status":          existingIdempotency.Status,
			"payment_id":      cachedResponse.PaymentID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayPaymentErrors.ErrorTypeNone,
		})
		logger.LogPlain(ctx, paymentServiceConstants.ServiceName, "served cached idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, nil
	}

	if existingIdempotency.PaymentID != "" {
		paymentID = existingIdempotency.PaymentID
	}

	// Begin Transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}
	txClosed := false
	rollbackDueToError := false
	logger.LogPlain(ctx, paymentServiceConstants.ServiceName, "started payment transaction idempotency_key=%s sender_id=%s receiver_id=%s", idempotencyKey, req.SenderID, req.ReceiverID)

	defer func() {
		if txClosed {
			return
		}

		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(ctx, "WARN", paymentServiceConstants.ServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(ctx, "ERROR", paymentServiceConstants.ServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	// Create Functions for rollback and markasfailed
	rollbackTechnicalFailure := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", paymentServiceConstants.ServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", paymentServiceConstants.ServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
			})
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, step, err)
		if markErr := s.paymentIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayPaymentErrors.ToPaymentErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, err
	}

	// Validate sender and receiver accounts
	accounts, err := s.accountRepository.GetAccountsBySenderReceiverId(ctx, tx, req.SenderID, req.ReceiverID)
	if err != nil {
		return rollbackTechnicalFailure("account_lock_and_load", err)
	}
	amount := req.Amount

	err = validateSenderAndReceiverAccounts(accounts, req, amount)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_validation", err)
		}
		return rollbackTechnicalFailure("account_validation", err)
	}

	// Create Outbox event
	paymentInitiatedEvent := domain.PaymentInitiatedEvent{
		ID:             paymentID,
		SenderID:       req.SenderID,
		ReceiverID:     req.ReceiverID,
		IdempotencyKey: idempotencyKey,
		OwnerToken:     ownerToken,
		Amount:         amount,
		TraceID:        traceId,
		RequestID:      requestId,
		RetryCount:     0,
		Currency:       req.Currency,
		CreatedAt:      time.Now(),
	}

	outboxEvent, err := MapPaymentInitiatedToOutbox(paymentInitiatedEvent, 0, traceId, requestId)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("outbox_event_creation", err)
		}
		return rollbackTechnicalFailure("outbox_event_creation", err)
	}

	logger.LogEvent(ctx, "INFO", paymentServiceConstants.ServiceName, "outbox_event_inserted", logger.Fields{
		"idempotency_key":      idempotencyKey,
		"trace_id":             traceId,
		"outbox_event_id":      outboxEvent.ID,
		"outbox_event_version": outboxEvent.EventVersion,
		"sender_id":            req.SenderID,
		"receiver_id":          req.ReceiverID,
		"amount":               req.Amount,
		"currency":             req.Currency,
		"retry_count":          paymentInitiatedEvent.RetryCount,
		"error_type":           flowpayPaymentErrors.ErrorTypeNone,
	})

	err = s.outboxEventRepository.InsertOutboxEvent(tx, ctx, outboxEvent)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("outbox_event_insertion", err)
		}
		return rollbackTechnicalFailure("outbox_event_insertion", err)
	}

	response := dto.PaymentResponseDTO{
		PaymentID: paymentID,
		Status:    types.PROCESSING,
	}

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, "tx_commit", err)
		return dto.PaymentResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(ctx, "INFO", paymentServiceConstants.ServiceName, "payment_tx_committed", logger.Fields{
		"idempotency_key": idempotencyKey,
		"trace_id":        traceId,
		"payment_id":      paymentID,
		"outbox_event_id": outboxEvent.ID,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"retry_count":     paymentInitiatedEvent.RetryCount,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
	})
	logger.LogPlain(ctx, paymentServiceConstants.ServiceName, "committed payment transaction payment_id=%s outboxEvent_id=%s idempotency_key=%s", paymentID, outboxEvent.ID, idempotencyKey)

	return response, nil
}

func (s *PaymentService) GetPaymentByID(ctx context.Context, paymentID string) (dto.GetPaymentResponseDTO, error) {
	payment, err := s.paymentRepository.GetPaymentByID(ctx, paymentID)
	if err == nil {
		return dto.GetPaymentResponseDTO{
			PaymentID:      payment.ID,
			IdempotencyKey: payment.IdempotencyKey,
			SenderID:       payment.SenderID,
			ReceiverID:     payment.ReceiverID,
			Amount:         payment.Amount,
			Currency:       payment.Currency,
			Status:         payment.Status,
			CreatedAt:      payment.CreatedAt,
			UpdatedAt:      payment.UpdatedAt,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return dto.GetPaymentResponseDTO{}, err
	}

	idempotency, err := s.paymentIdempotencyRepository.GetByPaymentID(ctx, paymentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return dto.GetPaymentResponseDTO{}, flowpayPaymentErrors.ErrPaymentNotFound
		}
		return dto.GetPaymentResponseDTO{}, err
	}

	response := dto.GetPaymentResponseDTO{
		PaymentID:      idempotency.PaymentID,
		IdempotencyKey: idempotency.IdempotencyKey,
		Status:         paymentStatusFromIdempotency(idempotency.Status),
		CreatedAt:      idempotency.CreatedAt,
		UpdatedAt:      idempotency.UpdatedAt,
	}

	outboxEvent, err := s.outboxEventRepository.GetLatestByAggregateID(ctx, paymentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return response, nil
		}
		return dto.GetPaymentResponseDTO{}, err
	}

	var paymentInitiatedEvent domain.PaymentInitiatedEvent
	if err := json.Unmarshal([]byte(outboxEvent.Payload), &paymentInitiatedEvent); err != nil {
		return dto.GetPaymentResponseDTO{}, fmt.Errorf("decode outbox payment payload: %w", err)
	}

	response.SenderID = paymentInitiatedEvent.SenderID
	response.ReceiverID = paymentInitiatedEvent.ReceiverID
	response.Amount = paymentInitiatedEvent.Amount
	response.Currency = paymentInitiatedEvent.Currency

	return response, nil
}

func paymentStatusFromIdempotency(status string) string {
	switch status {
	case "IN_PROGRESS":
		return string(types.PROCESSING)
	case "FAILED":
		return string(types.FAILED)
	case "COMPLETED":
		return string(types.SUCCESS)
	default:
		return status
	}
}
