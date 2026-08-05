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
	"flowpay/pkg/notifications"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/utils"

	"github.com/redis/go-redis/v9"
)

type PaymentRepository interface {
	CreatePayment(tx *sql.Tx, ctx context.Context, payment domain.Payment) error
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error)
	GetPaymentByID(ctx context.Context, paymentID string) (domain.Payment, error)
}

type AccountRepository interface {
	GetAccountsBySenderReceiverId(ctx context.Context, tx *sql.Tx, senderId string, receiverId string) (map[string]domain.Account, error)
	GetAccountsByIDs(ctx context.Context, ids []string) (map[string]domain.Account, error)
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
	redisClient                  *redis.Client
	paymentRepository            PaymentRepository
	transactionRepository        TransactionRepository
	paymentIdempotencyRepository PaymentIdempotencyRepository
	accountRepository            AccountRepository
	outboxEventRepository        OutboxEventRepository
	timelinePublisher            *notifications.TimelinePublisher
}

func NewPaymentService(db *sql.DB,
	redisClient *redis.Client,
	paymentRepository PaymentRepository,
	transactionRepository TransactionRepository,
	paymentIdempotencyRepository PaymentIdempotencyRepository,
	accountRepository AccountRepository,
	outboxEventRepository OutboxEventRepository,
	timelinePublisher *notifications.TimelinePublisher,
) *PaymentService {
	return &PaymentService{
		db:                           db,
		redisClient:                  redisClient,
		paymentRepository:            paymentRepository,
		transactionRepository:        transactionRepository,
		paymentIdempotencyRepository: paymentIdempotencyRepository,
		accountRepository:            accountRepository,
		outboxEventRepository:        outboxEventRepository,
		timelinePublisher:            timelinePublisher,
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

func logPaymentStepFailure(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string, step string, err error, start time.Time) {
	logger.LogEvent(ctx, "ERROR", paymentServiceConstants.ServiceName, "payment_step_failed", logger.Fields{
		"step":            step,
		"idempotency_key": idempotencyKey,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
		"error":           err.Error(),
		"outcome":         "failed",
		"duration_ms":     time.Since(start).Milliseconds(),
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

	eventType := "payment_initiated"
	if event.OfferID != "" {
		eventType = "offer_initiated"
	}

	return domain.OutboxEventType{
		ID:             eventId,
		AggregateType:  "payment",
		AggregateID:    event.ID,
		EventType:      eventType,
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

func (s *PaymentService) CreatePayment(ctx context.Context, req dto.PaymentRequestDTO, idempotencyKey string, traceId string, requestId string) (dto.PaymentResponseDTO, bool, error) {
	start := time.Now()

	// Compute Request Hash
	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return dto.PaymentResponseDTO{}, false, fmt.Errorf("failed to compute hash: %w", err)
	}
	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return dto.PaymentResponseDTO{}, false, fmt.Errorf("failed to compute hash: %w", err)
	}

	paymentIdempotencyKey := fmt.Sprintf("payment:create:idempotency:%s", idempotencyKey)

	cached, err := s.redisClient.Get(ctx, paymentIdempotencyKey).Result()
	if err == nil {
		var resp dto.PaymentResponseDTO
		if json.Unmarshal([]byte(cached), &resp) == nil {
			return resp, true, nil
		}
	}

	ownerToken, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, false, err
	}

	paymentID, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, false, err
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
		logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_claim_or_get", err, start)
		return dto.PaymentResponseDTO{}, false, err
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
				"outcome":         "idempotency_mismatch",
				"duration_ms":     time.Since(start).Milliseconds(),
			})
			return dto.PaymentResponseDTO{}, false, fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayPaymentErrors.ErrIdempotencyInProgress, idempotencyKey)
			logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_in_progress", err, start)
			return dto.PaymentResponseDTO{}, false, err
		}

		cachedResponse, err := cachedIdempotencyResult(existingIdempotency)
		if err != nil {
			logPaymentStepFailure(ctx, req, idempotencyKey, "idempotency_cached_result", err, start)
			return dto.PaymentResponseDTO{}, false, err
		}

		cachedResponseJSON, err := json.Marshal(cachedResponse)

		_ = s.redisClient.Set(ctx,
			paymentIdempotencyKey,
			cachedResponseJSON,
			5*time.Minute,
		).Err()

		logger.LogEvent(ctx, "INFO", paymentServiceConstants.ServiceName, "idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"trace_id":        traceId,
			"status":          existingIdempotency.Status,
			"payment_id":      cachedResponse.PaymentID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			"outcome":         "success",
			"duration_ms":     time.Since(start).Milliseconds(),
		})
		logger.LogPlain(ctx, paymentServiceConstants.ServiceName, "served cached idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, false, nil
	}

	if existingIdempotency.PaymentID != "" {
		paymentID = existingIdempotency.PaymentID
	}

	processing := dto.PaymentResponseDTO{
		PaymentID: paymentID,
		Status:    types.PROCESSING,
	}

	payload, _ := json.Marshal(processing)

	_ = s.redisClient.Set(
		ctx,
		paymentIdempotencyKey,
		payload,
		30*time.Second,
	).Err()

	// Begin Transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dto.PaymentResponseDTO{}, false, err
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
				"outcome":         "rolled_back",
				"duration_ms":     time.Since(start).Milliseconds(),
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
				"outcome":         "rollback_failed",
				"duration_ms":     time.Since(start).Milliseconds(),
			})
		}
	}()

	// Create Functions for rollback and markasfailed
	rollbackTechnicalFailure := func(step string, err error) (dto.PaymentResponseDTO, bool, error) {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, step, err, start)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", paymentServiceConstants.ServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"offer_id":        req.OfferId,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
				"outcome":         "rollback_failed",
				"duration_ms":     time.Since(start).Milliseconds(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", paymentServiceConstants.ServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceId,
				"sender_id":       req.SenderID,
				"receiver_id":     req.ReceiverID,
				"amount":          req.Amount,
				"currency":        req.Currency,
				"offer_id":        req.OfferId,
				"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
				"outcome":         "rolled_back",
				"duration_ms":     time.Since(start).Milliseconds(),
			})
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, false, err
	}

	markFailedAndCommit := func(step string, err error) (dto.PaymentResponseDTO, bool, error) {
		rollbackDueToError = true
		logPaymentStepFailure(ctx, req, idempotencyKey, step, err, start)
		if markErr := s.paymentIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayPaymentErrors.ToPaymentErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		failedPayment := dto.PaymentResponseDTO{
			PaymentID: paymentID,
			Status:    types.FAILED,
		}
		failedPaymentPayload, _ := json.Marshal(failedPayment)

		_ = s.redisClient.Set(
			ctx,
			paymentIdempotencyKey,
			failedPaymentPayload,
			24*time.Hour,
		).Err()
		txClosed = true
		return dto.PaymentResponseDTO{}, false, err
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
		OfferID:        req.OfferId,
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
		"offer_id":             req.OfferId,
		"retry_count":          paymentInitiatedEvent.RetryCount,
		"error_type":           flowpayPaymentErrors.ErrorTypeNone,
		"outcome":              "success",
		"duration_ms":          time.Since(start).Milliseconds(),
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
		logPaymentStepFailure(ctx, req, idempotencyKey, "tx_commit", err, start)
		return dto.PaymentResponseDTO{}, false, err
	}
	txCommitPayload, _ := json.Marshal(response)

	_ = s.redisClient.Set(
		ctx,
		paymentIdempotencyKey,
		txCommitPayload,
		24*time.Hour,
	).Err()
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
		"offer_id":        req.OfferId,
		"retry_count":     paymentInitiatedEvent.RetryCount,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
		"outcome":         "success",
		"duration_ms":     time.Since(start).Milliseconds(),
	})
	logger.LogPlain(ctx, paymentServiceConstants.ServiceName, "committed payment transaction payment_id=%s outboxEvent_id=%s idempotency_key=%s", paymentID, outboxEvent.ID, idempotencyKey)

	go func() {
		publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		s.timelinePublisher.Publish(publishCtx, paymentServiceConstants.ServiceName, paymentID, notifications.StepPaymentInitiated, notifications.StatusSuccess, traceId, requestId)
	}()

	return response, false, nil
}

func offerSummaryFromPayment(payment domain.Payment) *dto.OfferSummaryDTO {
	if payment.OfferID == "" {
		return nil
	}

	summary := &dto.OfferSummaryDTO{
		OfferID:   payment.OfferID,
		OfferCode: payment.OfferCode,
		OfferType: payment.OfferType,
	}

	switch payment.OfferType {
	case "CASHBACK":
		summary.CashbackAmount = payment.OfferBenefitAmount
	default:
		summary.DiscountAmount = payment.OfferBenefitAmount
	}

	return summary
}

func (s *PaymentService) GetPaymentByID(ctx context.Context, paymentID string) (dto.GetPaymentResponseDTO, error) {
	payment, err := s.paymentRepository.GetPaymentByID(ctx, paymentID)
	if err == nil {
		response := dto.GetPaymentResponseDTO{
			PaymentID:      payment.ID,
			IdempotencyKey: payment.IdempotencyKey,
			SenderID:       payment.SenderID,
			ReceiverID:     payment.ReceiverID,
			Amount:         payment.Amount,
			Currency:       payment.Currency,
			PaymentMethod:  payment.PaymentMethod,
			Status:         payment.Status,
			CreatedAt:      payment.CreatedAt,
			UpdatedAt:      payment.UpdatedAt,
			CompletedAt:    payment.CompletedAt,
			Offer:          offerSummaryFromPayment(payment),
		}
		if err := s.attachPartyDetails(ctx, &response); err != nil {
			return dto.GetPaymentResponseDTO{}, err
		}
		return response, nil
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
		PaymentMethod:  "WALLET",
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
	if paymentInitiatedEvent.OfferID != "" {
		response.Offer = &dto.OfferSummaryDTO{OfferID: paymentInitiatedEvent.OfferID}
	}

	if err := s.attachPartyDetails(ctx, &response); err != nil {
		return dto.GetPaymentResponseDTO{}, err
	}

	return response, nil
}

// attachPartyDetails populates Sender/Receiver on the response with account name and
// payment handle, looked up in a single query. SenderID/ReceiverID are left untouched.
func (s *PaymentService) attachPartyDetails(ctx context.Context, response *dto.GetPaymentResponseDTO) error {
	if response.SenderID == "" || response.ReceiverID == "" {
		return nil
	}

	accounts, err := s.accountRepository.GetAccountsByIDs(ctx, []string{response.SenderID, response.ReceiverID})
	if err != nil {
		return err
	}

	if sender, ok := accounts[response.SenderID]; ok {
		response.Sender = &dto.PaymentPartyDTO{
			ID:            sender.ID,
			Name:          sender.AccountName,
			PaymentHandle: sender.PaymentHandle,
		}
	}
	if receiver, ok := accounts[response.ReceiverID]; ok {
		response.Receiver = &dto.PaymentPartyDTO{
			ID:            receiver.ID,
			Name:          receiver.AccountName,
			PaymentHandle: receiver.PaymentHandle,
		}
	}

	return nil
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
