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
	"flowpay/pkg/notifications"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"
	"fmt"
	"time"
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
	CreateTransactions(tx *sql.Tx, ctx context.Context, transactions []domain.Transaction) error
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
	InsertOutboxEvent(tx *sql.Tx, ctx context.Context, payload domain.OutboxEventType) error
}

type PaymentExecutorService struct {
	db                     *sql.DB
	accountRepository      AccountRepository
	paymentRepository      PaymentRepository
	transactionRepository  TransactionRepository
	outboxEventsRepository OutboxEventsRepository
	idempotencyRepository  IdempotencyRepository
	timelinePublisher      *notifications.TimelinePublisher
}

func NewPaymentExecutorService(db *sql.DB, accountRepository AccountRepository, paymentRepository PaymentRepository, transactionRepository TransactionRepository, idempotencyRepository IdempotencyRepository, outboxEventsRepository OutboxEventsRepository, timelinePublisher *notifications.TimelinePublisher) *PaymentExecutorService {
	return &PaymentExecutorService{
		db:                     db,
		paymentRepository:      paymentRepository,
		transactionRepository:  transactionRepository,
		accountRepository:      accountRepository,
		idempotencyRepository:  idempotencyRepository,
		outboxEventsRepository: outboxEventsRepository,
		timelinePublisher:      timelinePublisher,
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

func logOfferRejectedForPaymentStepFailure(ctx context.Context, req domain.OfferOutboxEventType, step string, err error, offer domain.OfferRejectedEvent) {
	logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_rejection_step_failed", logger.Fields{
		"step":            step,
		"sender_id":       offer.SenderID,
		"receiver_id":     offer.ReceiverID,
		"idempotency_key": req.IdempotencyKey,
		"trace_id":        req.TraceID,
		"request_id":      req.RequestID,
		"amount":          offer.Amount,
		"currency":        offer.Currency,
		"retry_count":     req.RetryCount,
		"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
		"error":           err.Error(),
	})
}

func MapPaymentSuccessToOutbox(event domain.PaymentSuccessEvent, retryCount int8, traceID string, requestID string) (domain.OutboxEventType, error) {
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return domain.OutboxEventType{}, err
	}

	eventId, err := newPaymentID()
	if err != nil {
		return domain.OutboxEventType{}, err
	}

	eventType := "payment_success"
	if event.OfferID != nil && *event.OfferID != "" {
		eventType = "offer_applicable_payment_success"
	}

	return domain.OutboxEventType{
		ID:             eventId,
		AggregateType:  "payment",
		AggregateID:    event.PaymentID,
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

func validateSenderAndReceiverAccounts(accounts map[string]domain.Account, event domain.PaymentInitiatedEvent, amount int64, discountAmount int64) error {

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

	if !senderAccount.AllowNegativeBalance && senderAccount.Balance < (amount-discountAmount) {
		return fmt.Errorf("%w: sender_id=%s", flowpayPaymentErrors.ErrInsufficientBalance, event.SenderID)
	}
	return nil
}

func getOfferToBeRedeemed(
	amount int64,
	offer *domain.OfferReservedEvent,
) int64 {

	if offer == nil {
		return 0
	}

	if amount < int64(offer.MinimumPaymentAmount) {
		return 0
	}

	// Fixed amount offer
	if offer.OfferAmount != nil && *offer.OfferAmount > 0 {
		return min(amount, int64(*offer.OfferAmount))
	}

	// Percentage offer
	if offer.OfferPercentage != nil && *offer.OfferPercentage > 0 {
		benefit := amount * int64(*offer.OfferPercentage) / 100

		if offer.MaxBenefitAmount > 0 {
			benefit = min(
				benefit,
				int64(offer.MaxBenefitAmount),
			)
		}

		return benefit
	}

	return 0
}

func (r *PaymentExecutorService) ExecutePayment(ctx context.Context, event domain.PaymentInitiatedEvent, offer *domain.OfferReservedEvent) (dto.PaymentResponseDTO, error) {
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
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentCompleted, notifications.StatusFailed, traceID, requestID)
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
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentCompleted, notifications.StatusFailed, traceID, requestID)
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

	discountAmount := int64(0)
	cashbackAmount := int64(0)

	if offer != nil {
		switch offer.OfferType {
		case "DISCOUNT":
			{
				discountAmount += getOfferToBeRedeemed(amount, offer)
				break
			}
		case "CASHBACK":
			{
				cashbackAmount += getOfferToBeRedeemed(amount, offer)
				break
			}
		default:
			{
				break
			}
		}
	}

	err = validateSenderAndReceiverAccounts(accounts, event, amount, discountAmount)

	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_verification", err)
		}

		return rollbackTechnicalFailure("account_validation", err)
	}

	r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentValidated, notifications.StatusSuccess, traceID, requestID)

	// Update balances

	err = r.accountRepository.UpdateBalanceForSenderAndReceiver(tx, eventCtx, senderID, receiverID, amount-discountAmount)

	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_verification", err)
		}

		return rollbackTechnicalFailure("account_validation", err)
	}

	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "updated balances sender_id=%s receiver_id=%s amount=%d", senderID, receiverID, amount-discountAmount)

	r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepAccountsUpdated, notifications.StatusSuccess, traceID, requestID)

	offerDiscount := discountAmount
	if cashbackAmount > 0 {
		offerDiscount = cashbackAmount
	}

	offerType := ""
	if offer != nil {
		offerType = offer.OfferType
	}

	payment := domain.Payment{
		ID:             paymentId,
		IdempotencyKey: event.IdempotencyKey,
		SenderID:       senderID,
		ReceiverID:     receiverID,
		Amount:         amount,
		NetAmount:      amount - discountAmount,
		OfferID:        event.OfferID,
		OfferType:      offerType,
		OfferAmount:    offerDiscount,
		Currency:       event.Currency,
		Status:         string(types.SUCCESS),
	}

	// Create Payment Entries in payment table
	err = r.paymentRepository.CreatePayment(tx, eventCtx, payment)
	if err != nil {
		return rollbackTechnicalFailure("payment_insert", err)
	}
	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "inserted payment row payment_id=%s sender_id=%s receiver_id=%s amount=%d", payment.ID, payment.SenderID, payment.ReceiverID, payment.Amount)

	r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentPersisted, notifications.StatusSuccess, traceID, requestID)

	// Create Transacion entries for both sender and reciever
	transactions := []domain.Transaction{}
	senderTransaction, err := generateSenderTransaction(payment, "PAYMENT")
	if err != nil {
		return rollbackTechnicalFailure("sender_transaction_generate", err)
	}
	transactions = append(transactions, senderTransaction)

	receiverTransaction, err := generateReceiverTransaction(payment, "PAYMENT")
	if err != nil {
		return rollbackTechnicalFailure("receiver_transaction_generate", err)
	}
	transactions = append(transactions, receiverTransaction)

	isCashbackPayment := payment.OfferType == "CASHBACK" && payment.OfferAmount > 0

	if isCashbackPayment {
		poolDebitTx, _ := generatePromotionPoolDebitTransaction(
			payment,
			offer.PromotionPoolAccountId,
		)

		cashbackCreditTx, _ := generateCashbackCreditTransaction(
			payment,
		)

		transactions = append(
			transactions,
			poolDebitTx,
			cashbackCreditTx,
		)
	}

	err = r.transactionRepository.CreateTransactions(tx, eventCtx, transactions)
	if err != nil {
		return rollbackTechnicalFailure("transaction_insert", err)
	}

	r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepTransactionsCompleted, notifications.StatusSuccess, traceID, requestID)

	if isCashbackPayment {
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPromoPoolDebited, notifications.StatusSuccess, traceID, requestID)
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepCashbackCredited, notifications.StatusSuccess, traceID, requestID)
	}

	// Create Outbox event
	offerIDPtr := (*string)(nil)
	offerTypePtr := (*string)(nil)
	var offerAmountPtr *int64
	reservationID := ""
	successIdempotencyKey := event.IdempotencyKey

	if offer != nil {
		offerIDPtr = &offer.OfferID
		offerTypePtr = &offer.OfferType
		offerAmountPtr = offer.OfferAmount
		reservationID = offer.ReservationID
		successIdempotencyKey = offer.IdempotencyKey
	}

	paymentSuccessEvent := domain.PaymentSuccessEvent{
		PaymentID:  payment.ID,
		SenderID:   payment.SenderID,
		ReceiverID: payment.ReceiverID,
		Amount:     payment.Amount,
		NetAmount:  payment.NetAmount,
		RetryCount: 0,

		OfferID:        offerIDPtr,
		OfferType:      offerTypePtr,
		OfferAmount:    offerAmountPtr,
		IdempotencyKey: successIdempotencyKey,
		ReservationID:  reservationID,

		Currency:  payment.Currency,
		TraceID:   traceID,
		RequestID: requestID,
		CreatedAt: time.Now(),
	}

	outboxEvent, err := MapPaymentSuccessToOutbox(paymentSuccessEvent, 0, paymentSuccessEvent.TraceID, paymentSuccessEvent.RequestID)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("outbox_event_creation", err)
		}
		return rollbackTechnicalFailure("outbox_event_creation", err)
	}

	err = r.outboxEventsRepository.InsertOutboxEvent(tx, ctx, outboxEvent)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("outbox_event_insertion", err)
		}
		return rollbackTechnicalFailure("outbox_event_insertion", err)
	}

	logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "outbox_event_inserted", logger.Fields{
		"trace_id":             outboxEvent.TraceID,
		"outbox_event_id":      outboxEvent.ID,
		"outbox_event_version": outboxEvent.EventVersion,
		"sender_id":            paymentSuccessEvent.SenderID,
		"receiver_id":          paymentSuccessEvent.ReceiverID,
		"amount":               paymentSuccessEvent.Amount,
		"currency":             paymentSuccessEvent.Currency,
		"offer_id":             event.OfferID,
		"reservation_id":       paymentSuccessEvent.ReservationID,
		"retry_count":          paymentSuccessEvent.RetryCount,
		"error_type":           flowpayPaymentErrors.ErrorTypeNone,
	})

	r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepOutboxEventCreated, notifications.StatusSuccess, traceID, requestID)

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
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentCompleted, notifications.StatusFailed, traceID, requestID)
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

	// Offer-based payments defer PAYMENT_COMPLETED until offer-service resolves
	// the redemption (see OfferService.RedeemOffer); only the no-offer path
	// completes here.
	if offer == nil {
		r.timelinePublisher.Publish(eventCtx, constants.PaymentExecutorServiceName, paymentId, notifications.StepPaymentCompleted, notifications.StatusSuccess, traceID, requestID)
	}

	return response, nil
}

func (r *PaymentExecutorService) HandlePaymentInitiated(ctx context.Context, event domain.PaymentInitiatedEvent) (dto.PaymentResponseDTO, error) {
	response, err := r.ExecutePayment(ctx, event, nil)
	return response, err
}

func (r *PaymentExecutorService) ExecuteOfferReservedPayment(
	ctx context.Context,
	event domain.OfferOutboxEventType,
) (dto.PaymentResponseDTO, error) {

	var offer domain.OfferReservedEvent

	if err := json.Unmarshal([]byte(event.Payload), &offer); err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	paymentEvent := domain.PaymentInitiatedEvent{
		ID:             offer.PaymentID,
		SenderID:       offer.SenderID,
		ReceiverID:     offer.ReceiverID,
		IdempotencyKey: offer.PaymentIdempotencyKey,
		TraceID:        offer.TraceID,
		RequestID:      offer.RequestID,
		Amount:         offer.Amount,
		Currency:       offer.Currency,
		OfferID:        offer.OfferID,
		CreatedAt:      time.Now(),
	}

	return r.ExecutePayment(ctx, paymentEvent, &offer)
}

func (r *PaymentExecutorService) ExecuteOfferRejectedPayment(ctx context.Context, event domain.OfferOutboxEventType) (dto.PaymentResponseDTO, error) {
	traceID := event.TraceID
	requestID := event.RequestID
	var offer domain.OfferRejectedEvent

	if err := json.Unmarshal([]byte(event.Payload), &offer); err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	eventCtx := tracing.WithTraceAndRequestIDs(ctx, traceID, requestID)
	logger.LogEvent(eventCtx, "INFO", constants.PaymentExecutorServiceName, "offer_rejection_process_received", logger.Fields{
		"ID":              event.ID,
		"sender_id":       offer.SenderID,
		"receiver_id":     offer.ReceiverID,
		"idempotency_key": event.IdempotencyKey,
		"amount":          offer.Amount,
		"currency":        offer.Currency,
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

	// Already completed -> duplicate event
	if idempotencyItem.Status == "COMPLETED" {
		logger.LogEvent(eventCtx, "INFO",
			constants.PaymentExecutorServiceName,
			"offer_rejection_duplicate_completed_payment",
			logger.Fields{
				"payment_id":      idempotencyItem.PaymentID,
				"idempotency_key": offer.PaymentIdempotencyKey,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			},
		)

		return dto.PaymentResponseDTO{
			PaymentID: idempotencyItem.PaymentID,
			Status:    "Payment Accepted",
		}, nil
	}

	// Already failed -> duplicate event
	if idempotencyItem.Status == "FAILED" {
		logger.LogEvent(eventCtx, "INFO",
			constants.PaymentExecutorServiceName,
			"offer_rejection_duplicate_failed_payment",
			logger.Fields{
				"payment_id":      idempotencyItem.PaymentID,
				"idempotency_key": offer.PaymentIdempotencyKey,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			},
		)

		return dto.PaymentResponseDTO{
			PaymentID: idempotencyItem.PaymentID,
			Status:    "Payment Failed",
		}, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "failed_to_start_tx_for_offer_rejected", logger.Fields{
			"error": err.Error(),
		})
		return dto.PaymentResponseDTO{}, err
	}

	txClosed := false
	rollbackDueToError := false

	logger.LogPlain(ctx, constants.PaymentExecutorServiceName, "started payment executor offer rejection transaction id=%s sender_id=%s receiver_id=%s amount=%d", event.ID, offer.SenderID, offer.ReceiverID, offer.Amount)

	defer func() {
		if txClosed {
			return
		}
		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(eventCtx, "WARN", constants.PaymentExecutorServiceName, "payment_executor_tx_rolled_back", logger.Fields{
				"id":              event.ID,
				"sender_id":       offer.SenderID,
				"receiver_id":     offer.ReceiverID,
				"idempotency_key": event.IdempotencyKey,
				"amount":          offer.Amount,
				"currency":        offer.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "payment_executor_tx_rollback_failed", logger.Fields{
				"id":              event.ID,
				"sender_id":       offer.SenderID,
				"receiver_id":     offer.ReceiverID,
				"idempotency_key": event.IdempotencyKey,
				"amount":          offer.Amount,
				"currency":        offer.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	rollbackTechnicalFailure := func(step string, err error) (dto.PaymentResponseDTO, error) {
		rollbackDueToError = true

		logOfferRejectedForPaymentStepFailure(eventCtx, event, step, err, offer)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(eventCtx, "ERROR", constants.PaymentExecutorServiceName, "payment_tx_rollback_failed", logger.Fields{
				"idempotency_key": event.IdempotencyKey,
				"sender_id":       offer.SenderID,
				"receiver_id":     offer.ReceiverID,
				"amount":          offer.Amount,
				"currency":        offer.Currency,
				"error_type":      flowpayPaymentErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(eventCtx, "WARN", constants.PaymentExecutorServiceName, "payment_tx_rolled_back", logger.Fields{
				"idempotency_key": event.IdempotencyKey,
				"sender_id":       offer.SenderID,
				"receiver_id":     offer.ReceiverID,
				"amount":          offer.Amount,
				"currency":        offer.Currency,
				"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
			})
		}
		txClosed = true
		return dto.PaymentResponseDTO{}, err
	}

	// Mark Idempotency Failed
	response := dto.PaymentResponseDTO{
		PaymentID: paymentId,
		Status:    "Payment Failed_Due_To_Rejected_Offer",
	}

	logger.LogEvent(
		eventCtx,
		"INFO",
		constants.PaymentExecutorServiceName,
		"marking_payment_failed_due_to_offer_rejection",
		logger.Fields{
			"payment_id":      idempotencyItem.PaymentID,
			"idempotency_key": event.IdempotencyKey,
			"offer_id":        offer.OfferID,
			"error_type":      flowpayPaymentErrors.ErrorTypeOfferRejected,
		},
	)

	if err := r.idempotencyRepository.MarkFailed(tx, eventCtx, event.IdempotencyKey, flowpayPaymentErrors.ErrorTypeOfferRejected, "offer rejected", offer.PaymentOwnerToken); err != nil {
		return rollbackTechnicalFailure("idempotency_mark_failed", err)
	}

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logOfferRejectedForPaymentStepFailure(eventCtx, event, "tx_commit", err, offer)
		return dto.PaymentResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(eventCtx, "INFO", constants.PaymentExecutorServiceName, "payment_executor_tx_committed", logger.Fields{
		"idempotency_key": event.IdempotencyKey,
		"payment_id":      idempotencyItem.PaymentID,
		"sender_id":       offer.SenderID,
		"receiver_id":     offer.ReceiverID,
		"amount":          offer.Amount,
		"currency":        offer.Currency,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
	})
	logger.LogPlain(eventCtx, constants.PaymentExecutorServiceName, "committed payment transaction outboxEvent_id=%s idempotency_key=%s", event.ID, event.IdempotencyKey)

	return response, nil
}

func generateSenderTransaction(payment domain.Payment, transactionCategory string) (domain.Transaction, error) {
	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}
	return domain.Transaction{
		ID:                  transactionID,
		PaymentID:           payment.ID,
		AccountID:           payment.SenderID,
		Type:                "DEBIT",
		TransactionCategory: transactionCategory,
		Amount:              payment.Amount,
		Currency:            payment.Currency,
		Status:              "SUCCESS",
	}, nil
}

func generateReceiverTransaction(payment domain.Payment, transactionCategory string) (domain.Transaction, error) {
	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}
	return domain.Transaction{
		ID:                  transactionID,
		PaymentID:           payment.ID,
		AccountID:           payment.ReceiverID,
		Type:                "CREDIT",
		TransactionCategory: transactionCategory,
		Amount:              payment.Amount,
		Currency:            payment.Currency,
		Status:              "SUCCESS",
	}, nil
}

func generatePromotionPoolDebitTransaction(
	payment domain.Payment,
	promotionPoolAccountID string,
) (domain.Transaction, error) {

	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}

	return domain.Transaction{
		ID:                  transactionID,
		PaymentID:           payment.ID,
		AccountID:           promotionPoolAccountID,
		Type:                "DEBIT",
		TransactionCategory: "CASHBACK",
		Amount:              payment.OfferAmount,
		Currency:            payment.Currency,
		Status:              "SUCCESS",
	}, nil
}

func generateCashbackCreditTransaction(
	payment domain.Payment,
) (domain.Transaction, error) {

	transactionID, err := newPaymentID()
	if err != nil {
		return domain.Transaction{}, err
	}

	return domain.Transaction{
		ID:                  transactionID,
		PaymentID:           payment.ID,
		AccountID:           payment.SenderID,
		Type:                "CREDIT",
		TransactionCategory: "CASHBACK",
		Amount:              payment.OfferAmount,
		Currency:            payment.Currency,
		Status:              "SUCCESS",
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
