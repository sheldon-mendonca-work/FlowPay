package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flowpay/offer-service/internal/constants"
	"flowpay/offer-service/internal/domain"
	offerCreateDTO "flowpay/offer-service/internal/dto/OfferCreate"
	offerRedeemDTO "flowpay/offer-service/internal/dto/OfferRedeem"
	offerReserveDTO "flowpay/offer-service/internal/dto/OfferReserve"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/offer-service/internal/types"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/utils"
	"fmt"
	"time"
)

type OfferRepository interface {
	CreateOffer(ctx context.Context, tx *sql.Tx, newOfferItem domain.OfferEntity) error
	GetOfferByIDForUpdate(ctx context.Context, tx *sql.Tx, offerID string) (domain.OfferEntity, error)
	IncrementReservedCount(
		ctx context.Context,
		tx *sql.Tx,
		offerID string,
	) error
	ConvertReservedToRedeemed(
		ctx context.Context,
		tx *sql.Tx,
		offerID string,
	) error
}
type CompanyRepository interface {
}
type OfferEventsRepository interface {
	CreateEvent(
		ctx context.Context,
		tx *sql.Tx,
		event domain.OfferEventEntity,
	) error
}
type AccountsRepository interface {
	CreateNewAccountForOffer(ctx context.Context, tx *sql.Tx, account domain.Account) error
}
type OfferRedemptionIdempotencyRepository interface {
	ClaimOrGet(ctx context.Context, idempotency domain.OfferRedemptionIdempotencyKeyEntity) (domain.OfferRedemptionIdempotencyKeyEntity, bool, error)
	MarkFailed(
		tx *sql.Tx,
		ctx context.Context,
		idempotencyKey string,
		errorCode string,
		errorMessage string,
		ownerToken string,
	) error
	MarkCompleted(tx *sql.Tx, ctx context.Context, idempotencyKey string, responseBody string, redemptionID string, ownerToken string) error
}

type OfferReservationIdempotencyRepository interface {
	ClaimOrGet(ctx context.Context, idempotency domain.OfferReservationIdempotencyKeyEntity) (domain.OfferReservationIdempotencyKeyEntity, bool, error)
	MarkCompleted(
		tx *sql.Tx,
		ctx context.Context,
		idempotencyKey string,
		responseBody string,
		reservationID string,
		paymentID string,
		ownerToken string,
	) error

	MarkFailed(
		tx *sql.Tx,
		ctx context.Context,
		idempotencyKey string,
		errorCode string,
		errorMessage string,
		ownerToken string,
	) error
}

type OfferIdempotencyRepository interface {
	ClaimOrGet(ctx context.Context, idempotency domain.OfferIdempotencyKey) (domain.OfferIdempotencyKey, bool, error)
	MarkFailed(
		tx *sql.Tx,
		ctx context.Context,
		idempotencyKey string,
		errorCode string,
		errorMessage string,
		ownerToken string,
	) error
	MarkCompleted(tx *sql.Tx, ctx context.Context, idempotencyKey string, responseBody string, offerID string, ownerToken string) error
}
type OfferRedemptionsRepository interface {
	CreateRedemption(ctx context.Context, tx *sql.Tx, redemption domain.OfferRedemptionEntity) error
}
type OfferReservationsRepository interface {
	CreateReservation(ctx context.Context, tx *sql.Tx, reservation domain.OfferReservationEntity) error
	CountActiveReservationsForAccount(
		ctx context.Context,
		tx *sql.Tx,
		offerID string,
		accountID string,
	) (int, error)
	GetReservationByID(ctx context.Context, tx *sql.Tx, reservationID string) (domain.OfferReservationEntity, error)
	MarkReservationAsRedeemed(ctx context.Context, tx *sql.Tx, reservationID string) error
}
type OfferOutboxRepository interface {
	InsertOfferOutboxEvent(ctx context.Context, tx *sql.Tx, offerOutboxEvent domain.OfferOutboxEvent) error
	MarkPublished(ctx context.Context, tx *sql.Tx, eventID string, eventType string) error
	MarkFailed(ctx context.Context, eventID string, errorCode string, errorText string, eventType string) error
	MarkRetryableFailure(ctx context.Context, eventID string, errorCode string, errorText string) error
}
type UserRepository interface {
	GetUsersByUserAndCompanyId(ctx context.Context, tx *sql.Tx, companyId string, userId string) (domain.UsersEntity, error)
}

type OfferService struct {
	db                                    *sql.DB
	offerRepository                       OfferRepository
	companyRepository                     CompanyRepository
	offerEventsRepository                 OfferEventsRepository
	offerIdempotencyRepository            OfferIdempotencyRepository
	offerRedemptionIdempotencyRepository  OfferRedemptionIdempotencyRepository
	offerReservationIdempotencyRepository OfferReservationIdempotencyRepository
	offerRedemptionRepository             OfferRedemptionsRepository
	offerReservationRepository            OfferReservationsRepository
	offerOutboxRepository                 OfferOutboxRepository
	accountRepository                     AccountsRepository
	usersRepository                       UserRepository
}

func NewOfferService(db *sql.DB, offerRepository OfferRepository,
	companyRepository CompanyRepository,
	offerEventsRepository OfferEventsRepository,
	offerIdempotencyRepository OfferIdempotencyRepository,
	offerRedemptionsRepository OfferRedemptionsRepository,
	offerReservationsRepository OfferReservationsRepository,
	userRepository UserRepository,
	offerReservationIdempotencyRepository OfferReservationIdempotencyRepository,
	offerRedemptionIdempotencyRepository OfferRedemptionIdempotencyRepository,
	accountRepository AccountsRepository,
	offerOutboxRepository OfferOutboxRepository,
) *OfferService {
	return &OfferService{
		db:                                    db,
		offerRepository:                       offerRepository,
		companyRepository:                     companyRepository,
		offerEventsRepository:                 offerEventsRepository,
		offerIdempotencyRepository:            offerIdempotencyRepository,
		offerRedemptionRepository:             offerRedemptionsRepository,
		offerReservationRepository:            offerReservationsRepository,
		usersRepository:                       userRepository,
		offerRedemptionIdempotencyRepository:  offerRedemptionIdempotencyRepository,
		offerReservationIdempotencyRepository: offerReservationIdempotencyRepository,
		offerOutboxRepository:                 offerOutboxRepository,
		accountRepository:                     accountRepository,
	}
}

func logOfferStepFailure(ctx context.Context, req offerCreateDTO.OfferCreationRequestDTO, idempotencyKey string, step string, err error) {
	logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_step_failed", logger.Fields{
		"step":            step,
		"idempotency_key": idempotencyKey,
		"offer_code":      req.OfferCode,
		"offer_type":      req.OfferType,
		"created_by":      req.CreatedBy,
		"company_id":      req.CompanyId,
		"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
		"error":           err.Error(),
	})
}

func logOfferReserveStepFailure(ctx context.Context, req offerReserveDTO.OfferReservationRequestDTO, idempotencyKey string, offerID string, step string, err error) {
	logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_step_failed", logger.Fields{
		"step":            step,
		"idempotency_key": idempotencyKey,
		"offer_id":        offerID,
		"payment_id":      req.PaymentID,
		"account_id":      req.AccountID,
		"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
		"error":           err.Error(),
	})
}

func logOfferRedeemStepFailure(ctx context.Context, req offerRedeemDTO.OfferRedemptionRequestDTO, idempotencyKey string, offerID string, step string, err error) {
	logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_step_failed", logger.Fields{
		"step":            step,
		"idempotency_key": idempotencyKey,
		"offer_id":        offerID,
		"payment_id":      req.PaymentID,
		"account_id":      req.AccountID,
		"reservation_id":  req.ReservationID,
		"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
		"error":           err.Error(),
	})
}

func cachedIdempotencyResult(record domain.OfferIdempotencyKey) (offerCreateDTO.OfferCreationResponseDTO, error) {
	switch record.Status {
	case "COMPLETED":
		var cachedResponse offerCreateDTO.OfferCreationResponseDTO
		if err := json.Unmarshal([]byte(record.ResponseBody), &cachedResponse); err != nil {
			return offerCreateDTO.OfferCreationResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	case "FAILED":
		return offerCreateDTO.OfferCreationResponseDTO{}, replayableIdempotencyError(record)
	default:
		return offerCreateDTO.OfferCreationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, record.IdempotencyKey)
	}
}

func cachedReserveIdempotencyResult(record domain.OfferReservationIdempotencyKeyEntity) (offerReserveDTO.OfferReservationResponseDTO, error) {
	switch record.Status {
	case "COMPLETED":
		var cachedResponse offerReserveDTO.OfferReservationResponseDTO
		if err := json.Unmarshal([]byte(record.ResponseBody), &cachedResponse); err != nil {
			return offerReserveDTO.OfferReservationResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	case "FAILED":
		return offerReserveDTO.OfferReservationResponseDTO{}, replayableReserveIdempotencyError(record)
	default:
		return offerReserveDTO.OfferReservationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, record.IdempotencyKey)
	}
}

func cachedRedeemIdempotencyResult(record domain.OfferRedemptionIdempotencyKeyEntity) (offerRedeemDTO.OfferRedemptionResponseDTO, error) {
	switch record.Status {
	case "COMPLETED":
		var cachedResponse offerRedeemDTO.OfferRedemptionResponseDTO
		if err := json.Unmarshal([]byte(record.ResponseBody), &cachedResponse); err != nil {
			return offerRedeemDTO.OfferRedemptionResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	case "FAILED":
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, replayableRedeemIdempotencyError(record)
	default:
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, record.IdempotencyKey)
	}
}

func replayableIdempotencyError(record domain.OfferIdempotencyKey) error {
	switch record.ErrorCode {
	default:
		if record.ErrorMessage != "" {
			return errors.New(record.ErrorMessage)
		}
		return flowpayOfferErrors.ErrCreateOfferFailed
	}
}

func replayableReserveIdempotencyError(record domain.OfferReservationIdempotencyKeyEntity) error {
	switch record.ErrorCode {
	default:
		if record.ErrorMessage != "" {
			return errors.New(record.ErrorMessage)
		}
		return flowpayOfferErrors.ErrReserveOfferFailed
	}
}

func replayableRedeemIdempotencyError(record domain.OfferRedemptionIdempotencyKeyEntity) error {
	switch record.ErrorCode {
	default:
		if record.ErrorMessage != "" {
			return errors.New(record.ErrorMessage)
		}
		return flowpayOfferErrors.ErrRedeemOfferFailed
	}
}

func shouldPersistFailedIdempotency(err error) bool {
	switch {
	case err == nil:
		return false
	case
		errors.Is(err, flowpayOfferErrors.ErrOfferCodeRequired),
		errors.Is(err, flowpayOfferErrors.ErrCreatedByRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferTypeRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferTypeInvalid),
		errors.Is(err, flowpayOfferErrors.ErrCompanyIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrCompanyIdIsInvalid),
		errors.Is(err, flowpayOfferErrors.ErrUserNotFound),
		errors.Is(err, flowpayOfferErrors.ErrUserNotInCompany),
		errors.Is(err, flowpayOfferErrors.ErrOfferAmountOrPercentageRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferAmountInvalid),
		errors.Is(err, flowpayOfferErrors.ErrOfferPercentageInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxBenefitAmountInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxRedemptionsPerUserInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxRedemptionsInvalid),
		errors.Is(err, flowpayOfferErrors.ErrStartTimeRequired),
		errors.Is(err, flowpayOfferErrors.ErrEndTimeRequired),
		errors.Is(err, flowpayOfferErrors.ErrStartTimeAfterEndTime),
		errors.Is(err, flowpayOfferErrors.ErrAccountIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrPaymentIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrReservationIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrRedemptionIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferIdRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferNotActive),
		errors.Is(err, flowpayOfferErrors.ErrOfferExpired),
		errors.Is(err, flowpayOfferErrors.ErrOfferNotStarted),
		errors.Is(err, flowpayOfferErrors.ErrOfferRedemptionLimitReached),
		errors.Is(err, flowpayOfferErrors.ErrOfferReservationLimitReached),
		errors.Is(err, flowpayOfferErrors.ErrPaymentAlreadyReserved),
		errors.Is(err, flowpayOfferErrors.ErrInvalidRequestBody),
		errors.Is(err, flowpayOfferErrors.ErrCreateOfferFailed),
		errors.Is(err, flowpayOfferErrors.ErrReserveOfferFailed),
		errors.Is(err, flowpayOfferErrors.ErrRedeemOfferFailed),
		errors.Is(err, flowpayOfferErrors.ErrMinimumPaymentAmountNegative),
		errors.Is(err, flowpayOfferErrors.ErrMaximumPaymentAmountNegative),
		errors.Is(err, flowpayOfferErrors.ErrMinimumPaymentAmountInvalid),
		errors.Is(err, flowpayOfferErrors.ErrReservationAccountMismatch),
		errors.Is(err, flowpayOfferErrors.ErrReservationPaymentMismatch),
		errors.Is(err, flowpayOfferErrors.ErrReservationOfferMismatch),
		errors.Is(err, flowpayOfferErrors.ErrReservationExpired),
		errors.Is(err, flowpayOfferErrors.ErrReservationReservationIDMismatch),
		errors.Is(err, flowpayOfferErrors.ErrReservationNotReserved):
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

func reservationExpiryFromNow() time.Time {
	return time.Now().UTC().Add(15 * time.Minute)
}

func (r *OfferService) CreateNewOffer(ctx context.Context, req offerCreateDTO.OfferCreationRequestDTO, idempotencyKey string, requestID string, traceID string) (offerCreateDTO.OfferCreationResponseDTO, error) {

	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, fmt.Errorf("failed to extract request json: %w", err)
	}

	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	offerId, err := generateRandomId()
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	ownerToken, err := generateRandomId()
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	promotionPoolAccountId, err := generateRandomId()
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	idempotencyPayload := domain.OfferIdempotencyKey{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		OfferID:        offerId,
		Status:         "IN_PROGRESS",
		OwnerToken:     ownerToken,
		LockedUntil:    leaseExpiryFromNow(),
	}

	existingIdempotency, idempotencyClaimed, err := r.offerIdempotencyRepository.ClaimOrGet(ctx, idempotencyPayload)
	if err != nil {
		logOfferStepFailure(ctx, req, idempotencyKey, "idempotency_claim_or_get", err)
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	if !idempotencyClaimed {
		if existingIdempotency.RequestHash != payloadHash {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_idempotency_mismatch", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_code":      req.OfferCode,
				"offer_type":      req.OfferType,
				"created_by":      req.CreatedBy,
				"company_id":      req.CompanyId,
				"error_type":      flowpayOfferErrors.ErrorTypeIdempotencyMismatch,
			})
			return offerCreateDTO.OfferCreationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, idempotencyKey)
			logOfferStepFailure(ctx, req, idempotencyKey, "idempotency_in_progress", err)
			return offerCreateDTO.OfferCreationResponseDTO{}, err
		}

		cachedResponse, err := cachedIdempotencyResult(existingIdempotency)
		if err != nil {
			logOfferStepFailure(ctx, req, idempotencyKey, "idempotency_cached_result", err)
			return offerCreateDTO.OfferCreationResponseDTO{}, err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"trace_id":        traceID,
			"status":          existingIdempotency.Status,
			"offer_id":        cachedResponse.OfferID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayOfferErrors.ErrorTypeNone,
		})
		logger.LogPlain(ctx, constants.ServiceName, "served cached idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, nil
	}

	if existingIdempotency.OfferID != "" {
		offerId = existingIdempotency.OfferID
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	txClosed := false
	rollbackDueToError := false
	logger.LogPlain(ctx, constants.ServiceName, "started offer transaction offer_code = %s offer_type=%s created_by=%s company_id=%s", req.OfferCode, req.OfferType, req.CreatedBy, req.CompanyId)

	defer func() {
		if txClosed {
			return
		}

		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_code":      req.OfferCode,
				"offer_type":      req.OfferType,
				"created_by":      req.CreatedBy,
				"company_id":      req.CompanyId,
				"error_type":      flowpayOfferErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_code":      req.OfferCode,
				"offer_type":      req.OfferType,
				"created_by":      req.CreatedBy,
				"company_id":      req.CompanyId,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	rollbackTechnicalFailure := func(step string, err error) (offerCreateDTO.OfferCreationResponseDTO, error) {
		rollbackDueToError = true
		logOfferStepFailure(ctx, req, idempotencyKey, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_code":      req.OfferCode,
				"offer_type":      req.OfferType,
				"created_by":      req.CreatedBy,
				"company_id":      req.CompanyId,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_code":      req.OfferCode,
				"offer_type":      req.OfferType,
				"created_by":      req.CreatedBy,
				"company_id":      req.CompanyId,
				"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
			})
		}
		txClosed = true
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (offerCreateDTO.OfferCreationResponseDTO, error) {
		rollbackDueToError = true
		logOfferStepFailure(ctx, req, idempotencyKey, step, err)
		if markErr := r.offerIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayOfferErrors.ToOfferErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}

	// validate company id and user ID
	_, err = r.usersRepository.GetUsersByUserAndCompanyId(ctx, tx, req.CompanyId, req.CreatedBy)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("account_validation", err)
		}

		return rollbackTechnicalFailure("account_validation", err)
	}

	newOfferItem := domain.OfferEntity{
		ID:                     offerId,
		OfferCode:              req.OfferCode,
		OfferType:              req.OfferType,
		OfferAmount:            req.OfferAmount,
		OfferPercentage:        req.OfferPercentage,
		MaxBenefitAmount:       req.MaxBenefitAmount,
		MinimumPaymentAmount:   req.MinimumPaymentAmount,
		MaximumPaymentAmount:   req.MaximumPaymentAmount,
		MaxRedemptions:         req.MaxRedemptions,
		MaxRedemptionsPerUser:  req.MaxRedemptionsPerUser,
		RedeemedCount:          0,
		ReservedCount:          0,
		IdempotencyKey:         idempotencyKey,
		Status:                 "DRAFT",
		Version:                1,
		PromotionPoolAccountId: promotionPoolAccountId,
		CreatedBy:              req.CreatedBy,
		BudgetAmount:           req.BudgetAmount,
		StartTime:              req.StartTime,
		EndTime:                req.EndTime,
	}

	// create offer
	err = r.offerRepository.CreateOffer(ctx, tx, newOfferItem)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_creation", err)
		}

		return rollbackTechnicalFailure("offer_creation", err)
	}

	metadata := map[string]interface{}{
		"offer_code":                newOfferItem.OfferCode,
		"offer_type":                newOfferItem.OfferType,
		"created_by":                newOfferItem.CreatedBy,
		"company_id":                req.CompanyId,
		"promotion_pool_account_id": promotionPoolAccountId,
	}

	metadataBytes, _ := json.Marshal(metadata)
	offerEventID, err := generateRandomId()
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_creation", err)
		}

		return rollbackTechnicalFailure("offer_creation", err)
	}

	offerEvent := domain.OfferEventEntity{
		ID:        offerEventID,
		OfferID:   newOfferItem.ID,
		EventType: string(types.OFFER_CREATED),
		ActorID:   newOfferItem.CreatedBy,
		ActorType: "USER",
		Metadata:  metadataBytes,
	}
	err = r.offerEventsRepository.CreateEvent(
		ctx,
		tx,
		offerEvent,
	)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_creation", err)
		}

		return rollbackTechnicalFailure("offer_creation", err)
	}

	promotionPoolAccount := domain.Account{
		ID: promotionPoolAccountId,
		AccountName: fmt.Sprintf(
			"%s_%s_CASHBACK_POOL",
			newOfferItem.CreatedBy,
			newOfferItem.OfferCode,
		),
		AccountType:          "PROMOTION_POOL",
		AllowNegativeBalance: true,
		Balance:              *newOfferItem.BudgetAmount, // <- total campaign budget
		Currency:             "INR",
	}

	err = r.accountRepository.CreateNewAccountForOffer(ctx, tx, promotionPoolAccount)
	if err != nil {
		return rollbackTechnicalFailure("promotional_account_creation", err)
	}

	response := offerCreateDTO.OfferCreationResponseDTO{
		OfferID: offerId,
		Status:  types.SUCCESS,
	}

	responseBody, err := json.Marshal(response)
	if err != nil {
		return rollbackTechnicalFailure("idempotency_response_encode", fmt.Errorf("encode idempotency response: %w", err))
	}

	if err := r.offerIdempotencyRepository.MarkCompleted(tx, ctx, idempotencyKey, string(responseBody), offerId, ownerToken); err != nil {
		return rollbackTechnicalFailure("idempotency_mark_completed", err)
	}

	logger.LogPlain(ctx, constants.ServiceName, "updated idempotency entries offer_code = %s offer_type=%s created_by=%s company_id=%s", req.OfferCode, req.OfferType, req.CreatedBy, req.CompanyId)

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logOfferStepFailure(ctx, req, idempotencyKey, "tx_commit", err)
		return offerCreateDTO.OfferCreationResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_tx_committed", logger.Fields{
		"idempotency_key": idempotencyKey,
		"trace_id":        traceID,
		"offer_id":        offerId,
		"offer_code":      req.OfferCode,
		"offer_type":      req.OfferType,
		"created_by":      req.CreatedBy,
		"company_id":      req.CompanyId,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})
	logger.LogPlain(ctx, constants.ServiceName, "committed offer creation transaction offer_id=%s", offerId)

	return response, nil
}

func (s *OfferService) ReserveOffer(ctx context.Context, req offerReserveDTO.OfferReservationRequestDTO, idempotencyKey string, offerID string, requestID string, traceID string) (offerReserveDTO.OfferReservationResponseDTO, error) {
	requestForHash := struct {
		OfferID   string `json:"offer_id"`
		AccountID string `json:"account_id"`
	}{
		OfferID:   offerID,
		AccountID: req.AccountID,
	}

	reqAsBytes, err := json.Marshal(requestForHash)
	if err != nil {
		return offerReserveDTO.OfferReservationResponseDTO{}, fmt.Errorf("failed to extract request json: %w", err)
	}

	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return offerReserveDTO.OfferReservationResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	reservationId, err := generateRandomId()
	if err != nil {
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	ownerToken, err := generateRandomId()
	if err != nil {
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	idempotencyPayload := domain.OfferReservationIdempotencyKeyEntity{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		OfferID:        offerID,
		ReservationID:  reservationId,
		PaymentID:      req.PaymentID,
		Status:         "IN_PROGRESS",
		OwnerToken:     ownerToken,
		LockedUntil:    leaseExpiryFromNow(),
	}

	existingIdempotency, idempotencyClaimed, err := s.offerReservationIdempotencyRepository.ClaimOrGet(ctx, idempotencyPayload)
	if err != nil {
		logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, "idempotency_claim_or_get", err)
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	if !idempotencyClaimed {
		if existingIdempotency.RequestHash != payloadHash {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_reserve_idempotency_mismatch", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  existingIdempotency.ReservationID,
				"account_id":      req.AccountID,
				"error_type":      flowpayOfferErrors.ErrorTypeIdempotencyMismatch,
			})
			return offerReserveDTO.OfferReservationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, idempotencyKey)
			logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, "offer_reservation_idempotency_in_progress", err)
			return offerReserveDTO.OfferReservationResponseDTO{}, err
		}

		cachedResponse, err := cachedReserveIdempotencyResult(existingIdempotency)
		if err != nil {
			logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, "offer_reservation_idempotency_cached_result", err)
			return offerReserveDTO.OfferReservationResponseDTO{}, err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_reservation_idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"trace_id":        traceID,
			"status":          existingIdempotency.Status,
			"offer_id":        cachedResponse.OfferID,
			"payment_id":      cachedResponse.PaymentID,
			"reservation_id":  cachedResponse.ReservationID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayOfferErrors.ErrorTypeNone,
		})
		logger.LogPlain(ctx, constants.ServiceName, "served cached offer reservation idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, nil
	}

	if idempotencyClaimed {
		if existingIdempotency.ReservationID != "" {
			reservationId = existingIdempotency.ReservationID
		}

		if existingIdempotency.PaymentID != "" {
			idempotencyPayload.PaymentID = existingIdempotency.PaymentID
		}

		if existingIdempotency.OfferID != "" {
			idempotencyPayload.OfferID = existingIdempotency.OfferID
		}

	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	txClosed := false
	rollbackDueToError := false
	logger.LogPlain(ctx, constants.ServiceName, "started offer reservation transaction reservation_id = %s offer_id=%s payment_id=%s", reservationId, offerID, req.PaymentID)

	defer func() {
		if txClosed {
			return
		}

		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_reserve_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationId,
				"error_type":      flowpayOfferErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_reserve_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationId,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	rollbackTechnicalFailure := func(step string, err error) (offerReserveDTO.OfferReservationResponseDTO, error) {
		rollbackDueToError = true
		logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_reserve_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationId,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_reserve_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationId,
				"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
			})
		}
		txClosed = true
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (offerReserveDTO.OfferReservationResponseDTO, error) {
		rollbackDueToError = true
		logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, step, err)
		if markErr := s.offerReservationIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayOfferErrors.ToOfferErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}

		offerRejectedOutboxEvent := types.OfferRejectedEvent{
			PaymentID: req.PaymentID,
			AccountID: req.AccountID,
			OfferID:   offerID,

			SenderID:               req.SenderID,
			ReceiverID:             req.ReceiverID,
			PaymentIdempotencyKey:  req.PaymentIdempotencyKey,
			PromotionPoolAccountId: req.PromotionPoolAccountId,
			PaymentOwnerToken:      req.PaymentOwnerToken,
			TraceID:                req.TraceID,
			RequestID:              req.RequestID,
			Amount:                 req.Amount,
			Currency:               req.Currency,

			ErrorCode: "OFFER_NOT_FOUND",
			ErrorText: flowpayOfferErrors.ErrOfferNotFound.Error(),
		}

		outboxID, err := generateRandomId()
		if err != nil {
			return rollbackTechnicalFailure(
				"offer_reserved_outbox_id",
				err,
			)
		}

		payloadBytes, err := json.Marshal(offerRejectedOutboxEvent)
		if err != nil {
			return rollbackTechnicalFailure(
				"offer_reserved_payload",
				err,
			)
		}

		err = s.offerOutboxRepository.InsertOfferOutboxEvent(
			ctx,
			tx,
			domain.OfferOutboxEvent{
				ID:             outboxID,
				AggregateType:  "OFFER",
				AggregateID:    offerID,
				EventType:      "OFFER_REJECTED",
				EventVersion:   1,
				IdempotencyKey: idempotencyKey,
				Payload:        string(payloadBytes),
				Status:         "PENDING",
				TraceID:        traceID,
				RequestID:      requestID,
				RetryCount:     0,
			},
		)

		if err != nil {
			return rollbackTechnicalFailure(
				"offer_reserved_outbox_insert",
				err,
			)
		}

		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}

	offer, err := s.offerRepository.GetOfferByIDForUpdate(ctx, tx, offerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {

			return markFailedAndCommit(
				"offer_lookup",
				flowpayOfferErrors.ErrOfferNotFound,
			)
		}

		return rollbackTechnicalFailure("offer_lookup", err)
	}

	if offer.Status != "ACTIVE" {
		return markFailedAndCommit(
			"offer_status_validation",
			flowpayOfferErrors.ErrOfferNotActive,
		)
	}

	now := time.Now().UTC()

	if now.Before(offer.StartTime) {
		return markFailedAndCommit(
			"offer_window_validation",
			flowpayOfferErrors.ErrOfferNotStarted,
		)
	}

	if now.After(offer.EndTime) {
		return markFailedAndCommit(
			"offer_window_validation",
			flowpayOfferErrors.ErrOfferExpired,
		)
	}

	reservationCount, err := s.offerReservationRepository.
		CountActiveReservationsForAccount(
			ctx,
			tx,
			offerID,
			req.AccountID,
		)

	if err != nil {
		return rollbackTechnicalFailure(
			"account_reservation_count",
			err,
		)
	}

	if reservationCount >= int(offer.MaxRedemptionsPerUser) {
		return markFailedAndCommit(
			"account_reservation_limit",
			flowpayOfferErrors.ErrOfferReservationLimitReached,
		)
	}

	newOfferReserveItem := domain.OfferReservationEntity{
		ID:             reservationId,
		OfferID:        offerID,
		PaymentID:      req.PaymentID,
		AccountID:      req.AccountID,
		IdempotencyKey: idempotencyKey,
		Status:         "RESERVED",
		ExpiresAt:      reservationExpiryFromNow(),
	}

	// create reservation
	err = s.offerReservationRepository.CreateReservation(ctx, tx, newOfferReserveItem)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_reservation_creation", err)
		}

		return rollbackTechnicalFailure("offer_reservation_creation", err)
	}

	// update reservation count
	err = s.offerRepository.IncrementReservedCount(ctx, tx, offerID)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_reservation_deduction", err)
		}

		return rollbackTechnicalFailure("offer_reservation_deduction", err)
	}

	response := offerReserveDTO.OfferReservationResponseDTO{
		ReservationID: newOfferReserveItem.ID,
		PaymentID:     newOfferReserveItem.PaymentID,
		OfferID:       newOfferReserveItem.OfferID,
		AccountID:     newOfferReserveItem.AccountID,
		Status:        newOfferReserveItem.Status,
		ExpiresAt:     newOfferReserveItem.ExpiresAt,
	}

	responseBody, err := json.Marshal(response)
	if err != nil {
		return rollbackTechnicalFailure("idempotency_response_encode", fmt.Errorf("encode idempotency response: %w", err))
	}

	if err := s.offerReservationIdempotencyRepository.MarkCompleted(tx, ctx, idempotencyKey, string(responseBody), reservationId, idempotencyPayload.PaymentID, ownerToken); err != nil {
		return rollbackTechnicalFailure("reservation_idempotency_mark_completed", err)
	}

	logger.LogPlain(ctx, constants.ServiceName, "updated reservation idempotency entries offer_id = %s payment_id=%s account_id=%s", offerID, req.PaymentID, req.AccountID)

	// create outbox Event
	offerReservedOutboxEvent := types.OfferReservedEvent{
		OfferID:       offerID,
		ReservationID: reservationId,
		PaymentID:     req.PaymentID,
		AccountID:     req.AccountID,

		SenderID:               req.SenderID,
		ReceiverID:             req.ReceiverID,
		Amount:                 req.Amount,
		Currency:               req.Currency,
		PaymentIdempotencyKey:  req.PaymentIdempotencyKey,
		PromotionPoolAccountId: req.PromotionPoolAccountId,
		PaymentOwnerToken:      req.PaymentOwnerToken,
		OfferType:              offer.OfferType,
		OfferAmount:            offer.OfferAmount,
		OfferPercentage:        offer.OfferPercentage,
		MaxBenefitAmount:       offer.MaxBenefitAmount,
		MinimumPaymentAmount:   offer.MinimumPaymentAmount,
		MaximumPaymentAmount:   offer.MaximumPaymentAmount,

		IdempotencyKey: idempotencyKey,
		TraceID:        req.TraceID,
		RequestID:      req.RequestID,
	}

	payloadBytes, err := json.Marshal(offerReservedOutboxEvent)
	if err != nil {
		return rollbackTechnicalFailure(
			"offer_reserved_payload",
			err,
		)
	}

	outboxID, err := generateRandomId()
	if err != nil {
		return rollbackTechnicalFailure(
			"offer_reserved_outbox_id",
			err,
		)
	}

	err = s.offerOutboxRepository.InsertOfferOutboxEvent(
		ctx,
		tx,
		domain.OfferOutboxEvent{
			ID:             outboxID,
			AggregateType:  "OFFER",
			AggregateID:    offerID,
			EventType:      "OFFER_RESERVED",
			EventVersion:   1,
			IdempotencyKey: idempotencyKey,
			Payload:        string(payloadBytes),
			Status:         "PENDING",
			TraceID:        traceID,
			RequestID:      requestID,
			RetryCount:     0,
		},
	)

	if err != nil {
		return rollbackTechnicalFailure(
			"offer_reserved_outbox_insert",
			err,
		)
	}

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logOfferReserveStepFailure(ctx, req, idempotencyKey, offerID, "tx_commit", err)
		return offerReserveDTO.OfferReservationResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_reserve_tx_committed", logger.Fields{
		"idempotency_key": idempotencyKey,
		"trace_id":        traceID,
		"reservation_id":  newOfferReserveItem.ID,
		"payment_id":      newOfferReserveItem.PaymentID,
		"offer_id":        newOfferReserveItem.OfferID,
		"account_id":      newOfferReserveItem.AccountID,
		"status":          newOfferReserveItem.Status,
		"expires_at":      newOfferReserveItem.ExpiresAt,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})
	logger.LogPlain(ctx, constants.ServiceName, "committed offer reservation transaction reservation_id=%s, offer_id=%s", reservationId, offerID)

	return response, nil
}

func (s *OfferService) RedeemOffer(ctx context.Context, req offerRedeemDTO.OfferRedemptionRequestDTO, idempotencyKey string, offerID string, requestID string, traceID string) (offerRedeemDTO.OfferRedemptionResponseDTO, error) {
	reservationID := req.ReservationID

	requestForHash := struct {
		OfferID       string `json:"offer_id"`
		AccountID     string `json:"account_id"`
		ReservationID string `json:"reservation_id"`
	}{
		OfferID:       offerID,
		AccountID:     req.AccountID,
		ReservationID: req.ReservationID,
	}

	reqAsBytes, err := json.Marshal(requestForHash)
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, fmt.Errorf("failed to extract request json: %w", err)
	}

	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	redemptionID, err := generateRandomId()
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	ownerToken, err := generateRandomId()
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	idempotencyPayload := domain.OfferRedemptionIdempotencyKeyEntity{
		IdempotencyKey: idempotencyKey,
		RequestHash:    payloadHash,
		OfferID:        offerID,
		RedemptionID:   redemptionID,
		PaymentID:      req.PaymentID,
		ReservationID:  req.ReservationID,
		Status:         "IN_PROGRESS",
		OwnerToken:     ownerToken,
		LockedUntil:    leaseExpiryFromNow(),
	}

	existingIdempotency, idempotencyClaimed, err := s.offerRedemptionIdempotencyRepository.ClaimOrGet(ctx, idempotencyPayload)
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	if !idempotencyClaimed {
		if existingIdempotency.RequestHash != payloadHash {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_redeem_idempotency_mismatch", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  existingIdempotency.ReservationID,
				"account_id":      req.AccountID,
				"error_type":      flowpayOfferErrors.ErrorTypeIdempotencyMismatch,
			})
			return offerRedeemDTO.OfferRedemptionResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, idempotencyKey)
			logOfferRedeemStepFailure(ctx, req, idempotencyKey, offerID, "offer_redemption_idempotency_in_progress", err)
			return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
		}

		cachedResponse, err := cachedRedeemIdempotencyResult(existingIdempotency)
		if err != nil {
			logOfferRedeemStepFailure(ctx, req, idempotencyKey, offerID, "offer_redemption_idempotency_in_progress", err)
			return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_redemption_idempotency_hit", logger.Fields{
			"idempotency_key": idempotencyKey,
			"trace_id":        traceID,
			"status":          existingIdempotency.Status,
			"offer_id":        cachedResponse.OfferID,
			"payment_id":      req.PaymentID,
			"reservation_id":  req.ReservationID,
			"error_code":      existingIdempotency.ErrorCode,
			"error_type":      flowpayOfferErrors.ErrorTypeNone,
		})
		logger.LogPlain(ctx, constants.ServiceName, "served cached offer redemption idempotency result for idempotency_key=%s status=%s", idempotencyKey, existingIdempotency.Status)
		return cachedResponse, nil
	}

	if idempotencyClaimed {
		if existingIdempotency.ReservationID != "" {
			reservationID = existingIdempotency.ReservationID
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	rollbackDueToError := false
	txClosed := false

	logger.LogPlain(ctx, constants.ServiceName, "started offer redemption transaction reservation_id = %s offer_id=%s payment_id=%s", reservationID, offerID, req.PaymentID)

	defer func() {
		if txClosed {
			return
		}

		rollbackErr := tx.Rollback()
		switch {
		case rollbackErr == nil && rollbackDueToError:
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_redeem_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationID,
				"error_type":      flowpayOfferErrors.ErrorTypeNone,
			})
		case rollbackErr != nil && rollbackErr != sql.ErrTxDone:
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_redeem_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationID,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		}
	}()

	rollbackTechnicalFailure := func(step string, err error) (offerRedeemDTO.OfferRedemptionResponseDTO, error) {
		rollbackDueToError = true
		logOfferRedeemStepFailure(ctx, req, idempotencyKey, offerID, step, err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_redeem_tx_rollback_failed", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationID,
				"error_type":      flowpayOfferErrors.ErrorTypeDBFailure,
				"error":           rollbackErr.Error(),
			})
		} else {
			logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_redeem_tx_rolled_back", logger.Fields{
				"idempotency_key": idempotencyKey,
				"trace_id":        traceID,
				"offer_id":        offerID,
				"payment_id":      req.PaymentID,
				"reservation_id":  reservationID,
				"error_type":      flowpayOfferErrors.ToOfferErrorType(err),
			})
		}
		txClosed = true
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (offerRedeemDTO.OfferRedemptionResponseDTO, error) {
		rollbackDueToError = true
		logOfferRedeemStepFailure(ctx, req, idempotencyKey, offerID, step, err)
		if markErr := s.offerRedemptionIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayOfferErrors.ToOfferErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}

	// validate
	reservation, err := s.offerReservationRepository.GetReservationByID(ctx, tx, reservationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return markFailedAndCommit(
				"offer_lookup",
				flowpayOfferErrors.ErrReservationNotFound,
			)
		}

		return rollbackTechnicalFailure("offer_lookup", err)
	}

	if reservation.AccountID != req.AccountID {
		return markFailedAndCommit(
			"account_validation",
			flowpayOfferErrors.ErrReservationAccountMismatch,
		)
	}

	if reservation.PaymentID != req.PaymentID {
		return markFailedAndCommit(
			"account_validation",
			flowpayOfferErrors.ErrReservationPaymentMismatch,
		)
	}

	if reservation.OfferID != offerID {
		return markFailedAndCommit(
			"account_validation",
			flowpayOfferErrors.ErrReservationOfferMismatch,
		)
	}

	if reservation.ExpiresAt.Before(time.Now().UTC()) {
		return markFailedAndCommit(
			"reservation_expired",
			flowpayOfferErrors.ErrReservationExpired,
		)
	}

	if reservation.Status != "RESERVED" {
		return markFailedAndCommit(
			"account_validation",
			flowpayOfferErrors.ErrReservationNotReserved,
		)
	}

	newOfferRedeemItem := domain.OfferRedemptionEntity{
		ID:             redemptionID,
		OfferID:        reservation.OfferID,
		AccountID:      reservation.AccountID,
		PaymentID:      reservation.PaymentID,
		ReservationID:  reservation.ID,
		IdempotencyKey: idempotencyKey,
		Status:         "SUCCESS",
	}

	// insert offer redeem item
	err = s.offerRedemptionRepository.CreateRedemption(ctx, tx, newOfferRedeemItem)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_redemption_creation", err)
		}

		return rollbackTechnicalFailure("offer_redemption_creation", err)
	}

	// update offer reservation
	err = s.offerReservationRepository.MarkReservationAsRedeemed(ctx, tx, newOfferRedeemItem.ReservationID)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_reservation_reserved_to_redeemed", err)
		}

		return rollbackTechnicalFailure("offer_reservation_reserved_to_redeemed", err)
	}

	// update offer
	err = s.offerRepository.ConvertReservedToRedeemed(ctx, tx, reservation.OfferID)
	if err != nil {
		if isDeterministicBusinessFailure(err) {
			return markFailedAndCommit("offer_reserved_to_redeem_counter", err)
		}

		return rollbackTechnicalFailure("offer_reserved_to_redeem_counter", err)
	}

	// update offer redeem idempotency
	response := offerRedeemDTO.OfferRedemptionResponseDTO{
		RedemptionID:   redemptionID,
		PaymentID:      req.PaymentID,
		OfferID:        offerID,
		Status:         newOfferRedeemItem.Status,
		RedemptionTime: time.Now(),
	}

	responseBody, err := json.Marshal(response)
	if err != nil {
		return rollbackTechnicalFailure("idempotency_response_encode", fmt.Errorf("encode idempotency response: %w", err))
	}

	err = s.offerRedemptionIdempotencyRepository.MarkCompleted(tx, ctx, idempotencyKey, string(responseBody), redemptionID, ownerToken)
	if err != nil {
		return rollbackTechnicalFailure("offer_redemption_idempotency_completed", err)
	}

	// Commit all transactions
	if err := tx.Commit(); err != nil {
		rollbackDueToError = true
		logOfferRedeemStepFailure(ctx, req, idempotencyKey, offerID, "tx_commit", err)
		return offerRedeemDTO.OfferRedemptionResponseDTO{}, err
	}
	txClosed = true

	logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_redeem_tx_committed", logger.Fields{
		"idempotency_key": idempotencyKey,
		"trace_id":        traceID,
		"redemption_id":   newOfferRedeemItem.ID,
		"reservation_id":  newOfferRedeemItem.ReservationID,
		"payment_id":      newOfferRedeemItem.PaymentID,
		"offer_id":        newOfferRedeemItem.OfferID,
		"account_id":      newOfferRedeemItem.AccountID,
		"status":          newOfferRedeemItem.Status,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})
	logger.LogPlain(ctx, constants.ServiceName, "committed offer reservation transaction redemption_id=%s, reservation_id=%s, offer_id=%s", redemptionID, reservationID, offerID)

	return response, nil
}

func generateRandomId() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate random id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
