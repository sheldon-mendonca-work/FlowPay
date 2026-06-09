package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flowpay/offer-service/internal/constants"
	"flowpay/offer-service/internal/domain"
	"flowpay/offer-service/internal/dto"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/offer-service/internal/types"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/utils"
	"fmt"
	"time"
)

type OfferRepository interface {
	CreateOffer(ctx context.Context, tx *sql.Tx, newOfferItem domain.OfferEntity) error
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
}
type OfferReservationsRepository interface {
}
type UserRepository interface {
	GetUsersByUserAndCompanyId(ctx context.Context, tx *sql.Tx, companyId string, userId string) (domain.UsersEntity, error)
}

type OfferService struct {
	db                         *sql.DB
	offerRepository            OfferRepository
	companyRepository          CompanyRepository
	offerEventsRepository      OfferEventsRepository
	offerIdempotencyRepository OfferIdempotencyRepository
	offerRedemptionRepository  OfferRedemptionsRepository
	offerReservationRepository OfferReservationsRepository
	usersRepository            UserRepository
}

func NewOfferService(db *sql.DB, offerRepository OfferRepository,
	companyRepository CompanyRepository,
	offerEventsRepository OfferEventsRepository,
	offerIdempotencyRepository OfferIdempotencyRepository,
	offerRedemptionsRepository OfferRedemptionsRepository,
	offerReservationsRepository OfferReservationsRepository,
	userRepository UserRepository,
) *OfferService {
	return &OfferService{
		db:                         db,
		offerRepository:            offerRepository,
		companyRepository:          companyRepository,
		offerEventsRepository:      offerEventsRepository,
		offerIdempotencyRepository: offerIdempotencyRepository,
		offerRedemptionRepository:  offerRedemptionsRepository,
		offerReservationRepository: offerReservationsRepository,
		usersRepository:            userRepository,
	}
}

func logOfferStepFailure(ctx context.Context, req dto.OfferCreationRequestDTO, idempotencyKey string, step string, err error) {
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

func cachedIdempotencyResult(record domain.OfferIdempotencyKey) (dto.OfferCreationResponseDTO, error) {
	switch record.Status {
	case "COMPLETED":
		var cachedResponse dto.OfferCreationResponseDTO
		if err := json.Unmarshal([]byte(record.ResponseBody), &cachedResponse); err != nil {
			return dto.OfferCreationResponseDTO{}, fmt.Errorf("decode idempotency response: %w", err)
		}
		return cachedResponse, nil
	case "FAILED":
		return dto.OfferCreationResponseDTO{}, replayableIdempotencyError(record)
	default:
		return dto.OfferCreationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, record.IdempotencyKey)
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
		errors.Is(err, flowpayOfferErrors.ErrUserNotInCompany),
		errors.Is(err, flowpayOfferErrors.ErrUserNotFound),
		errors.Is(err, flowpayOfferErrors.ErrOfferAmountOrPercentageRequired),
		errors.Is(err, flowpayOfferErrors.ErrOfferAmountInvalid),
		errors.Is(err, flowpayOfferErrors.ErrOfferPercentageInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxBenefitAmountInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxRedemptionsPerUserInvalid),
		errors.Is(err, flowpayOfferErrors.ErrMaxRedemptionsInvalid),
		errors.Is(err, flowpayOfferErrors.ErrStartTimeRequired),
		errors.Is(err, flowpayOfferErrors.ErrEndTimeRequired),
		errors.Is(err, flowpayOfferErrors.ErrStartTimeAfterEndTime):
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

func (r *OfferService) CreateNewOffer(ctx context.Context, req dto.OfferCreationRequestDTO, idempotencyKey string, requestID string, traceID string) (dto.OfferCreationResponseDTO, error) {

	reqAsBytes, err := json.Marshal(req)
	if err != nil {
		return dto.OfferCreationResponseDTO{}, fmt.Errorf("failed to extract request json: %w", err)
	}

	payloadHash, err := utils.ComputeHash(reqAsBytes)
	if err != nil {
		return dto.OfferCreationResponseDTO{}, fmt.Errorf("failed to compute hash: %w", err)
	}

	offerId, err := generateRandomId()
	if err != nil {
		return dto.OfferCreationResponseDTO{}, err
	}

	ownerToken, err := generateRandomId()
	if err != nil {
		return dto.OfferCreationResponseDTO{}, err
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
		return dto.OfferCreationResponseDTO{}, err
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
			return dto.OfferCreationResponseDTO{}, fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyMismatch, idempotencyKey)
		}

		if existingIdempotency.Status == "IN_PROGRESS" {
			err := fmt.Errorf("%w: idempotency_key=%s", flowpayOfferErrors.ErrIdempotencyInProgress, idempotencyKey)
			logOfferStepFailure(ctx, req, idempotencyKey, "idempotency_in_progress", err)
			return dto.OfferCreationResponseDTO{}, err
		}

		cachedResponse, err := cachedIdempotencyResult(existingIdempotency)
		if err != nil {
			logOfferStepFailure(ctx, req, idempotencyKey, "idempotency_cached_result", err)
			return dto.OfferCreationResponseDTO{}, err
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
		return dto.OfferCreationResponseDTO{}, err
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

	rollbackTechnicalFailure := func(step string, err error) (dto.OfferCreationResponseDTO, error) {
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
		return dto.OfferCreationResponseDTO{}, err
	}

	markFailedAndCommit := func(step string, err error) (dto.OfferCreationResponseDTO, error) {
		rollbackDueToError = true
		logOfferStepFailure(ctx, req, idempotencyKey, step, err)
		if markErr := r.offerIdempotencyRepository.MarkFailed(tx, ctx, idempotencyKey, flowpayOfferErrors.ToOfferErrorType(err), err.Error(), ownerToken); markErr != nil {
			return rollbackTechnicalFailure(step+"_mark_failed", markErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return rollbackTechnicalFailure(step+"_commit_failed", commitErr)
		}
		txClosed = true
		return dto.OfferCreationResponseDTO{}, err
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
		ID:                    offerId,
		OfferCode:             req.OfferCode,
		OfferType:             req.OfferType,
		OfferAmount:           req.OfferAmount,
		OfferPercentage:       req.OfferPercentage,
		MaxBenefitAmount:      req.MaxBenefitAmount,
		MinimumPaymentAmount:  req.MinimumPaymentAmount,
		MaximumPaymentAmount:  req.MaximumPaymentAmount,
		MaxRedemptions:        req.MaxRedemptions,
		MaxRedemptionsPerUser: req.MaxRedemptionsPerUser,
		RedeemedCount:         0,
		IdempotencyKey:        idempotencyKey,
		Status:                "DRAFT",
		Version:               1,
		CreatedBy:             req.CreatedBy,
		StartTime:             req.StartTime,
		EndTime:               req.EndTime,
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
		"offer_code": newOfferItem.OfferCode,
		"offer_type": newOfferItem.OfferType,
		"created_by": newOfferItem.CreatedBy,
		"company_id": req.CompanyId,
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
		EventType: "OFFER_CREATED",
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

	response := dto.OfferCreationResponseDTO{
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
		return dto.OfferCreationResponseDTO{}, err
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

func generateRandomId() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate random id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
