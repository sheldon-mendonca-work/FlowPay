package service

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/checks/payment"
	"flowpay/reconciliation-service/internal/dto"
	"flowpay/reconciliation-service/internal/models"
	"flowpay/reconciliation-service/internal/repository"
)

type ReconciliationService struct {
	db                    *sql.DB
	paymentRepository     *repository.PaymentRepository
	outboxRepository      *repository.OutboxEventsRepository
	idempotencyRepository *repository.PaymentIdempotencyRepository
	transactionRepository *repository.TransactionRepository
	accountRepository     *repository.AccountRepository
}

func NewReconciliationService(
	db *sql.DB,
	paymentRepository *repository.PaymentRepository,
	outboxRepository *repository.OutboxEventsRepository,
	idempotencyRepository *repository.PaymentIdempotencyRepository,
	transactionRepository *repository.TransactionRepository,
	accountRepository *repository.AccountRepository,
) *ReconciliationService {
	return &ReconciliationService{
		db:                    db,
		paymentRepository:     paymentRepository,
		outboxRepository:      outboxRepository,
		idempotencyRepository: idempotencyRepository,
		transactionRepository: transactionRepository,
		accountRepository:     accountRepository,
	}
}

func (s *ReconciliationService) GetPaymentsWithoutTransactions(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	paymentsList, err := s.paymentRepository.GetPaymentsWithoutTransactions(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.BuildPaymentsWithoutTransactionsAnomalies(paymentsList)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetPaymentsWithoutDebitOrCredit(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = tx.Rollback()
	}()

	paymentsList, err := s.paymentRepository.GetPaymentsWithInvalidTransactionPair(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.BuildPaymentsWithInvalidTransactionPairAnomalies(paymentsList)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetPaymentsWithoutIdempotencyRows(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	paymentsList, err := s.paymentRepository.GetPaymentsWithoutIdempotencyRows(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.BuildPaymentsWithoutIdempotencyRowsAnomalies(paymentsList)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetPaymentsWithIdempotencyPaymentIDMismatch(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	paymentsList, err := s.paymentRepository.GetPaymentsWithIdempotencyPaymentIDMismatch(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.PaymentsWithIdempotencyPaymentIDMismatchAnamolies(paymentsList)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetPaymentsMissingCompletedIdempotency(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	paymentsList, err := s.paymentRepository.GetPaymentsMissingCompletedIdempotency(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.PaymentsMissingCompletedIdempotencyAnamolies(paymentsList)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) RunPaymentChecks(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	paymentWithoutTransactionsAnomalies, err := s.GetPaymentsWithoutTransactions(ctx, checkType)
	if err != nil {
		return nil, err
	}

	response = append(response, paymentWithoutTransactionsAnomalies...)

	paymentWithoutCreditOrDebitAnamolies, err := s.GetPaymentsWithoutDebitOrCredit(ctx, checkType)
	if err != nil {
		return nil, err
	}

	response = append(response, paymentWithoutCreditOrDebitAnamolies...)

	paymentsWithoutIdempotencyRowsAnamolies, err := s.GetPaymentsWithoutIdempotencyRows(ctx, checkType)
	if err != nil {
		return nil, err
	}

	response = append(response, paymentsWithoutIdempotencyRowsAnamolies...)

	paymentsWithIdempotencyPaymentIDMismatchAnamolies, err := s.GetPaymentsWithIdempotencyPaymentIDMismatch(ctx, checkType)
	if err != nil {
		return nil, err
	}

	response = append(response, paymentsWithIdempotencyPaymentIDMismatchAnamolies...)
	// future checks
	// missingLedger, _ := s.GetPaymentsWithoutLedgerEntries(ctx)
	// response = append(response, missingLedger...)

	return response, nil
}

func (s *ReconciliationService) RunOutboxChecks(ctx context.Context) ([]models.Anomaly, error) {
	return []models.Anomaly{}, nil
}

func (s *ReconciliationService) RunIdempotencyChecks(ctx context.Context) ([]models.Anomaly, error) {
	return []models.Anomaly{}, nil
}

func (s *ReconciliationService) RunLedgerChecks(ctx context.Context) ([]models.Anomaly, error) {
	return []models.Anomaly{}, nil
}

func toAnomalyResponseDTOs(response []dto.AnomalyResponseDTO, anomalies []models.Anomaly) []dto.AnomalyResponseDTO {

	for _, anomaly := range anomalies {
		response = append(response, dto.AnomalyResponseDTO{
			CheckName:   anomaly.CheckName,
			EntityType:  anomaly.EntityType,
			EntityID:    anomaly.EntityID,
			Description: anomaly.Description,
			Severity:    string(anomaly.Severity),
		})
	}

	return response
}
