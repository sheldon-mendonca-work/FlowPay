package service

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/checks/payment"
	"flowpay/reconciliation-service/internal/dto"
	"flowpay/reconciliation-service/internal/models"
	"flowpay/reconciliation-service/internal/repository"
)

const PaymentChecksName = payment.PaymentsWithoutTransactionsCheckName

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

func (s *ReconciliationService) RunPaymentChecks(ctx context.Context) ([]dto.AnomalyResponseDTO, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	paymentsList, err := s.paymentRepository.GetPaymentsWithoutTransactions(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := payment.BuildPaymentsWithoutTransactionsAnomalies(paymentsList)
	return toAnomalyResponseDTOs(anomalies), nil
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

func toAnomalyResponseDTOs(anomalies []models.Anomaly) []dto.AnomalyResponseDTO {
	response := make([]dto.AnomalyResponseDTO, 0, len(anomalies))

	for _, anomaly := range anomalies {
		response = append(response, dto.AnomalyResponseDTO{
			EntityType:  anomaly.EntityType,
			EntityID:    anomaly.EntityID,
			Description: anomaly.Description,
			Severity:    string(anomaly.Severity),
		})
	}

	return response
}
