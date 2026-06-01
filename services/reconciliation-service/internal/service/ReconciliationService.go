package service

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/checks/idempotency"
	"flowpay/reconciliation-service/internal/checks/outbox"
	"flowpay/reconciliation-service/internal/checks/payment"
	transactioncheck "flowpay/reconciliation-service/internal/checks/transaction"
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

func (s *ReconciliationService) GetIdempotencyCompletedWithoutPayment(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	idempotencies, err := s.idempotencyRepository.GetIdempotencyCompletedWithoutPayment(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildCompletedWithoutPaymentAnomalies(idempotencies)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyMissingPaymentID(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	idempotencies, err := s.idempotencyRepository.GetIdempotencyMissingPaymentID(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildMissingPaymentIDAnomalies(idempotencies)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyDuplicatePaymentID(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	duplicates, err := s.idempotencyRepository.GetIdempotencyDuplicatePaymentID(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildDuplicatePaymentIDAnomalies(duplicates)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyExpiredInProgress(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	expiredRows, err := s.idempotencyRepository.GetIdempotencyExpiredInProgress(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildExpiredInProgressAnomalies(expiredRows)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyInProgressWithoutOutbox(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	idempotencies, err := s.idempotencyRepository.GetIdempotencyInProgressWithoutOutbox(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildInProgressWithoutOutboxAnomalies(idempotencies)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyOutboxPaymentIDMismatch(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	mismatches, err := s.idempotencyRepository.GetIdempotencyOutboxPaymentIDMismatch(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildOutboxPaymentIDMismatchAnomalies(mismatches)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetIdempotencyMultipleOutboxPaymentIDs(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := s.idempotencyRepository.GetIdempotencyMultipleOutboxPaymentIDs(ctx, tx)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := idempotency.BuildMultipleOutboxPaymentIDsAnomalies(rows)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) RunIdempotencyChecks(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	return s.runChecks(ctx, checkType,
		s.GetIdempotencyCompletedWithoutPayment,
		s.GetIdempotencyMissingPaymentID,
		s.GetIdempotencyDuplicatePaymentID,
		s.GetIdempotencyExpiredInProgress,
		s.GetIdempotencyInProgressWithoutOutbox,
		s.GetIdempotencyOutboxPaymentIDMismatch,
		s.GetIdempotencyMultipleOutboxPaymentIDs,
	)
}

func (s *ReconciliationService) GetOutboxStuckProcessing(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	events, err := s.outboxRepository.GetOutboxStuckProcessing(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildStuckProcessingAnomalies(events)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetOutboxFailedRetryExhausted(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	events, err := s.outboxRepository.GetOutboxFailedRetryExhausted(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildFailedRetryExhaustedAnomalies(events)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetOutboxEligibleBacklog(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	events, err := s.outboxRepository.GetOutboxEligibleBacklog(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildEligibleBacklogAnomalies(events)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetOutboxBacklogSummary(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	summaries, err := s.outboxRepository.GetOutboxBacklogSummary(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildBacklogSummaryAnomalies(summaries)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetOutboxWithoutIdempotencyRows(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	events, err := s.outboxRepository.GetOutboxWithoutIdempotencyRows(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildWithoutIdempotencyRowsAnomalies(events)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetOutboxPublishedWithoutPayment(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	events, err := s.outboxRepository.GetOutboxPublishedWithoutPayment(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := outbox.BuildPublishedWithoutPaymentAnomalies(events)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) RunOutboxChecks(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	return s.runChecks(ctx, checkType,
		s.GetOutboxStuckProcessing,
		s.GetOutboxFailedRetryExhausted,
		s.GetOutboxEligibleBacklog,
		s.GetOutboxBacklogSummary,
		s.GetOutboxWithoutIdempotencyRows,
		s.GetOutboxPublishedWithoutPayment,
	)
}

func (s *ReconciliationService) GetTransactionsMissingLedgerSide(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	ledgers, err := s.transactionRepository.GetTransactionsMissingLedgerSide(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := transactioncheck.BuildMissingLedgerSideAnomalies(ledgers)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetTransactionsAmountCurrencyImbalance(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	imbalances, err := s.transactionRepository.GetTransactionsAmountCurrencyImbalance(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := transactioncheck.BuildAmountCurrencyImbalanceAnomalies(imbalances)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetTransactionsWithoutPayments(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	transactions, err := s.transactionRepository.GetTransactionsWithoutPayments(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := transactioncheck.BuildWithoutPaymentsAnomalies(transactions)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) GetTransactionsStatusMismatch(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	mismatches, err := s.transactionRepository.GetTransactionsStatusMismatch(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	anomalies := transactioncheck.BuildStatusMismatchAnomalies(mismatches)
	response = toAnomalyResponseDTOs(response, anomalies)
	return response, nil
}

func (s *ReconciliationService) RunTransactionChecks(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	return s.runChecks(ctx, checkType,
		s.GetTransactionsMissingLedgerSide,
		s.GetTransactionsAmountCurrencyImbalance,
		s.GetTransactionsWithoutPayments,
		s.GetTransactionsStatusMismatch,
	)
}

func (s *ReconciliationService) RunAllChecks(ctx context.Context, checkType string) ([]dto.AnomalyResponseDTO, error) {
	return s.runChecks(ctx, checkType,
		s.RunPaymentChecks,
		s.RunIdempotencyChecks,
		s.RunOutboxChecks,
		s.RunTransactionChecks,
	)
}

func (s *ReconciliationService) runChecks(
	ctx context.Context,
	checkType string,
	checks ...func(context.Context, string) ([]dto.AnomalyResponseDTO, error),
) ([]dto.AnomalyResponseDTO, error) {
	response := make([]dto.AnomalyResponseDTO, 0)

	for _, check := range checks {
		anomalies, err := check(ctx, checkType)
		if err != nil {
			return nil, err
		}
		response = append(response, anomalies...)
	}

	return response, nil
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
