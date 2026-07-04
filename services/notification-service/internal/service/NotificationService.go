package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"flowpay/notification-service/internal/domain"
	"flowpay/notification-service/internal/dto"
	flowpayNotificationErrors "flowpay/notification-service/internal/errors"
	"flowpay/notification-service/internal/types"
	"fmt"
	"sync"
	"time"
)

type NotificationRepository interface {
	InsertTimelineStep(ctx context.Context, step domain.PaymentTimelineStep) (bool, error)
	GetTimelineStepsByPaymentID(ctx context.Context, paymentID string) ([]domain.PaymentTimelineStep, error)
}

type NotificationService struct {
	db                     *sql.DB
	notificationRepository NotificationRepository

	mu          sync.Mutex
	subscribers map[string]map[chan dto.PaymentTimelineDTO]struct{}
}

func NewNotificationService(db *sql.DB,
	notificationRepository NotificationRepository,
) *NotificationService {
	return &NotificationService{
		db:                     db,
		notificationRepository: notificationRepository,
		subscribers:            make(map[string]map[chan dto.PaymentTimelineDTO]struct{}),
	}
}

// IngestTimelineEvent persists a timeline step. Duplicate (payment_id, step_name, status)
// combinations are silently ignored and do not trigger a broadcast.
func (s *NotificationService) IngestTimelineEvent(ctx context.Context, event domain.PaymentTimelineEvent) error {
	if event.PaymentID == "" {
		return flowpayNotificationErrors.ErrPaymentIDRequired
	}
	if event.StepName == "" {
		return flowpayNotificationErrors.ErrStepNameRequired
	}
	if event.Status == "" {
		return flowpayNotificationErrors.ErrStatusRequired
	}

	id, err := generateRandomId()
	if err != nil {
		return err
	}

	completedTime := event.OccurredAt
	if completedTime.IsZero() {
		completedTime = time.Now().UTC()
	}

	inserted, err := s.notificationRepository.InsertTimelineStep(ctx, domain.PaymentTimelineStep{
		ID:            id,
		PaymentID:     event.PaymentID,
		StepName:      event.StepName,
		Status:        event.Status,
		TraceID:       event.TraceID,
		RequestID:     event.RequestID,
		CompletedTime: completedTime,
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}

	timeline, err := s.GetTimeline(ctx, event.PaymentID)
	if err != nil {
		return err
	}

	s.publish(event.PaymentID, timeline)
	return nil
}

func (s *NotificationService) GetTimeline(ctx context.Context, paymentID string) (dto.PaymentTimelineDTO, error) {
	if paymentID == "" {
		return dto.PaymentTimelineDTO{}, flowpayNotificationErrors.ErrPaymentIDRequired
	}

	steps, err := s.notificationRepository.GetTimelineStepsByPaymentID(ctx, paymentID)
	if err != nil {
		return dto.PaymentTimelineDTO{}, err
	}

	timelineSteps := make([]types.PaymentTimelineType, 0, len(steps))
	status := types.CREATED
	for _, step := range steps {
		timelineSteps = append(timelineSteps, types.PaymentTimelineType{
			StepName:      step.StepName,
			Status:        types.NotificationStatusEnum(step.Status),
			CompletedTime: step.CompletedTime,
		})
		status = types.NotificationStatusEnum(step.Status)
	}

	return dto.PaymentTimelineDTO{
		PaymentID:     paymentID,
		Status:        status,
		TimelineSteps: timelineSteps,
	}, nil
}

// Subscribe registers the caller for timeline updates for a payment_id. The returned
// unsubscribe func must be called exactly once to release resources.
func (s *NotificationService) Subscribe(paymentID string) (<-chan dto.PaymentTimelineDTO, func()) {
	ch := make(chan dto.PaymentTimelineDTO, 1)

	s.mu.Lock()
	if s.subscribers[paymentID] == nil {
		s.subscribers[paymentID] = make(map[chan dto.PaymentTimelineDTO]struct{})
	}
	s.subscribers[paymentID][ch] = struct{}{}
	s.mu.Unlock()

	unsubscribe := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subscribers[paymentID], ch)
		if len(s.subscribers[paymentID]) == 0 {
			delete(s.subscribers, paymentID)
		}
		close(ch)
	}

	return ch, unsubscribe
}

// publish sends the latest timeline snapshot to every subscriber of paymentID. It never
// blocks: since each subscriber channel is only ever written to while holding s.mu, a full
// channel is drained of its stale snapshot before the new one is queued.
func (s *NotificationService) publish(paymentID string, timeline dto.PaymentTimelineDTO) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for ch := range s.subscribers[paymentID] {
		select {
		case ch <- timeline:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- timeline:
			default:
			}
		}
	}
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
