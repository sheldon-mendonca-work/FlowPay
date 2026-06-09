package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

type OfferEventsRepository struct {
	db *sql.DB
}

func NewOfferEventsRepository(db *sql.DB) *OfferEventsRepository {
	return &OfferEventsRepository{
		db: db,
	}
}

func (r *OfferEventsRepository) CreateEvent(
	ctx context.Context,
	tx *sql.Tx,
	event domain.OfferEventEntity,
) error {

	query := `
		INSERT INTO offer_events (
			id,
			offer_id,
			event_type,
			actor_id,
			actor_type,
			metadata,
			created_at
		)
		VALUES (
			$1,$2,$3,$4,$5,$6,NOW()
		)
	`

	res, err := tx.ExecContext(
		ctx,
		query,
		event.ID,
		event.OfferID,
		event.EventType,
		event.ActorID,
		event.ActorType,
		event.Metadata,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Offer Event Creation failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}
