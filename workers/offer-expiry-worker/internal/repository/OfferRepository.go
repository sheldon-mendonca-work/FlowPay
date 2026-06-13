package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type OfferRepository struct {
	db *sql.DB
}

func NewOfferRepository(db *sql.DB) *OfferRepository {
	return &OfferRepository{
		db: db,
	}
}

func (r *OfferRepository) DecrementReservedCount(
	ctx context.Context,
	tx *sql.Tx,
	offerID string,
) error {
	query := `
		UPDATE offers
		SET
			reserved_count = reserved_count-1,
			updated_at = NOW()
		WHERE id = $1
		  AND reserved_count > 0;
	`

	res, err := tx.ExecContext(ctx, query, offerID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("offer deduction failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}
