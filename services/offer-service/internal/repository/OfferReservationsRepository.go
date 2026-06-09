package repository

import "database/sql"

type OfferReservationsRepository struct {
	db *sql.DB
}

func NewOfferReservationsRepository(db *sql.DB) *OfferReservationsRepository {
	return &OfferReservationsRepository{
		db: db,
	}
}
