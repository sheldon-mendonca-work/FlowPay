package repository

import "database/sql"

type OfferRedemptionsRepository struct {
	db *sql.DB
}

func NewOfferRedemptionsRepository(db *sql.DB) *OfferRedemptionsRepository {
	return &OfferRedemptionsRepository{
		db: db,
	}
}
