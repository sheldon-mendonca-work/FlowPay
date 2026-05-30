package repository

import "database/sql"

type OutboxEventsRepository struct {
	db *sql.DB
}

func NewOutboxEventsRepository(db *sql.DB) *OutboxEventsRepository {
	return &OutboxEventsRepository{db: db}
}
