package domain

import "time"

type Company struct {
	ID           string
	Name         string
	BusinessName string
	EmailID      string
	PhoneNumber  string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
