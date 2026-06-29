package service

import (
	"database/sql"
)

type AccountsRepository interface {
}

type CredentialsRepository interface {
}

type RefreshTokenRepository interface {
}

type AuthService struct {
	db                     *sql.DB
	accountsRepository     AccountsRepository
	refreshTokenRepository RefreshTokenRepository
	credentialsRepository  CredentialsRepository
}

func NewAuthService(db *sql.DB,
	accountsRepository AccountsRepository,
	refreshTokenRepository RefreshTokenRepository,
	credentialsRepository CredentialsRepository,
) *AuthService {
	return &AuthService{
		db:                     db,
		accountsRepository:     accountsRepository,
		refreshTokenRepository: credentialsRepository,
		credentialsRepository:  credentialsRepository,
	}
}
