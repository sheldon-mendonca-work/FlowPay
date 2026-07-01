package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"flowpay/auth-service/internal/client"
	"flowpay/auth-service/internal/domain"
	"flowpay/auth-service/internal/dto"
	authErrors "flowpay/auth-service/internal/errors"
	"flowpay/auth-service/internal/jwt"
	"flowpay/auth-service/internal/password"
	"flowpay/auth-service/internal/repository"
)

const refreshTokenTTL = 7 * 24 * time.Hour

type CredentialsRepository interface {
	Create(ctx context.Context, tx *sql.Tx, creds domain.Credentials) error
	FindByEmail(ctx context.Context, email string) (*domain.Credentials, error)
}

type RefreshTokenRepository interface {
	Upsert(ctx context.Context, tx *sql.Tx, token domain.RefreshToken) error
	FindByHash(ctx context.Context, tokenHash string) (*domain.RefreshToken, error)
	DeleteByHash(ctx context.Context, tokenHash string) error
}

type DefaultCredsRepository interface {
	ExistsByAccountID(ctx context.Context, accountID string) (bool, error)
	ExistsByAccountIDAsAccount(ctx context.Context, accountID string) (bool, error)
	ExistsByAccountIDAsUser(ctx context.Context, accountID string) (bool, error)
}

type AuthService struct {
	db               *sql.DB
	credentialsRepo  CredentialsRepository
	refreshTokenRepo RefreshTokenRepository
	defaultCredsRepo DefaultCredsRepository
	accountClient    *client.AccountServiceClient
	jwtManager       *jwt.Manager
}

func NewAuthService(
	db *sql.DB,
	credentialsRepo *repository.CredentialsRepository,
	refreshTokenRepo *repository.RefreshTokenRepository,
	defaultCredsRepo *repository.DefaultCredentialsRepository,
	accountClient *client.AccountServiceClient,
	jwtManager *jwt.Manager,
) *AuthService {
	return &AuthService{
		db:               db,
		credentialsRepo:  credentialsRepo,
		refreshTokenRepo: refreshTokenRepo,
		defaultCredsRepo: defaultCredsRepo,
		accountClient:    accountClient,
		jwtManager:       jwtManager,
	}
}

func (s *AuthService) Register(ctx context.Context, req dto.RegisterRequest) (*dto.AuthResponse, error) {
	accountResp, err := s.accountClient.CreateAccount(ctx, client.CreateAccountRequest{
		AccountName:   req.AccountName,
		PaymentHandle: req.PaymentHandle,
		AccountType:   req.AccountType,
		Currency:      req.Currency,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", authErrors.ErrAccountCreationFailed, err)
	}

	passwordHash, err := password.Hash(req.Password)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	err = s.credentialsRepo.Create(ctx, tx, domain.Credentials{
		AccountID:    accountResp.AccountID,
		Email:        req.Email,
		PasswordHash: passwordHash,
	})
	if err != nil {
		return nil, authErrors.ErrEmailAlreadyExists
	}

	rawToken, tokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        tokenID,
		AccountID: accountResp.AccountID,
		TokenHash: repository.HashRefreshToken(rawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(accountResp.AccountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
	}, nil
}

func (s *AuthService) Login(ctx context.Context, req dto.LoginRequest) (*dto.AuthResponse, error) {
	creds, err := s.credentialsRepo.FindByEmail(ctx, req.Email)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return nil, authErrors.ErrInvalidCredentials
	}

	if err := password.Check(req.Password, creds.PasswordHash); err != nil {
		return nil, authErrors.ErrInvalidCredentials
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rawToken, tokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        tokenID,
		AccountID: creds.AccountID,
		TokenHash: repository.HashRefreshToken(rawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(creds.AccountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
	}, nil
}

func (s *AuthService) Refresh(ctx context.Context, rawToken string) (*dto.AuthResponse, error) {
	tokenHash := repository.HashRefreshToken(rawToken)

	stored, err := s.refreshTokenRepo.FindByHash(ctx, tokenHash)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, authErrors.ErrInvalidRefreshToken
	}
	if time.Now().After(stored.ExpiresAt) {
		return nil, authErrors.ErrExpiredRefreshToken
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	newRawToken, newTokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        newTokenID,
		AccountID: stored.AccountID,
		TokenHash: repository.HashRefreshToken(newRawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(stored.AccountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: newRawToken,
	}, nil
}

func (s *AuthService) DefaultLogin(ctx context.Context, accountID string) (*dto.AuthResponse, error) {
	exists, err := s.defaultCredsRepo.ExistsByAccountID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, authErrors.ErrAuthenticationRequired
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rawToken, tokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        tokenID,
		AccountID: accountID,
		TokenHash: repository.HashRefreshToken(rawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(accountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
	}, nil
}

func (s *AuthService) DefaultLoginAccount(ctx context.Context, accountID, accountType string) (*dto.AuthResponse, error) {
	exists, err := s.defaultCredsRepo.ExistsByAccountIDAsAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, authErrors.ErrAuthenticationRequired
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rawToken, tokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        tokenID,
		AccountID: accountID,
		TokenHash: repository.HashRefreshToken(rawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(accountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
	}, nil
}

func (s *AuthService) DefaultLoginUser(ctx context.Context, accountID, accountType string) (*dto.AuthResponse, error) {
	exists, err := s.defaultCredsRepo.ExistsByAccountIDAsUser(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, authErrors.ErrAuthenticationRequired
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rawToken, tokenID, err := generateRefreshToken()
	if err != nil {
		return nil, err
	}

	err = s.refreshTokenRepo.Upsert(ctx, tx, domain.RefreshToken{
		ID:        tokenID,
		AccountID: accountID,
		TokenHash: repository.HashRefreshToken(rawToken),
		ExpiresAt: time.Now().Add(refreshTokenTTL),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(accountID)
	if err != nil {
		return nil, err
	}

	return &dto.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: rawToken,
	}, nil
}

func (s *AuthService) Logout(ctx context.Context, rawToken string) error {
	tokenHash := repository.HashRefreshToken(rawToken)
	return s.refreshTokenRepo.DeleteByHash(ctx, tokenHash)
}

func generateRefreshToken() (rawToken string, id string, err error) {
	tokenBytes := make([]byte, 32)
	if _, err = rand.Read(tokenBytes); err != nil {
		return "", "", err
	}
	rawToken = hex.EncodeToString(tokenBytes)

	idBytes := make([]byte, 16)
	if _, err = rand.Read(idBytes); err != nil {
		return "", "", err
	}
	id = fmt.Sprintf("%x-%x-%x-%x-%x",
		idBytes[0:4], idBytes[4:6], idBytes[6:8], idBytes[8:10], idBytes[10:16])

	return rawToken, id, nil
}
