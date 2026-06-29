package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"strings"

	"flowpay/account-service/internal/domain"
	"flowpay/account-service/internal/dto"
	accountErrors "flowpay/account-service/internal/errors"
	"flowpay/account-service/internal/repository"
)

type AccountService struct {
	db          *sql.DB
	companyRepo *repository.CompanyRepository
	userRepo    *repository.UserRepository
	accountRepo *repository.AccountsRepository
}

func NewAccountService(
	db *sql.DB,
	companyRepo *repository.CompanyRepository,
	userRepo *repository.UserRepository,
	accountRepo *repository.AccountsRepository,
) *AccountService {
	return &AccountService{
		db:          db,
		companyRepo: companyRepo,
		userRepo:    userRepo,
		accountRepo: accountRepo,
	}
}

func (s *AccountService) CreateAccount(ctx context.Context, req dto.CreateAccountRequest) (*dto.CreateAccountResponse, error) {
	if err := validateCreateAccountRequest(req); err != nil {
		return nil, err
	}

	accountType := req.AccountType
	if accountType == "" {
		accountType = "USER"
	}

	id, err := generateID()
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := s.accountRepo.Create(ctx, tx, domain.Account{
		ID:          id,
		AccountName: req.AccountName,
		AccountType: accountType,
		Balance:     0,
		Currency:    req.Currency,
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &dto.CreateAccountResponse{AccountID: id}, nil
}

func (s *AccountService) CreateCompany(ctx context.Context, req dto.CreateCompanyRequest) (*dto.CreateCompanyResponse, error) {
	if err := validateCreateCompanyRequest(req); err != nil {
		return nil, err
	}

	id, err := generateID()
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := s.companyRepo.Create(ctx, tx, domain.Company{
		ID:           id,
		Name:         req.Name,
		BusinessName: req.BusinessName,
		EmailID:      req.EmailID,
		PhoneNumber:  req.PhoneNumber,
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &dto.CreateCompanyResponse{CompanyID: id}, nil
}

// CreateUser creates a user. If account_id is not supplied, an account is auto-created (requires
// account_name + currency). If company_id is not supplied but company_name is, a company is
// auto-created. All writes happen in a single transaction.
func (s *AccountService) CreateUser(ctx context.Context, req dto.CreateUserRequest) (*dto.CreateUserResponse, error) {
	if err := validateCreateUserRequest(req); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Resolve account
	accountID := strings.TrimSpace(req.AccountID)
	if accountID != "" {
		acc, err := s.accountRepo.FindByID(ctx, accountID)
		if err != nil {
			return nil, err
		}
		if acc == nil {
			return nil, accountErrors.ErrAccountNotFound
		}
	} else {
		id, err := generateID()
		if err != nil {
			return nil, err
		}
		accountType := req.AccountType
		if accountType == "" {
			accountType = "USER"
		}
		if err := s.accountRepo.Create(ctx, tx, domain.Account{
			ID:          id,
			AccountName: req.AccountName,
			AccountType: accountType,
			Balance:     0,
			Currency:    req.Currency,
		}); err != nil {
			return nil, err
		}
		accountID = id
	}

	// Resolve company (optional)
	companyID := strings.TrimSpace(req.CompanyID)
	if companyID != "" {
		comp, err := s.companyRepo.FindByID(ctx, companyID)
		if err != nil {
			return nil, err
		}
		if comp == nil {
			return nil, accountErrors.ErrCompanyNotFound
		}
	} else if strings.TrimSpace(req.CompanyName) != "" {
		id, err := generateID()
		if err != nil {
			return nil, err
		}
		if err := s.companyRepo.Create(ctx, tx, domain.Company{
			ID:           id,
			Name:         req.CompanyName,
			BusinessName: req.CompanyBusinessName,
			EmailID:      req.CompanyEmailID,
			PhoneNumber:  req.CompanyPhoneNumber,
		}); err != nil {
			return nil, err
		}
		companyID = id
	}

	userID, err := generateID()
	if err != nil {
		return nil, err
	}

	if err := s.userRepo.Create(ctx, tx, domain.User{
		ID:        userID,
		AccountID: accountID,
		CompanyID: companyID,
		Role:      req.Role,
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := &dto.CreateUserResponse{
		UserID:    userID,
		AccountID: accountID,
	}
	if companyID != "" {
		c := companyID
		resp.CompanyID = &c
	}
	return resp, nil
}

func validateCreateAccountRequest(req dto.CreateAccountRequest) error {
	if strings.TrimSpace(req.AccountName) == "" {
		return accountErrors.ErrAccountNameRequired
	}
	if strings.TrimSpace(req.Currency) == "" {
		return accountErrors.ErrCurrencyRequired
	}
	if req.AccountType != "" {
		switch req.AccountType {
		case "USER", "SYSTEM", "PROMOTION_POOL":
		default:
			return accountErrors.ErrInvalidAccountType
		}
	}
	return nil
}

func validateCreateCompanyRequest(req dto.CreateCompanyRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return accountErrors.ErrCompanyNameRequired
	}
	if strings.TrimSpace(req.BusinessName) == "" {
		return accountErrors.ErrBusinessNameRequired
	}
	return nil
}

func validateCreateUserRequest(req dto.CreateUserRequest) error {
	if strings.TrimSpace(req.Role) == "" {
		return accountErrors.ErrRoleRequired
	}
	switch req.Role {
	case "ADMIN", "USER":
	default:
		return accountErrors.ErrInvalidRole
	}
	if strings.TrimSpace(req.AccountID) == "" {
		if strings.TrimSpace(req.AccountName) == "" {
			return accountErrors.ErrAccountNameRequired
		}
		if strings.TrimSpace(req.Currency) == "" {
			return accountErrors.ErrCurrencyRequired
		}
	}
	if strings.TrimSpace(req.CompanyName) != "" && strings.TrimSpace(req.CompanyBusinessName) == "" {
		return accountErrors.ErrBusinessNameRequired
	}
	return nil
}

func generateID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
