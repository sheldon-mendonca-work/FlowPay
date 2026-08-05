package service

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"flowpay/account-service/internal/constants"
	"flowpay/account-service/internal/domain"
	"flowpay/account-service/internal/dto"
	accountErrors "flowpay/account-service/internal/errors"
	"flowpay/account-service/internal/repository"
	"flowpay/pkg/observability/metrics"

	"github.com/redis/go-redis/v9"
)

type AccountService struct {
	db               *sql.DB
	redisClient      *redis.Client
	companyRepo      *repository.CompanyRepository
	userRepo         *repository.UserRepository
	accountRepo      *repository.AccountsRepository
	defaultCredsRepo *repository.DefaultCredentialsRepository
	transactionsRepo *repository.TransactionsRepository
}

func NewAccountService(
	db *sql.DB,
	redisClient *redis.Client,
	companyRepo *repository.CompanyRepository,
	userRepo *repository.UserRepository,
	accountRepo *repository.AccountsRepository,
	defaultCredsRepo *repository.DefaultCredentialsRepository,
	transactionsRepo *repository.TransactionsRepository,
) *AccountService {
	return &AccountService{
		db:               db,
		redisClient:      redisClient,
		companyRepo:      companyRepo,
		userRepo:         userRepo,
		accountRepo:      accountRepo,
		defaultCredsRepo: defaultCredsRepo,
		transactionsRepo: transactionsRepo,
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
		ID:            id,
		AccountName:   req.AccountName,
		PaymentHandle: req.PaymentHandle,
		AccountType:   accountType,
		Balance:       0,
		Currency:      req.Currency,
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
			ID:            id,
			AccountName:   req.AccountName,
			PaymentHandle: req.PaymentHandle,
			AccountType:   accountType,
			Balance:       0,
			Currency:      req.Currency,
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
	if strings.TrimSpace(req.PaymentHandle) == "" {
		return accountErrors.ErrPaymentHandleRequired
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
		if strings.TrimSpace(req.PaymentHandle) == "" {
			return accountErrors.ErrPaymentHandleRequired
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

func (s *AccountService) GetUserInfo(ctx context.Context, accountID string) (*dto.UserInfoResponse, error) {
	key := fmt.Sprintf("account:userinfo:%s", accountID)

	cached, err := s.redisClient.Get(ctx, key).Result()
	if err == nil {
		var resp dto.UserInfoResponse
		if json.Unmarshal([]byte(cached), &resp) == nil {
			metrics.AccountGetUserInfoRedisCount.WithLabelValues(constants.ServiceName, "success").Inc()
			return &resp, nil
		}
	}

	account, err := s.accountRepo.FindByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, accountErrors.ErrAccountNotFound
	}

	resp := &dto.UserInfoResponse{
		AccountID:            account.ID,
		AccountName:          account.AccountName,
		PaymentHandle:        account.PaymentHandle,
		AccountType:          account.AccountType,
		Balance:              account.Balance,
		Currency:             account.Currency,
		AllowNegativeBalance: account.AllowNegativeBalance,
	}

	user, err := s.userRepo.FindByAccountID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if user != nil {
		resp.UserID = &user.ID
		resp.Role = &user.Role

		if user.CompanyID != "" {
			company, err := s.companyRepo.FindByID(ctx, user.CompanyID)
			if err != nil {
				return nil, err
			}
			if company != nil {
				resp.CompanyID = &company.ID
				resp.CompanyName = &company.Name
				resp.CompanyBusinessName = &company.BusinessName
			}
		}
	}

	if bytes, err := json.Marshal(resp); err == nil {
		_ = s.redisClient.Set(ctx, key, bytes, 5*time.Minute).Err()
	}
	metrics.AccountGetUserInfoDatabaseCount.WithLabelValues(constants.ServiceName, "success").Inc()
	fmt.Println("Served from Database")
	return resp, nil
}

func (s *AccountService) GetUserByID(ctx context.Context, userID string) (*dto.UserResponse, error) {

	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, accountErrors.ErrUserNotFound
	}

	account, err := s.accountRepo.FindByID(ctx, user.AccountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, accountErrors.ErrAccountNotFound
	}

	resp := &dto.UserResponse{
		UserID:        user.ID,
		AccountID:     account.ID,
		Role:          user.Role,
		AccountName:   account.AccountName,
		PaymentHandle: account.PaymentHandle,
		AccountType:   account.AccountType,
		Balance:       account.Balance,
		Currency:      account.Currency,
	}

	if user.CompanyID != "" {
		company, err := s.companyRepo.FindByID(ctx, user.CompanyID)
		if err != nil {
			return nil, err
		}
		if company != nil {
			resp.CompanyID = &company.ID
			resp.CompanyName = &company.Name
			resp.CompanyBusinessName = &company.BusinessName
		}
	}

	return resp, nil
}

func (s *AccountService) GetUserByPaymentHandle(ctx context.Context, paymentHandle string) (*dto.UserResponse, error) {
	account, err := s.accountRepo.FindByPaymentHandle(ctx, paymentHandle)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, accountErrors.ErrUserNotFound
	}

	user, err := s.userRepo.FindByAccountID(ctx, account.ID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, accountErrors.ErrUserNotFound
	}

	resp := &dto.UserResponse{
		UserID:        user.ID,
		AccountID:     account.ID,
		Role:          user.Role,
		AccountName:   account.AccountName,
		PaymentHandle: account.PaymentHandle,
		AccountType:   account.AccountType,
		Balance:       account.Balance,
		Currency:      account.Currency,
	}

	if user.CompanyID != "" {
		company, err := s.companyRepo.FindByID(ctx, user.CompanyID)
		if err != nil {
			return nil, err
		}
		if company != nil {
			resp.CompanyID = &company.ID
			resp.CompanyName = &company.Name
			resp.CompanyBusinessName = &company.BusinessName
		}
	}

	return resp, nil
}

func (s *AccountService) GetDefaultList(ctx context.Context, callerAccountID, listType string) (*dto.DefaultListResponse, error) {
	resp := &dto.DefaultListResponse{Type: listType}
	switch listType {
	case "accounts":
		accounts, err := s.defaultCredsRepo.ListAccountsExcluding(ctx, callerAccountID)
		if err != nil {
			return nil, err
		}
		resp.Accounts = accounts
	case "company":
		sysAccounts, err := s.defaultCredsRepo.ListSystemAccounts(ctx)
		if err != nil {
			return nil, err
		}
		resp.SystemAccounts = sysAccounts
	default:
		return nil, accountErrors.ErrInvalidListType
	}
	return resp, nil
}

func (s *AccountService) ListUserAccounts(ctx context.Context, callerAccountID, search string, page, pageSize int) (*dto.ListAccountsResponse, error) {
	accounts, total, err := s.accountRepo.ListPaged(ctx, callerAccountID, search, page, pageSize)
	if err != nil {
		return nil, err
	}
	return &dto.ListAccountsResponse{
		Accounts: accounts,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

func (s *AccountService) GetDefaultAccounts(ctx context.Context) (*dto.ListDefaultAccountsResponse, error) {
	items, err := s.defaultCredsRepo.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	return &dto.ListDefaultAccountsResponse{Accounts: items}, nil
}

func (s *AccountService) GetDefaultUsers(ctx context.Context) (*dto.ListDefaultUsersResponse, error) {
	items, err := s.defaultCredsRepo.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	return &dto.ListDefaultUsersResponse{Users: items}, nil
}

func (s *AccountService) GetDefaultCompanies(ctx context.Context) (*dto.ListDefaultCompaniesResponse, error) {
	items, err := s.defaultCredsRepo.ListCompanies(ctx)
	if err != nil {
		return nil, err
	}
	return &dto.ListDefaultCompaniesResponse{Companies: items}, nil
}

func (s *AccountService) GetBalance(ctx context.Context, accountID string) (*dto.BalanceResponse, error) {
	account, err := s.accountRepo.FindByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, accountErrors.ErrAccountNotFound
	}
	return &dto.BalanceResponse{Balance: account.Balance, Currency: account.Currency}, nil
}

func (s *AccountService) GetTransactions(ctx context.Context, accountID string, page, pageSize int) (*dto.TransactionsListResponse, error) {
	account, err := s.accountRepo.FindByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, accountErrors.ErrAccountNotFound
	}

	transactions, total, err := s.transactionsRepo.ListPagedByAccountID(ctx, accountID, page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]dto.TransactionListItem, 0, len(transactions))
	for _, t := range transactions {
		items = append(items, dto.TransactionListItem{
			TransactionID:       t.ID,
			PaymentID:           t.PaymentID,
			Type:                t.Type,
			TransactionCategory: t.TransactionCategory,
			Amount:              t.Amount,
			Currency:            t.Currency,
			Status:              t.Status,
			CreatedAt:           t.CreatedAt.Format(time.RFC3339),
		})
	}

	return &dto.TransactionsListResponse{
		Transactions: items,
		Total:        total,
		Page:         page,
		PageSize:     pageSize,
	}, nil
}

func (s *AccountService) GetTransactionsByPaymentID(ctx context.Context, paymentID string) (*dto.PaymentTransactionsResponse, error) {
	transactions, err := s.transactionsRepo.ListByPaymentID(ctx, paymentID)
	if err != nil {
		return nil, err
	}

	items := make([]dto.PaymentTransactionItem, 0, len(transactions))
	for _, t := range transactions {
		items = append(items, dto.PaymentTransactionItem{
			TransactionID: t.ID,
			AccountID:     t.AccountID,
			Type:          t.Type,
			Amount:        t.Amount,
			Currency:      t.Currency,
			CreatedAt:     t.CreatedAt.Format(time.RFC3339),
		})
	}

	return &dto.PaymentTransactionsResponse{
		PaymentID:    paymentID,
		Transactions: items,
	}, nil
}

func generateID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
