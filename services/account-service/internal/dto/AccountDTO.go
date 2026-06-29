package dto

type CreateAccountRequest struct {
	AccountName string `json:"account_name"`
	AccountType string `json:"account_type"`
	Currency    string `json:"currency"`
}

type CreateAccountResponse struct {
	AccountID string `json:"account_id"`
}

type CreateCompanyRequest struct {
	Name         string `json:"name"`
	BusinessName string `json:"business_name"`
	EmailID      string `json:"email_id"`
	PhoneNumber  string `json:"phone_number"`
}

type CreateCompanyResponse struct {
	CompanyID string `json:"company_id"`
}

// CreateUserRequest supports auto-creation of account and company when IDs are not supplied.
// If account_id is omitted, account_name + currency are required.
// If company_id and company_name are both omitted, the user is created with no company.
// If company_name is provided but company_id is omitted, a company is auto-created.
type CreateUserRequest struct {
	// Account resolution
	AccountID   string `json:"account_id"`
	AccountName string `json:"account_name"`
	Currency    string `json:"currency"`
	AccountType string `json:"account_type"`

	// Company resolution
	CompanyID           string `json:"company_id"`
	CompanyName         string `json:"company_name"`
	CompanyBusinessName string `json:"company_business_name"`
	CompanyEmailID      string `json:"company_email_id"`
	CompanyPhoneNumber  string `json:"company_phone_number"`

	// User fields
	Role string `json:"role"`
}

type CreateUserResponse struct {
	UserID    string  `json:"user_id"`
	AccountID string  `json:"account_id"`
	CompanyID *string `json:"company_id,omitempty"`
}
