package dto

type DefaultAccountItem struct {
	AccountID     string  `json:"account_id"`
	AccountName   string  `json:"account_name"`
	PaymentHandle string  `json:"payment_handle"`
	Currency      string  `json:"currency"`
	AccountType   string  `json:"account_type"`
	CompanyName   *string `json:"company_name,omitempty"`
	Description   string  `json:"description,omitempty"`
}

type DefaultUserItem struct {
	UserID        string  `json:"user_id"`
	AccountID     string  `json:"account_id"`
	PaymentHandle string  `json:"payment_handle"`
	CompanyID     *string `json:"company_id,omitempty"`
	Role          string  `json:"role"`
	Description   string  `json:"description,omitempty"`
}

type DefaultCompanyItem struct {
	CompanyID     string `json:"company_id"`
	Name          string `json:"name"`
	BusinessName  string `json:"business_name"`
	AccountID     string `json:"account_id"`
	PaymentHandle string `json:"payment_handle"`
	Description   string `json:"description,omitempty"`
}

type ListDefaultAccountsResponse struct {
	Accounts []DefaultAccountItem `json:"accounts"`
}

type ListDefaultUsersResponse struct {
	Users []DefaultUserItem `json:"users"`
}

type ListDefaultCompaniesResponse struct {
	Companies []DefaultCompanyItem `json:"companies"`
}

type DefaultSystemAccountItem struct {
	AccountID     string `json:"account_id"`
	AccountName   string `json:"account_name"`
	PaymentHandle string `json:"payment_handle"`
	AccountType   string `json:"account_type"`
	Description   string `json:"description,omitempty"`
}

type DefaultListRequest struct {
	Type string `json:"type"`
}

type DefaultListResponse struct {
	Type           string                     `json:"type"`
	Accounts       []DefaultAccountItem       `json:"accounts,omitempty"`
	SystemAccounts []DefaultSystemAccountItem `json:"system_accounts,omitempty"`
}
