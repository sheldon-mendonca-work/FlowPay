package dto

type DefaultAccountItem struct {
	AccountID   string  `json:"account_id"`
	AccountName string  `json:"account_name"`
	Currency    string  `json:"currency"`
	AccountType string  `json:"account_type"`
	CompanyName *string `json:"company_name,omitempty"`
	DisplayName string  `json:"display_name"`
	Description string  `json:"description,omitempty"`
}

type DefaultUserItem struct {
	UserID      string  `json:"user_id"`
	AccountID   string  `json:"account_id"`
	CompanyID   *string `json:"company_id,omitempty"`
	Role        string  `json:"role"`
	DisplayName string  `json:"display_name"`
	Description string  `json:"description,omitempty"`
}

type DefaultCompanyItem struct {
	CompanyID    string `json:"company_id"`
	Name         string `json:"name"`
	BusinessName string `json:"business_name"`
	AccountID    string `json:"account_id"`
	DisplayName  string `json:"display_name"`
	Description  string `json:"description,omitempty"`
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
	AccountID   string `json:"account_id"`
	AccountName string `json:"account_name"`
	AccountType string `json:"account_type"`
	DisplayName string `json:"display_name"`
	Description string `json:"description,omitempty"`
}

type DefaultListRequest struct {
	Type string `json:"type"`
}

type DefaultListResponse struct {
	Type     string                     `json:"type"`
	Users    []DefaultUserItem          `json:"users,omitempty"`
	Accounts []DefaultSystemAccountItem `json:"accounts,omitempty"`
}
