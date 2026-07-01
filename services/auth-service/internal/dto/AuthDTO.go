package dto

type RegisterRequest struct {
	Email         string `json:"email"`
	Password      string `json:"password"`
	AccountName   string `json:"account_name"`
	PaymentHandle string `json:"payment_handle"`
	AccountType   string `json:"account_type"`
	Currency      string `json:"currency"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type LogoutRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type AuthResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type DefaultLoginRequest struct {
	AccountID   string `json:"account_id"`
	AccountType string `json:"account_type"`
}
