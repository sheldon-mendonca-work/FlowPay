package jwt

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	accessTokenTTL  = 15 * time.Minute
	TokenTypeAccess = "access"
)

type AccessClaims struct {
	Type string `json:"type"`
	jwt.RegisteredClaims
}

type Manager struct {
	secret string
}

func NewManager(secret string) *Manager {
	return &Manager{secret: secret}
}

func (m *Manager) GenerateAccessToken(accountID string) (string, error) {
	claims := AccessClaims{
		Type: TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   accountID,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(accessTokenTTL)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(m.secret))
}
