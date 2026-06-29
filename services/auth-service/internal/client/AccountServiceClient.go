package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"flowpay/pkg/observability/tracing"
)

type CreateAccountRequest struct {
	AccountName string `json:"account_name"`
	AccountType string `json:"account_type"`
	Currency    string `json:"currency"`
}

type CreateAccountResponse struct {
	AccountID string `json:"account_id"`
}

type AccountServiceClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewAccountServiceClient(baseURL string) *AccountServiceClient {
	return &AccountServiceClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *AccountServiceClient) CreateAccount(ctx context.Context, req CreateAccountRequest) (*CreateAccountResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/accounts", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if traceID := tracing.GetTraceID(ctx); traceID != "" {
		httpReq.Header.Set("X-Trace-Id", traceID)
	}
	if requestID := tracing.GetRequestID(ctx); requestID != "" {
		httpReq.Header.Set("X-Request-Id", requestID)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("account service returned %d", resp.StatusCode)
	}

	var result CreateAccountResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
