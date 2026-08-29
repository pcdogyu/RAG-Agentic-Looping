package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	baseURL   string
	http      *http.Client
	mu        sync.Mutex
	failures  int
	openUntil time.Time
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: timeout}}
}

func (c *Client) Post(ctx context.Context, path string, request, response any) error {
	if err := c.allow(); err != nil {
		return err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := c.http.Do(httpRequest)
	if err != nil {
		c.failed()
		return err
	}
	defer httpResponse.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(httpResponse.Body, 32<<20))
	if err != nil {
		c.failed()
		return err
	}
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		c.failed()
		var detail struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(payload, &detail)
		if detail.Detail == "" {
			detail.Detail = httpResponse.Status
		}
		return fmt.Errorf("adapter %s: %s", path, detail.Detail)
	}
	c.succeeded()
	if response == nil {
		return nil
	}
	return json.Unmarshal(payload, response)
}

func (c *Client) allow() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Now().Before(c.openUntil) {
		return errors.New("adapter circuit is open")
	}
	return nil
}
func (c *Client) failed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures >= 3 {
		c.openUntil = time.Now().Add(30 * time.Second)
	}
}
func (c *Client) succeeded() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.openUntil = time.Time{}
}

type EmbeddingResponse struct {
	Vectors    [][]float32 `json:"vectors"`
	Dimensions int         `json:"dimensions"`
}
type TokenCountResponse struct {
	Counts []int `json:"counts"`
}

func (c *Client) Embed(ctx context.Context, texts []string) (EmbeddingResponse, error) {
	var response EmbeddingResponse
	err := c.Post(ctx, "/v1/embed", map[string]any{"texts": texts}, &response)
	return response, err
}
func (c *Client) TokenCount(ctx context.Context, texts []string) (TokenCountResponse, error) {
	var response TokenCountResponse
	err := c.Post(ctx, "/v1/token-count", map[string]any{"texts": texts}, &response)
	return response, err
}
