package queueline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client talks to QueueLine's enqueue API. Dedup is a body field (dedupKey), not a header.
type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

type enqueueResponse struct {
	ID string `json:"id"`
}

func (c *Client) Enqueue(ctx context.Context, queue string, payload any, dedupKey string) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	body := map[string]any{
		"payload":      json.RawMessage(raw),
		"priority":     0,
		"delaySeconds": 0,
		"maxAttempts":  3,
	}
	if dedupKey != "" {
		body["dedupKey"] = dedupKey
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("%s/v1/queues/%s/jobs", c.baseURL, queue)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("queueline enqueue: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("queueline enqueue status %d: %s", resp.StatusCode, string(respBody))
	}

	var out enqueueResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	return out.ID, nil
}
