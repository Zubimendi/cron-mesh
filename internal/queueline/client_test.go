package queueline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestClientDedupReplay(t *testing.T) {
	var mu sync.Mutex
	jobs := map[string]string{}
	var posts int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			DedupKey string `json:"dedupKey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		posts++
		if id, ok := jobs[body.DedupKey]; ok {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
			return
		}
		id := "job-" + body.DedupKey
		jobs[body.DedupKey] = id
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer srv.Close()

	c := New(srv.URL)
	ctx := context.Background()
	id1, err := c.Enqueue(ctx, "default", map[string]string{"x": "1"}, "abc:2026-09-25T14:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.Enqueue(ctx, "default", map[string]string{"x": "1"}, "abc:2026-09-25T14:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("ids differ: %s vs %s", id1, id2)
	}
	mu.Lock()
	defer mu.Unlock()
	if posts != 2 {
		t.Fatalf("posts=%d", posts)
	}
	if len(jobs) != 1 {
		t.Fatalf("logical jobs=%d", len(jobs))
	}
}
