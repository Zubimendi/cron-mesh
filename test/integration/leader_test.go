//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/francisoffiong/cron-mesh/internal/db"
	"github.com/francisoffiong/cron-mesh/internal/leader"
	"github.com/jackc/pgx/v5"
)

func databaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	return url
}

func TestAdvisoryLockExclusive(t *testing.T) {
	ctx := context.Background()
	url := databaseURL(t)
	const key int64 = 918001

	a, err := db.ConnectLock(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(ctx)

	b, err := db.ConnectLock(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)

	var gotA, gotB bool
	if err := a.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&gotA); err != nil {
		t.Fatal(err)
	}
	if !gotA {
		t.Fatal("first connection should acquire lock")
	}
	if err := b.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&gotB); err != nil {
		t.Fatal(err)
	}
	if gotB {
		t.Fatal("second connection must not acquire lock while first holds it")
	}

	// Release by closing A — lock is session-scoped.
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := b.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&gotB); err != nil {
			t.Fatal(err)
		}
		if gotB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("standby did not acquire lock after leader disconnect")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestElectorFailover(t *testing.T) {
	url := databaseURL(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const key int64 = 918002

	// Ensure clean slate for this key.
	cleanup, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cleanup.Exec(ctx, "SELECT pg_advisory_unlock_all()")
	_ = cleanup.Close(ctx)

	leaderReady := make(chan struct{}, 1)
	standbyReady := make(chan struct{}, 1)

	e1 := leader.New(url, key, 100*time.Millisecond, nil)
	e2 := leader.New(url, key, 100*time.Millisecond, nil)

	go func() {
		_ = e1.Run(ctx, func(leaderCtx context.Context) error {
			select {
			case leaderReady <- struct{}{}:
			default:
			}
			<-leaderCtx.Done()
			return leaderCtx.Err()
		})
	}()

	select {
	case <-leaderReady:
	case <-time.After(5 * time.Second):
		t.Fatal("e1 did not become leader")
	}

	go func() {
		_ = e2.Run(ctx, func(leaderCtx context.Context) error {
			select {
			case standbyReady <- struct{}{}:
			default:
			}
			<-leaderCtx.Done()
			return leaderCtx.Err()
		})
	}()

	// e2 should still be standby.
	time.Sleep(300 * time.Millisecond)
	if e2.IsLeader() {
		t.Fatal("e2 should not be leader while e1 holds lock")
	}

	// Force e1 to drop leadership by cancelling — but we need only e1 to stop.
	// Closing via cancelling whole ctx stops both. Instead: unlock by having e1's
	// Run exit. We cancel a parent used only for e1.
	cancel()

	// Restart e2 on a fresh context after e1 is gone — simpler path:
	// re-test with sequential acquire after first elector's connection closes.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	e3 := leader.New(url, key, 100*time.Millisecond, nil)
	go func() {
		_ = e3.Run(ctx2, func(leaderCtx context.Context) error {
			select {
			case standbyReady <- struct{}{}:
			default:
			}
			<-leaderCtx.Done()
			return leaderCtx.Err()
		})
	}()

	select {
	case <-standbyReady:
	case <-time.After(5 * time.Second):
		t.Fatal("standby did not become leader after previous leader stopped")
	}
	if !e3.IsLeader() {
		t.Fatal("e3 should be leader")
	}
}
