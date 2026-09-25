package leader

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Elector holds a dedicated Postgres session and attempts a session-level
// advisory lock. Exactly one replica can hold the lock at a time; Postgres
// releases it the instant this connection closes.
type Elector struct {
	lockKey       int64
	databaseURL   string
	retryInterval time.Duration
	log           *slog.Logger

	conn     *pgx.Conn
	isLeader atomic.Bool
}

func New(databaseURL string, lockKey int64, retryInterval time.Duration, log *slog.Logger) *Elector {
	if log == nil {
		log = slog.Default()
	}
	return &Elector{
		lockKey:       lockKey,
		databaseURL:   databaseURL,
		retryInterval: retryInterval,
		log:           log,
	}
}

func (e *Elector) IsLeader() bool {
	return e.isLeader.Load()
}

// Run repeatedly tries to acquire the lock. When acquired, it calls onBecomeLeader
// and monitors the lock session; if the connection dies, leadership ends and
// standbys can take over immediately (Postgres releases the lock on disconnect).
func (e *Elector) Run(ctx context.Context, onBecomeLeader func(ctx context.Context) error) error {
	for {
		if ctx.Err() != nil {
			e.closeConn()
			return ctx.Err()
		}

		acquired, err := e.tryAcquire(ctx)
		if err != nil {
			e.log.Warn("leader acquire failed", "err", err)
			e.closeConn()
			if !sleep(ctx, e.retryInterval) {
				return ctx.Err()
			}
			continue
		}
		if !acquired {
			e.isLeader.Store(false)
			if !sleep(ctx, e.retryInterval) {
				e.closeConn()
				return ctx.Err()
			}
			continue
		}

		e.isLeader.Store(true)
		e.log.Info("became leader", "lock_key", e.lockKey)

		leaderCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go e.watchLockSession(leaderCtx, cancel, done)

		runErr := onBecomeLeader(leaderCtx)
		cancel()
		<-done

		e.isLeader.Store(false)
		e.log.Info("lost leadership", "err", runErr)
		e.closeConn()

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !sleep(ctx, e.retryInterval) {
			return ctx.Err()
		}
	}
}

func (e *Elector) watchLockSession(ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(e.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.conn == nil || e.conn.IsClosed() {
				cancel()
				return
			}
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			err := e.conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				e.log.Warn("lock session unhealthy", "err", err)
				cancel()
				return
			}
		}
	}
}

func (e *Elector) tryAcquire(ctx context.Context) (bool, error) {
	if e.conn == nil || e.conn.IsClosed() {
		conn, err := pgx.Connect(ctx, e.databaseURL)
		if err != nil {
			return false, fmt.Errorf("connect lock session: %w", err)
		}
		e.conn = conn
	}

	var acquired bool
	err := e.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", e.lockKey).Scan(&acquired)
	if err != nil {
		e.closeConn()
		return false, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	return acquired, nil
}

func (e *Elector) closeConn() {
	if e.conn == nil {
		return
	}
	// Closing the connection releases the session advisory lock automatically.
	_ = e.conn.Close(context.Background())
	e.conn = nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
