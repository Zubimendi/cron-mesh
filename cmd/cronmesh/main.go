package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/francisoffiong/cron-mesh/internal/api"
	"github.com/francisoffiong/cron-mesh/internal/config"
	"github.com/francisoffiong/cron-mesh/internal/db"
	"github.com/francisoffiong/cron-mesh/internal/jobs"
	"github.com/francisoffiong/cron-mesh/internal/leader"
	"github.com/francisoffiong/cron-mesh/internal/queueline"
	"github.com/francisoffiong/cron-mesh/internal/scheduler"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.ConnectPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := jobs.NewStore(pool)
	ql := queueline.New(cfg.QueueLineBaseURL)
	sched := scheduler.New(store, ql, cfg.TickInterval, log)
	elector := leader.New(cfg.DatabaseURL, cfg.LeaderLockKey, cfg.LeaderRetryInterval, log)

	srv := api.New(store, elector)
	httpSrv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("http listening", "port", cfg.HTTPPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	go func() {
		err := elector.Run(ctx, func(leaderCtx context.Context) error {
			return sched.Run(leaderCtx)
		})
		if err != nil && ctx.Err() == nil {
			log.Error("leader loop", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Info("shutdown complete")
}
