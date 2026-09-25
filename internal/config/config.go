package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL         string
	HTTPPort            string
	QueueLineBaseURL    string
	TickInterval        time.Duration
	LeaderRetryInterval time.Duration
	LeaderLockKey       int64
}

func Load() (Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	qlURL := os.Getenv("QUEUELINE_BASE_URL")
	if qlURL == "" {
		qlURL = "http://localhost:8081"
	}

	tickSecs := envInt("TICK_INTERVAL_SECONDS", 10)
	retrySecs := envInt("LEADER_RETRY_INTERVAL_SECONDS", 2)
	lockKey := int64(envInt("LEADER_LOCK_KEY", 918001))

	return Config{
		DatabaseURL:         dbURL,
		HTTPPort:            port,
		QueueLineBaseURL:    qlURL,
		TickInterval:        time.Duration(tickSecs) * time.Second,
		LeaderRetryInterval: time.Duration(retrySecs) * time.Second,
		LeaderLockKey:       lockKey,
	}, nil
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
