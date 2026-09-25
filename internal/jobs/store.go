package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job not found")
var ErrDuplicate = errors.New("job already exists")

type Job struct {
	ID                        uuid.UUID       `json:"id"`
	Name                      string          `json:"name"`
	CronExpr                  string          `json:"cronExpr"`
	QueueName                 string          `json:"queueName"`
	Payload                   json.RawMessage `json:"payload"`
	MisfireGracePeriodSeconds int             `json:"misfireGracePeriodSeconds"`
	LastRunAt                 *time.Time      `json:"lastRunAt,omitempty"`
	IsActive                  bool            `json:"isActive"`
	CreatedAt                 time.Time       `json:"createdAt"`
	UpdatedAt                 time.Time       `json:"updatedAt"`
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Create(ctx context.Context, j Job) (Job, error) {
	if j.ID == uuid.Nil {
		j.ID = uuid.New()
	}
	if j.Payload == nil {
		j.Payload = json.RawMessage(`{}`)
	}
	if j.MisfireGracePeriodSeconds <= 0 {
		j.MisfireGracePeriodSeconds = 60
	}
	j.IsActive = true

	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (id, name, cron_expr, queue_name, payload, misfire_grace_period_seconds, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, true)
		RETURNING created_at, updated_at
	`, j.ID, j.Name, j.CronExpr, j.QueueName, j.Payload, j.MisfireGracePeriodSeconds).
		Scan(&j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Job{}, ErrDuplicate
		}
		return Job{}, fmt.Errorf("insert job: %w", err)
	}
	return j, nil
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	return s.scanOne(ctx, `
		SELECT id, name, cron_expr, queue_name, payload, misfire_grace_period_seconds,
		       last_run_at, is_active, created_at, updated_at
		FROM jobs WHERE id = $1
	`, id)
}

func (s *Store) List(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, cron_expr, queue_name, payload, misfire_grace_period_seconds,
		       last_run_at, is_active, created_at, updated_at
		FROM jobs ORDER BY created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) ListActive(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, cron_expr, queue_name, payload, misfire_grace_period_seconds,
		       last_run_at, is_active, created_at, updated_at
		FROM jobs WHERE is_active = true ORDER BY created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("list active jobs: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) Deactivate(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET is_active = false, updated_at = now() WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("deactivate job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpdateLastRunAt(ctx context.Context, id uuid.UUID, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET last_run_at = $2, updated_at = now() WHERE id = $1
	`, id, at)
	return err
}

// RecordRun inserts a job_runs row. Returns false if (job_id, scheduled_time)
// already exists (handoff race — caller may still enqueue; QueueLine dedupes).
func (s *Store) RecordRun(ctx context.Context, jobID uuid.UUID, scheduled time.Time, status string, qlJobID *string) (bool, error) {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO job_runs (id, job_id, scheduled_time, status, queueline_job_id)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.New(), jobID, scheduled.UTC(), status, qlJobID)
	if err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("record run: %w", err)
	}
	return true, nil
}

func (s *Store) scanOne(ctx context.Context, q string, args ...any) (Job, error) {
	row := s.pool.QueryRow(ctx, q, args...)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (Job, error) {
	var j Job
	var payload []byte
	err := row.Scan(
		&j.ID, &j.Name, &j.CronExpr, &j.QueueName, &payload,
		&j.MisfireGracePeriodSeconds, &j.LastRunAt, &j.IsActive,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return Job{}, err
	}
	j.Payload = json.RawMessage(payload)
	return j, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
