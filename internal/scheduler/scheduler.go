package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/francisoffiong/cron-mesh/internal/jobs"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Enqueuer is the QueueLine boundary — production uses HTTP; tests use a fake.
type Enqueuer interface {
	Enqueue(ctx context.Context, queue string, payload any, dedupKey string) (jobID string, err error)
}

type JobStore interface {
	ListActive(ctx context.Context) ([]jobs.Job, error)
	RecordRun(ctx context.Context, jobID uuid.UUID, scheduled time.Time, status string, qlJobID *string) (inserted bool, err error)
	UpdateLastRunAt(ctx context.Context, id uuid.UUID, at time.Time) error
}

type Scheduler struct {
	store    JobStore
	enqueuer Enqueuer
	interval time.Duration
	log      *slog.Logger
	now      func() time.Time
}

func New(store JobStore, enqueuer Enqueuer, interval time.Duration, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		store:    store,
		enqueuer: enqueuer,
		interval: interval,
		log:      log,
		now:      time.Now,
	}
}

// Run ticks until ctx is cancelled. Caller should only invoke this while holding leadership.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	if err := s.Tick(ctx); err != nil {
		s.log.Warn("tick error", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				s.log.Warn("tick error", "err", err)
			}
		}
	}
}

func (s *Scheduler) Tick(ctx context.Context) error {
	active, err := s.store.ListActive(ctx)
	if err != nil {
		return err
	}
	now := s.now().UTC()
	for _, job := range active {
		if err := s.processJob(ctx, job, now); err != nil {
			s.log.Warn("process job", "job_id", job.ID, "name", job.Name, "err", err)
		}
	}
	return nil
}

func (s *Scheduler) processJob(ctx context.Context, job jobs.Job, now time.Time) error {
	sched, err := cron.ParseStandard(job.CronExpr)
	if err != nil {
		return fmt.Errorf("parse cron %q: %w", job.CronExpr, err)
	}

	grace := time.Duration(job.MisfireGracePeriodSeconds) * time.Second
	fromIsLastRun := job.LastRunAt != nil
	from := now.Add(-grace)
	if fromIsLastRun {
		from = job.LastRunAt.UTC()
	}

	// Walk forward from last_run_at (or grace lookback) collecting due scheduled times.
	dueTimes := dueScheduledTimes(sched, from, now, fromIsLastRun)
	for _, scheduled := range dueTimes {
		age := now.Sub(scheduled)
		if age > grace {
			s.log.Info("missed run skipped",
				"job_id", job.ID, "name", job.Name,
				"scheduled_time", scheduled, "age", age.String(), "grace", grace.String())
			if _, err := s.store.RecordRun(ctx, job.ID, scheduled, "missed", nil); err != nil {
				return err
			}
			if err := s.store.UpdateLastRunAt(ctx, job.ID, scheduled); err != nil {
				return err
			}
			continue
		}

		if err := s.trigger(ctx, job, scheduled); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) trigger(ctx context.Context, job jobs.Job, scheduled time.Time) error {
	dedupKey := DedupKey(job.ID, scheduled)

	// Enqueue first: if we crash after enqueue but before recording, the next
	// leader retries; QueueLine's dedupKey collapses the duplicate.
	qlID, err := s.enqueuer.Enqueue(ctx, job.QueueName, map[string]any{
		"jobId":         job.ID.String(),
		"jobName":       job.Name,
		"scheduledTime": scheduled.UTC().Format(time.RFC3339),
		"payload":       job.Payload,
	}, dedupKey)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	var qlPtr *string
	if qlID != "" {
		qlPtr = &qlID
	}
	if _, err := s.store.RecordRun(ctx, job.ID, scheduled, "triggered", qlPtr); err != nil {
		return err
	}
	// Always advance last_run_at, even if job_runs row already existed (handoff race).
	if err := s.store.UpdateLastRunAt(ctx, job.ID, scheduled); err != nil {
		return err
	}

	s.log.Info("triggered job",
		"job_id", job.ID, "name", job.Name,
		"scheduled_time", scheduled, "dedup_key", dedupKey, "queueline_job_id", qlID)
	return nil
}

// DedupKey is deterministic so a leadership handoff mid-trigger collapses into one QueueLine job.
func DedupKey(jobID uuid.UUID, scheduled time.Time) string {
	return fmt.Sprintf("%s:%s", jobID.String(), scheduled.UTC().Format(time.RFC3339))
}

// dueScheduledTimes returns cron fire times after `from` and at-or-before `now`.
// If fromIsLastRun is true, `from` is exclusive (already handled). Otherwise `from`
// is a lookback start and fires at/after it are candidates.
func dueScheduledTimes(sched cron.Schedule, from, now time.Time, fromIsLastRun bool) []time.Time {
	var out []time.Time
	cursor := from
	if !fromIsLastRun {
		// Start slightly before lookback so Next() can land on the first candidate.
		cursor = from.Add(-time.Second)
	}
	for {
		next := sched.Next(cursor)
		if next.IsZero() || next.After(now) {
			break
		}
		out = append(out, next.UTC())
		cursor = next
		if len(out) > 100 {
			// Safety: avoid pathological catch-up storms.
			break
		}
	}
	return out
}

// NextDueAfter is exported for unit tests: first fire strictly after t.
func NextDueAfter(cronExpr string, t time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(t.UTC()).UTC(), nil
}

// ShouldFire reports whether a scheduled time is still within the misfire grace window.
func ShouldFire(scheduled, now time.Time, graceSeconds int) bool {
	grace := time.Duration(graceSeconds) * time.Second
	return !now.Before(scheduled) && now.Sub(scheduled) <= grace
}
