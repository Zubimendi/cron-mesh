package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/francisoffiong/cron-mesh/internal/jobs"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

func TestShouldFireWithinGrace(t *testing.T) {
	scheduled := time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)
	now := scheduled.Add(30 * time.Second)
	if !ShouldFire(scheduled, now, 60) {
		t.Fatal("expected fire within 60s grace")
	}
}

func TestShouldFireOutsideGrace(t *testing.T) {
	scheduled := time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)
	now := scheduled.Add(2 * time.Minute)
	if ShouldFire(scheduled, now, 60) {
		t.Fatal("expected skip outside 60s grace")
	}
}

func TestShouldFireNotYetDue(t *testing.T) {
	scheduled := time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)
	now := scheduled.Add(-10 * time.Second)
	if ShouldFire(scheduled, now, 60) {
		t.Fatal("expected not fire before scheduled time")
	}
}

func TestDedupKeyDeterministic(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ts := time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)
	a := DedupKey(id, ts)
	b := DedupKey(id, ts.In(time.FixedZone("PST", -8*3600)))
	if a != b {
		t.Fatalf("dedup keys differ across zones: %q vs %q", a, b)
	}
	want := "11111111-1111-1111-1111-111111111111:2026-09-25T14:00:00Z"
	if a != want {
		t.Fatalf("got %q want %q", a, want)
	}
}

func TestNextDueAfterHourly(t *testing.T) {
	from := time.Date(2026, 9, 25, 14, 30, 0, 0, time.UTC)
	next, err := NextDueAfter("0 * * * *", from)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 9, 25, 15, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("got %v want %v", next, want)
	}
}

func TestDueScheduledTimesCatchup(t *testing.T) {
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 25, 14, 30, 0, 0, time.UTC)
	got := dueScheduledTimes(sched, last, now, true)
	if len(got) != 2 {
		t.Fatalf("got %d fires, want 2: %v", len(got), got)
	}
	if !got[0].Equal(time.Date(2026, 9, 25, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("first = %v", got[0])
	}
	if !got[1].Equal(time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("second = %v", got[1])
	}
}

type fakeStore struct {
	mu       sync.Mutex
	job      jobs.Job
	runs     []string
	lastRun  *time.Time
	seenKeys map[string]bool
}

func (f *fakeStore) ListActive(ctx context.Context) ([]jobs.Job, error) {
	j := f.job
	j.LastRunAt = f.lastRun
	return []jobs.Job{j}, nil
}

func (f *fakeStore) RecordRun(ctx context.Context, jobID uuid.UUID, scheduled time.Time, status string, qlJobID *string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := jobID.String() + ":" + scheduled.UTC().Format(time.RFC3339) + ":" + status
	if f.seenKeys == nil {
		f.seenKeys = map[string]bool{}
	}
	if f.seenKeys[key] {
		return false, nil
	}
	f.seenKeys[key] = true
	f.runs = append(f.runs, status)
	return true, nil
}

func (f *fakeStore) UpdateLastRunAt(ctx context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := at.UTC()
	f.lastRun = &t
	return nil
}

type fakeEnqueuer struct {
	mu    sync.Mutex
	calls []string
	ids   map[string]string
}

func (f *fakeEnqueuer) Enqueue(ctx context.Context, queue string, payload any, dedupKey string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, dedupKey)
	if f.ids == nil {
		f.ids = map[string]string{}
	}
	if id, ok := f.ids[dedupKey]; ok {
		return id, nil // replay — same as QueueLine 200
	}
	id := uuid.New().String()
	f.ids[dedupKey] = id
	return id, nil
}

func TestTickMisfireSkipAndCatchup(t *testing.T) {
	jobID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	last := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{
		job: jobs.Job{
			ID:                        jobID,
			Name:                      "hourly",
			CronExpr:                  "0 * * * *",
			QueueName:                 "default",
			MisfireGracePeriodSeconds: 300, // 5 minutes
			LastRunAt:                 &last,
			IsActive:                  true,
		},
		lastRun: &last,
	}
	enq := &fakeEnqueuer{}
	s := New(store, enq, time.Minute, nil)
	// now = 12:02 — owed 11:00 (outside 5m grace → miss) and 12:00 (within grace → trigger)
	s.now = func() time.Time { return time.Date(2026, 9, 25, 12, 2, 0, 0, time.UTC) }

	if err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.runs) != 2 {
		t.Fatalf("runs = %v, want missed+triggered", store.runs)
	}
	if store.runs[0] != "missed" || store.runs[1] != "triggered" {
		t.Fatalf("runs = %v", store.runs)
	}
	enq.mu.Lock()
	defer enq.mu.Unlock()
	if len(enq.calls) != 1 {
		t.Fatalf("enqueue calls = %d, want 1", len(enq.calls))
	}
}

func TestFakeQueueLineDedup(t *testing.T) {
	enq := &fakeEnqueuer{}
	ctx := context.Background()
	key := DedupKey(uuid.New(), time.Date(2026, 9, 25, 14, 0, 0, 0, time.UTC))
	id1, err := enq.Enqueue(ctx, "q", map[string]string{"a": "1"}, key)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := enq.Enqueue(ctx, "q", map[string]string{"a": "1"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("dedup should return same id: %s vs %s", id1, id2)
	}
	if len(enq.calls) != 2 {
		t.Fatalf("both attempts should be recorded, got %d", len(enq.calls))
	}
	if len(enq.ids) != 1 {
		t.Fatalf("only one logical job, got %d", len(enq.ids))
	}
}
