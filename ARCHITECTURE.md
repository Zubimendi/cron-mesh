# Architecture: principles → design

Same format as every project in this portfolio: each section names a
principle, what it means for a distributed cron service specifically,
and the concrete mechanism that implements it.

## System shape

```
   replica A ──┐                                ┌── replica B (standby)
              ▼                                ▼
       ┌─────────────┐                  ┌─────────────┐
       │ dedicated,     │                  │ dedicated,     │
       │ unpooled        │                  │ unpooled        │
       │ connection      │                  │ connection      │
       │ pg_try_advisory │                  │ pg_try_advisory │
       │ _lock(key)      │                  │ _lock(key)      │
       │ SUCCEEDS         │                  │ FAILS (blocks    │
       │ -> I am leader   │                  │  no one, retries │
       └──────┬──────────┘                  │  on a timer)     │
              │                             └─────────────────┘
              ▼
       ┌─────────────┐
       │ leader-only tick │  which jobs are due (cron expr + last_run_at
       │ loop              │  + misfire grace period)?
       └──────┬──────────┘
              │ trigger, with an IDEMPOTENCY KEY per (job_id, scheduled_time)
              ▼
       ┌─────────────┐
       │ QueueLine         │  actual execution - not reimplemented here
       │ (Project 1)       │
       └─────────────┘
```

---

## 1. Leader election via a session-level Postgres advisory lock

**The problem:** running N replicas of a service for availability means,
by default, N replicas independently deciding when to run every
scheduled job — not N *duplicate* runs necessarily (that depends on
downstream idempotency, see §3), but definitely N replicas doing
redundant, uncoordinated scheduling work, and no clean way to reason
about which one "actually" owns a given tick.

**The mechanism, and why it's the right tool here specifically:**
`pg_try_advisory_lock(key)` is a non-blocking attempt to acquire a named
lock scoped to the calling database *session* — exactly one session can
hold it at a time, and Postgres releases it automatically the instant
that session's connection closes, for any reason, including a hard
crash. Every CronMesh replica opens one dedicated, long-lived,
deliberately **unpooled** connection purely to attempt this lock on
startup and hold it for as long as it stays connected; only the replica
that successfully acquires it runs the scheduling tick loop. The
others retry the non-blocking acquisition attempt on a timer, costing
nothing while they wait, and take over automatically and immediately the
moment the current leader's connection drops — no lease to expire, no
manual handoff protocol, no cleanup step, because the lock's lifetime
*is* the connection's lifetime, guaranteed by Postgres itself.

**This is a deliberately different choice than PyDataRex's leader
election, and the difference is the interesting part.** PyDataRex
(Project 12) uses a lease row with a conditional `UPDATE` instead of an
advisory lock, specifically because session-level advisory locks require
a session-pinned connection and are incompatible with connection poolers
running in transaction-pooling mode (PgBouncer's most common production
configuration) — exactly the kind of pooled-connection setup a typical
web-facing application (which PyDataRex's scheduler runs alongside) uses
for everything else. CronMesh is a different kind of service: a small,
dedicated, always-on background process whose *entire job* is holding
this one lock and ticking a clock — there's no competing need to pool
its connections efficiently across many concurrent web requests, so
reserving one long-lived, unpooled connection for the lock isn't a
tradeoff, it's simply appropriate. Knowing which of two valid mechanisms
fits a given deployment shape — not just "how does leader election
work" — is the actual point of building both projects in the same
portfolio.

## 2. Misfire handling — what happens after a leader was gone for a while

**The problem:** leader election handles "exactly one replica is
scheduling right now," but says nothing about what should happen to a
job whose scheduled time passed *while there was no leader at all* — a
full cluster restart, a deployment window, a leader crash followed by a
slow failover. A job scheduled for every hour that was "owed" a run at
2:00 and 3:00 while the whole system was down needs an explicit,
configured answer to "what do we do about the runs we missed," not
silent loss or an accidental pile of retroactive catch-up runs nobody
wanted.

**The mechanism:** every job definition carries a `misfire_grace_period`.
When a (new or returning) leader begins its tick loop, it compares each
job's expected next-run time (derived from its cron expression and
`last_run_at`) against the current time. If a due run's scheduled time is
still within the grace period, it fires immediately as a catch-up run.
If it's outside the grace period, it's skipped — logged explicitly as a
missed run, not silently dropped — and scheduling resumes from the next
naturally-due tick going forward. This mirrors the same explicit-choice-
over-implicit-default discipline used for FlagForge's fallthrough
variation (an explicit, configured "what happens when no rule matches,"
not an accidental default) — a scheduler that doesn't make this decision
explicit will make it accidentally, usually by whichever behavior falls
out of how the code happens to be written, not by anyone's actual intent.

## 3. Why leader election alone is NOT exactly-once — and what closes the gap

**The honest, non-obvious point this project is built to make:** it's
tempting to believe "only one replica is ever the leader" implies "every
job runs exactly once." It doesn't. A leader can decide a job is due,
begin triggering it, and then crash *between* that decision and durably
recording that it happened — the classic gap between "acted" and
"remembered acting." When a new leader takes over (§1's automatic,
lock-release-triggered handoff), it has no way to know whether the
previous leader's in-flight trigger actually completed, so the only
safe assumption is that it might not have — meaning the new leader
should attempt it again rather than risk silently losing a scheduled
run. That means the *true* guarantee leader election alone provides is
**at-least-once triggering**, not exactly-once — the same honest
framing Dispatcher uses for webhook delivery (at-least-once delivery,
idempotent-consumer contract) rather than a stronger claim that isn't
actually true of any real distributed system without additional
machinery.

**What closes the gap:** every triggered run is submitted to
**QueueLine** (Project 1, not reimplemented here — see §4) with an
idempotency key derived deterministically from `(job_id,
scheduled_time)` — not a random value, specifically so that if a
leadership handoff mid-trigger causes the *same* scheduled run to be
submitted twice (once by the crashing leader, once by its successor,
both entirely reasonably behaving as if they might be the one that needs
to do it), QueueLine's own idempotent-enqueue guarantee (the same
mechanism LedgerLine and GateKeeper use for client-supplied idempotency
keys, here generated deterministically instead of client-chosen)
collapses the duplicate into a no-op. **Leader election bounds
concurrent triggering to effectively one attempt at a time; idempotent
triggering makes the rare handoff-window duplicate harmless.** Together,
not leader election alone, is what makes CronMesh's actual delivered
guarantee "effectively-once" — a specific, defensible, correctly-scoped
claim, not the stronger, not-actually-achievable "exactly-once" that
distributed-systems folklore likes to promise.

## 4. Deliberately not rebuilding a job queue a fourth time

**Where:** CronMesh's own responsibility ends at "decide a job is due,
and trigger it exactly-effectively-once" (§1–§3). Actually *running* a
triggered job — worker pools, retries with backoff, dead-lettering after
repeated failure — is handed off entirely to **QueueLine**. This is the
fourth appearance of the same "recognize infrastructure this portfolio
already built rather than re-solving it" discipline: ShipTrace declined
to rebuild Dispatcher-grade webhook delivery, PyDataRex declined to
rebuild a job queue for task execution, VoteGuard declined to rebuild a
rate limiter instead of sitting behind GateKeeper, and now CronMesh
declines to rebuild a job queue for the same reason PyDataRex did.
Recognizing "I already solved this problem two projects ago" is as much
the skill this portfolio is built to demonstrate as any individual
mechanism.

## What's out of scope, and why

- **A job-definition UI or a cron-expression builder.** Real product
  surface, orthogonal to the scheduling/leader-election/misfire
  mechanics this project exists to demonstrate.
- **Sub-second scheduling precision.** Cron-style scheduling is minute-
  granularity by convention and by the mechanism's own polling-tick
  nature; true sub-second scheduling is a different problem (closer to a
  real-time timer service) than what "distributed cron" means here.
- **Cross-datacenter / multi-region leader election.** v1 assumes one
  Postgres instance is reachable by every replica attempting the
  advisory lock — correct and sufficient for a single-region deployment;
  a genuinely multi-region cron service is a substantially harder,
  separate problem (likely needing a consensus protocol like Raft rather
  than a single database's lock primitive) intentionally not tackled
  here.
