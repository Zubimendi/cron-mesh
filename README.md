# CronMesh

Distributed cron with automatic, connection-crash-triggered leader
failover, built in Go on PostgreSQL. Week 9, Project 18 of a 15-week
backend roadmap — "run this job exactly once, on schedule, even though
we run 5 replicas of this service," solved with a Postgres session-level
advisory lock rather than a consensus protocol.

## Why this project exists

Leader election is one of the most name-dropped, least-actually-
implemented topics in backend interviews — most engineers can describe
Raft in the abstract and have never shipped a working leader-election
mechanism. CronMesh is a small, concrete, defensible one: a
`pg_try_advisory_lock` held on a dedicated connection, released
automatically by Postgres the instant that connection dies for any
reason, with no lease-expiry bookkeeping required. It's paired
deliberately with **PyDataRex** (Project 12), which solves the *same*
leader-election problem with a *different* mechanism (a lease row with a
conditional `UPDATE`) for a specifically different reason — connection-
pooler compatibility. Read both `ARCHITECTURE.md`s together for the
actual lesson: not "how does leader election work," but "which of two
valid mechanisms fits a given deployment shape."

The second, less obvious lesson this project is built to teach: **leader
election alone does not mean exactly-once execution.** A leader can
crash between deciding to trigger a job and durably recording that it
did. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) §3 walks through
exactly why, and what closes the gap (idempotent triggering via
QueueLine, Project 1 — not a second implementation of "exactly-once,"
which isn't actually achievable by leader election alone in any real
distributed system).

## What's here

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — design in full depth,
  including the explicit contrast with PyDataRex.
- [`docs/STORY.md`](docs/STORY.md) — LinkedIn blurb + interview talking
  points.
- [`docs/MEDIUM.md`](docs/MEDIUM.md) — Medium-ready article with diagram
  assets in [`docs/images/`](docs/images/).
- `cmd/cronmesh` — HTTP job API + advisory-lock leader + tick loop.
- QueueLine (Project 1) — actual job execution; CronMesh only decides
  *when* to trigger.

## Stack

Go 1.22, PostgreSQL 16, Docker Compose. Depends on **QueueLine** over
HTTP for enqueue (`dedupKey` = `{job_id}:{scheduled_time}`).

## Quick start

```bash
# Postgres + schema
make up

# Run a replica (point QUEUELINE_BASE_URL at a running QueueLine)
make run

# Create a job
curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hourly-report",
    "cronExpr": "0 * * * *",
    "queueName": "default",
    "payload": {"type": "report"},
    "misfireGracePeriodSeconds": 300
  }'

# Who is leader?
curl -s http://localhost:8080/v1/status
```

Run a second replica on another port (`HTTP_PORT=8081 make run`) against
the same database — only one becomes leader; killing the leader's
process releases the advisory lock and the standby takes over.

```bash
make test              # unit tests (misfire, dedup, QueueLine client)
make test-integration  # advisory-lock exclusivity + failover (needs make up)
```

## Status

MVP implemented: session-level advisory-lock leader election, misfire
grace handling, QueueLine idempotent triggers, job CRUD API.

## License

MIT.
