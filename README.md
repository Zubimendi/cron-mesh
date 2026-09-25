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
did. `docs/ARCHITECTURE.md` §3 walks through exactly why, and what
closes the gap (idempotent triggering via QueueLine, Project 1 — not a
second implementation of "exactly-once," which isn't actually achievable
by leader election alone in any real distributed system).

## What's here

Documentation-first, like AuthNexus, PyDataRex, and VoteGuard before it:
- `docs/ARCHITECTURE.md` — the design, in full depth, including the
  explicit contrast with PyDataRex's approach.
- `docs/STORY.md` — narrative for LinkedIn/Medium and interview talking
  points.
- `docs/CURSOR_CONTEXT.md` — the build plan, with infra/boilerplate setup
  intentionally left to you.

## Stack

Go, PostgreSQL, Docker. Depends on **QueueLine** (Project 1) for actual
job execution — CronMesh's own job is deciding *when* to trigger, not
running arbitrary code.

## Status

Architecture and planning complete. Implementation not started, by
design — see `docs/CURSOR_CONTEXT.md`.

## License

MIT.
