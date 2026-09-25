# Title: One Lock, Automatically Released — Leader Election Without a Cleanup Step

## Short version (LinkedIn)
Project 18 of a 30-project backend roadmap: **CronMesh**, distributed
cron built on a Postgres session-level advisory lock — whichever replica
grabs `pg_try_advisory_lock` is the leader, and failover happens
automatically the instant that replica's connection dies, because
Postgres itself releases the lock when the session ends. No lease to
expire, no manual handoff, no cleanup step. The less obvious lesson: I
paired this with an earlier project (PyDataRex) that solves the *same*
leader-election problem with a completely different mechanism — a lease
row with a conditional UPDATE — because that one runs behind a
connection pooler in transaction mode, which is flatly incompatible with
session-level advisory locks. Two valid answers to the same question,
and the actual skill is knowing which one fits which deployment.
And the deeper point: leader election alone doesn't give you exactly-
once execution — a leader can crash between deciding to run a job and
recording that it did. That gap gets closed by idempotent triggering,
not by a stronger lock. Repo: <your-fork-url>

## Longer version (Medium)

Publish-ready draft with diagram assets: [`MEDIUM.md`](MEDIUM.md).
Images to upload: [`images/`](images/).

## Interview talking points
1. Lead with the automatic-failover property of advisory locks — no
   lease, no cleanup, release tied to connection lifetime.
2. Immediately contrast with PyDataRex's lease-row approach and the
   PgBouncer/transaction-pooling reason for the difference — this is the
   single strongest "I understand tradeoffs, not just mechanisms" signal
   available in this whole roadmap.
3. Explain precisely why leader election alone isn't exactly-once, and
   what idempotent triggering adds — most candidates stop at "only one
   leader runs it," which is an incomplete (and slightly wrong) answer.
4. Name the QueueLine integration as a deliberate reuse decision, not a
   missing feature.

## Suggested post formats

**Short (LinkedIn/X):**
> Project 18 of a 30-project backend roadmap: distributed cron via a
> Postgres advisory lock that releases itself automatically on connection
> death - no lease, no cleanup. Paired it with an earlier project that
> solves the same leader-election problem a different way (a lease row)
> because it runs behind a connection pooler that's incompatible with
> advisory locks. Two correct answers, chosen for two different reasons -
> that's the actual lesson. Open source: <link>
