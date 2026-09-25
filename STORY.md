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

## Longer version
Leader election shows up constantly in system design interviews and
almost never in actual portfolios, because it sounds like it requires
implementing Raft from a whiteboard diagram. It doesn't, if the problem
you actually have is "exactly one of my N replicas should run this
scheduled job" rather than "coordinate consensus across an arbitrary
distributed system." Postgres already has a primitive built for
precisely this: `pg_try_advisory_lock`, a named lock scoped to a
database session, non-blocking to attempt, and — the detail that makes
it genuinely elegant for this use case — automatically released the
instant the holding session's connection closes, for any reason,
including a hard crash with no graceful shutdown at all. Failover isn't
something you build; it falls out of a normal TCP connection dying.

The part worth being precise about, because it's easy to overclaim: this
gives you *at-least-once* triggering, not exactly-once. A leader can
decide a job is due, start triggering it, and die before it finishes
recording that fact. The next leader, taking over automatically, has no
way to know whether that trigger completed — so the only safe assumption
is that it might not have, and it should try again. That means the
system needs a second piece to actually deliver something close to
exactly-once in practice: idempotent triggering, keyed deterministically
by job ID and scheduled time, so that if two leaders both attempt the
same scheduled run during a handoff, the second attempt is a harmless
no-op rather than a duplicate execution. Leader election bounds *how
many* replicas can be triggering at once; idempotency is what makes the
rare overlap safe. Neither one alone is the whole answer.

## Conclusion
The reason this project pairs naturally with PyDataRex isn't coincidence
— it's the actual point. Building the same distributed-systems problem
twice, with two different, both-correct mechanisms, chosen for two
different reasons, teaches something a single implementation can't: that
"how do you do leader election" doesn't have one right answer, it has a
right answer *for a given deployment shape*, and being able to name both
options and explain the tradeoff is worth more in an interview than
having memorized one of them cold. Pair that with the honest,
often-skipped detail that leader election by itself doesn't deliver
exactly-once execution — only leader election *plus* idempotent
triggering does — and this small project ends up covering more real
distributed-systems ground than its size suggests.

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
