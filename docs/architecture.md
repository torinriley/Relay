<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Architecture

Relay is a single-coordinator, durable queue. Protocol handlers are concurrent Tokio tasks; all correctness-critical state changes enter a small synchronous store API backed by one SQLite connection. SQLite WAL and `synchronous=FULL` provide crash-consistent commits.

## Components

```mermaid
sequenceDiagram
    participant P as Producer
    participant S as Server
    participant D as SQLite
    participant W as Worker
    P->>S: submit(queue, payload, key?)
    S->>D: INSERT transaction
    S-->>P: job_id
    W->>S: lease(queues, capacity slot)
    S->>D: BEGIN IMMEDIATE, promote or expire, select and fence
    S-->>W: job + lease_token
    loop long-running work
      W->>S: renew(job, token)
    end
    W->>S: ack/fail(job, token)
    S->>D: conditional UPDATE
```

## Persistence and recovery

The `jobs` row is the source of truth. State, attempts, schedule, retry policy, idempotency data, and lease metadata change atomically. A schema constraint requires all lease fields exactly when state is `leased`. On startup, Relay invalidates every old lease and returns it to `ready`; completed and dead rows are untouched.

SQLite serializes writes, matching Relay's one-coordinator model. This limits horizontal write scale but keeps ownership proofs local and understandable. A replicated version would need a consensus-backed transaction boundary, not merely multiple servers sharing a file.

## Concurrency

TCP connections and worker tasks are bounded. Workers acquire a semaphore permit before asking for a lease, so leased work never exceeds configured concurrency. Server handlers do no in-memory buffering of jobs. Store calls are short, serialized critical sections; the durable database absorbs backlog.

## Scheduler

Delayed and retrying rows become ready after `available_at_ms`. The 250 ms sweeper reduces idle latency; every lease transaction also promotes eligible rows and expires leases, so correctness does not depend on the background task. A partial index returns the oldest ready candidate in each of the four priority classes. Relay applies aging to those four candidates and selects the highest effective priority, then the oldest creation time. This preserves starvation resistance without sorting the whole ready set on every claim.

## Invariants

1. A transaction creates at most one current token per job.
2. Only an unexpired matching `(job, worker, token)` may renew, ACK, or fail.
3. Attempts increment in the same transaction that publishes a lease and never decrease except explicit dead-letter replay.
4. Terminal jobs are absent from scheduler queries.
5. A job is removed only by an explicit dead purge.

Persisted payloads, states, priorities, retry policies, attempt counts, and lease-field combinations are decoded and validated strictly. Invalid persisted data returns `StoreError::Corruption`; it is never silently replaced with a default state or payload.

## Shutdown

SIGINT stops the worker from claiming new jobs and starts a configurable bounded drain (30 seconds by default). In-flight jobs keep renewing their leases and send their final ACK or failure before exit. At the deadline, remaining tasks are aborted and their leases are left for expiry recovery. The server stops accepting connections, signals background tasks, flushes committed SQLite work, and exits.
