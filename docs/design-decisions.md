<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Design decisions

## Pull workers

Pulling couples claims to available worker semaphore slots and supplies natural backpressure. Push could reduce idle latency but would require per-worker buffering, flow control, and more disconnect recovery state.

## Leases and fencing tokens

A processing boolean cannot distinguish an active worker from a dead one, or an old owner from a replacement. Time-bounded leases recover abandoned work; random tokens fence stale owners. Clocks are evaluated only by the coordinator.

## At-least-once

Exactly-once external execution would require a transaction spanning Relay and arbitrary user systems. Relay states the achievable guarantee and gives consumers stable job IDs and submission keys for idempotency.

## SQLite

SQLite supplies durable, crash-consistent transactions without outsourcing queue semantics. It makes the project runnable as one binary and keeps claims auditable. The tradeoff is a single write coordinator and local-disk scale. Redis or Kafka would obscure the scheduling and ownership implementation; Raft would overwhelm the narrow learning goal.

## Priority aging

Strict priority can starve low work. Weighted round-robin needs persistent per-queue scheduler state. Aging has no extra state and eventually promotes every waiting job, though its exact high-priority share is workload-dependent.

## Bounded resources

The server caps connections and frames. Workers cap leases with a semaphore. Jobs remain on disk, not in an in-memory channel. These choices trade peak burst latency for predictable resource use.
