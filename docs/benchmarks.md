<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Benchmarks and profiling

These are measured results, not projections. They were collected on October 4, 2026 using a 10-core Apple M5 MacBook Pro with 16 GB RAM, macOS 27.0, APFS internal storage, and Rust 1.92.0. The release profile uses thin LTO. SQLite ran in WAL mode with `synchronous=FULL`; each row transition was durably committed.

## Reproduce

```bash
cargo build --release --bin relay-bench
./target/release/relay-bench \
  --jobs 5000 \
  --workers 4 \
  --concurrency 16 \
  --database /tmp/relay-bench.db
```

Use a new path for every run. The harness refuses a non-empty database, verifies that every submitted job reaches `SUCCEEDED`, and reports throughput, p50/p95/p99 operation latency, and process peak RSS.

## Current results

Each row submitted and then processed 5,000 jobs. `lease + ACK` includes the transactional claim, strict row decoding, and durable acknowledgement.

| Workers | Concurrency | Threads | Submit jobs/s | Submit p50/p95/p99 | Lease+ACK jobs/s | Lease+ACK p50/p95/p99 | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1 | 20,059 | 43/76/135 µs | 9,883 | 93/132/249 µs | 6.4 MiB |
| 1 | 16 | 16 | 19,050 | 47/77/125 µs | 9,712 | 95/127/253 µs | 6.6 MiB |
| 4 | 16 | 64 | 19,680 | 45/67/95 µs | 9,792 | 96/120/270 µs | 7.4 MiB |
| 8 | 16 | 128 | 21,134 | 42/67/100 µs | 9,159 | 100/142/279 µs | 8.4 MiB |

Throughput stays roughly flat as threads increase because Relay intentionally has one transactional SQLite writer. Additional workers improve application execution capacity, but they do not create more coordinator write capacity.

## Profile-driven scheduler improvement

### Observation

The original 5,000-job matrix plateaued near 4.2k lease+ACK operations per second. A macOS `sample` profile of a 50,000-job, 64-thread run showed worker threads predominantly waiting on the store mutex while the lease path repeatedly evaluated a computed priority expression over the ready set. That stress run became slow enough for a 30-second benchmark lease to expire before its ACK acquired the store lock.

### Hypothesis

`ORDER BY MIN(3, priority + age)` forced SQLite to reconsider and sort the full ready population for every claim. The resulting work grew with queue depth and amplified contention around the serialized writer.

### Change

Relay now maintains a partial index on ready jobs by priority and creation time. A claim reads only the oldest job in each of the four priority classes, computes aging across those four candidates, and transactionally fences the winner. Scheduling semantics are unchanged.

### Result

| 4 workers × 16 concurrency | Before | After | Change |
|---|---:|---:|---:|
| Lease+ACK throughput | 4,284 jobs/s | 9,792 jobs/s | +128.6% |
| p50 latency | 223 µs | 96 µs | −57.0% |
| p95 latency | 351 µs | 120 µs | −65.8% |
| p99 latency | 515 µs | 270 µs | −47.6% |

The post-change 50,000-job run completed all lease+ACK operations in 6.180 seconds at 8,090 jobs/s, with 101/144/894 µs p50/p95/p99 and 10.0 MiB peak RSS. The remaining profile is dominated by serialized durable commits and filesystem syncs. That is the expected single-writer durability tradeoff, so no weaker SQLite synchronization mode was adopted merely to improve a benchmark.

Results will vary with hardware and filesystem. Compare runs only when durability settings, payload, queue depth, build profile, and storage medium match.
