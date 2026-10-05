<!-- Author: Torin Etheridge | Date: 2026-10-04 -->

# Benchmarking and profiling

Build and run on an otherwise idle machine:

```bash
cargo build --release
./target/release/relay-bench --jobs 100000
```

The harness reports actual elapsed time, submissions per second, and p50/p95/p99 call latency. Record CPU, memory, operating system, filesystem, Rust version, and whether storage is durable when publishing a result. Do not compare in-memory results to file-backed production runs.

For end-to-end matrices, run the server against a temporary database, vary worker count and `--concurrency`, submit a fixed payload set, and derive completion time from `relay stats`. Monitor resident memory during a producer-overload run and recovery time after killing/restarting the server.

## Profiling protocol

No optimization claim is recorded without a captured profile. On macOS, use Instruments Time Profiler; on Linux, use `perf record` and a flamegraph. Record observation, hypothesis, change, and before/after result. The expected first bottleneck is the deliberately serialized SQLite writer, but that is a hypothesis—not a measured result in this repository.
