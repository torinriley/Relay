// Author: Torin Etheridge
// Date: 2026-10-04

use anyhow::Result;
use clap::Parser;
use relay::{
    model::{Priority, RetryPolicy, SubmitJob},
    Store,
};
use std::time::Instant;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 100_000)]
    jobs: u64,
}
fn percentile(mut values: Vec<u128>, p: f64) -> u128 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * p) as usize]
}
fn main() -> Result<()> {
    let a = Args::parse();
    let store = Store::memory()?;
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(a.jobs as usize);
    for i in 0..a.jobs {
        let t = Instant::now();
        store.submit(SubmitJob {
            queue: "bench".into(),
            payload: serde_json::json!({"n":i}),
            priority: Priority::Normal,
            max_attempts: 3,
            delay_ms: 0,
            idempotency_key: None,
            dedup_ms: 0,
            retry_policy: RetryPolicy::default(),
        })?;
        latencies.push(t.elapsed().as_micros());
    }
    let elapsed = start.elapsed();
    println!(
        "jobs={} elapsed_s={:.3} jobs_per_sec={:.0} p50_us={} p95_us={} p99_us={}",
        a.jobs,
        elapsed.as_secs_f64(),
        a.jobs as f64 / elapsed.as_secs_f64(),
        percentile(latencies.clone(), 0.50),
        percentile(latencies.clone(), 0.95),
        percentile(latencies, 0.99)
    );
    Ok(())
}
