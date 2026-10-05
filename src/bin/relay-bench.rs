// Author: Torin Etheridge
// Date: 2026-10-04

use anyhow::{bail, Result};
use clap::Parser;
use relay::{
    model::{Priority, RetryPolicy, SubmitJob},
    Store,
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

#[derive(Parser)]
#[command(about = "Reproducible Relay persistence and processing benchmark")]
struct Args {
    #[arg(long, default_value_t = 10_000)]
    jobs: u64,
    /// File-backed SQLite database. Omit for an in-memory comparison run.
    #[arg(long)]
    database: Option<PathBuf>,
    #[arg(long, default_value_t = 1)]
    workers: usize,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
}

fn percentile(values: &mut [u128], p: f64) -> u128 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * p) as usize]
}

fn summary(name: &str, count: u64, elapsed: std::time::Duration, mut latency_us: Vec<u128>) {
    println!(
        "{name}: count={count} elapsed_s={:.3} ops_per_sec={:.0} p50_us={} p95_us={} p99_us={}",
        elapsed.as_secs_f64(),
        count as f64 / elapsed.as_secs_f64(),
        percentile(&mut latency_us.clone(), 0.50),
        percentile(&mut latency_us.clone(), 0.95),
        percentile(&mut latency_us, 0.99),
    );
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.jobs == 0 || args.workers == 0 || args.concurrency == 0 {
        bail!("jobs, workers, and concurrency must be positive");
    }
    let storage = args
        .database
        .as_ref()
        .map_or_else(|| "memory".to_string(), |p| p.display().to_string());
    let store = match &args.database {
        Some(path) => Store::open(path)?,
        None => Store::memory()?,
    };
    if store
        .stats()?
        .iter()
        .any(|s| s.pending + s.ready + s.leased + s.retrying + s.succeeded + s.dead > 0)
    {
        bail!("benchmark database is not empty: {storage}");
    }

    println!(
        "relay-bench storage={} jobs={} workers={} concurrency={} threads={}",
        storage,
        args.jobs,
        args.workers,
        args.concurrency,
        args.workers.saturating_mul(args.concurrency)
    );

    let start = Instant::now();
    let mut submission_us = Vec::with_capacity(args.jobs as usize);
    for number in 0..args.jobs {
        let operation = Instant::now();
        store.submit(SubmitJob {
            queue: "bench".into(),
            payload: serde_json::json!({"n":number}),
            priority: Priority::Normal,
            max_attempts: 3,
            delay_ms: 0,
            idempotency_key: None,
            dedup_ms: 0,
            retry_policy: RetryPolicy::default(),
        })?;
        submission_us.push(operation.elapsed().as_micros());
    }
    summary("submit", args.jobs, start.elapsed(), submission_us);

    let processing_us = Arc::new(Mutex::new(Vec::with_capacity(args.jobs as usize)));
    let process_start = Instant::now();
    let thread_count = args.workers.saturating_mul(args.concurrency);
    let mut threads = Vec::with_capacity(thread_count);
    for slot in 0..thread_count {
        let store = store.clone();
        let processing_us = processing_us.clone();
        threads.push(thread::spawn(move || -> Result<()> {
            let worker = format!("bench-{slot}");
            loop {
                let operation = Instant::now();
                let Some((job, token)) = store.lease(&worker, &["bench".into()], 30_000)? else {
                    break;
                };
                store.ack(&job.id, &worker, &token)?;
                processing_us
                    .lock()
                    .expect("benchmark latency mutex poisoned")
                    .push(operation.elapsed().as_micros());
            }
            Ok(())
        }));
    }
    for worker in threads {
        worker.join().expect("benchmark worker panicked")?;
    }
    let elapsed = process_start.elapsed();
    let latency = Arc::try_unwrap(processing_us)
        .expect("all benchmark workers joined")
        .into_inner()
        .expect("benchmark latency mutex poisoned");
    if latency.len() != args.jobs as usize {
        bail!(
            "accounting failure: processed {} of {} jobs",
            latency.len(),
            args.jobs
        );
    }
    summary("lease+ack", args.jobs, elapsed, latency);
    let stats = store.stats()?;
    let succeeded: u64 = stats.iter().map(|s| s.succeeded).sum();
    let unfinished: u64 = stats
        .iter()
        .map(|s| s.pending + s.ready + s.leased + s.retrying)
        .sum();
    if succeeded != args.jobs || unfinished != 0 {
        bail!(
            "terminal accounting failure: succeeded={succeeded} unfinished={unfinished} expected={}",
            args.jobs
        );
    }
    println!("peak_rss_mib={:.1}", peak_rss_bytes() as f64 / 1_048_576.0);
    Ok(())
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the supplied rusage structure on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    // SAFETY: the successful call above initialized the structure.
    let rss = unsafe { usage.assume_init() }.ru_maxrss.max(0) as u64;
    #[cfg(target_os = "macos")]
    {
        rss
    }
    #[cfg(not(target_os = "macos"))]
    {
        rss.saturating_mul(1024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}
