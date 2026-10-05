// Author: Torin Etheridge
// Date: 2026-10-04

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use relay::{
    model::{Priority, RetryPolicy, SubmitJob},
    worker::run_worker,
    Client, ServerConfig, Store,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "relay",
    version,
    about = "A durable, lease-based distributed job queue"
)]
struct Cli {
    #[arg(long, global = true, default_value = "127.0.0.1:7400")]
    address: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Server {
        #[arg(long, default_value = "relay.db")]
        db: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7401")]
        metrics_address: String,
        #[arg(long, default_value_t = 1024)]
        max_connections: usize,
    },
    Worker {
        #[arg(short, long, required = true)]
        queue: Vec<String>,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long)]
        worker_id: Option<String>,
        #[arg(long, default_value_t = 30_000)]
        lease_ms: u64,
        #[arg(long, default_value_t = 100)]
        poll_ms: u64,
    },
    Submit(SubmitArgs),
    Inspect {
        job_id: String,
    },
    Stats,
    Dead {
        #[command(subcommand)]
        command: DeadCommand,
    },
}
#[derive(Args)]
struct SubmitArgs {
    queue: String,
    #[arg(long)]
    payload: String,
    #[arg(long,value_enum,default_value_t=Priority::Normal)]
    priority: Priority,
    #[arg(long, default_value_t = 3)]
    retries: u32,
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    #[arg(long)]
    idempotency_key: Option<String>,
    #[arg(long, default_value = "exponential")]
    retry: String,
    #[arg(long, default_value_t = 1000)]
    retry_delay_ms: u64,
    #[arg(long, default_value_t = 60_000)]
    max_retry_delay_ms: u64,
    #[arg(long, default_value_t = true)]
    jitter: bool,
}
#[derive(Subcommand)]
enum DeadCommand {
    List {
        #[arg(long)]
        queue: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Retry {
        job_id: String,
    },
    Purge {
        job_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("relay=info".parse()?),
        )
        .json()
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Server {
            db,
            metrics_address,
            max_connections,
        } => {
            let config = ServerConfig {
                listen: cli.address.parse().context("invalid listen address")?,
                metrics_listen: metrics_address.parse().context("invalid metrics address")?,
                max_connections,
            };
            relay::run_server(Store::open(db)?, config).await?;
        }
        Command::Worker {
            queue,
            concurrency,
            worker_id,
            lease_ms,
            poll_ms,
        } => {
            let id =
                worker_id.unwrap_or_else(|| format!("worker_{}", uuid::Uuid::new_v4().simple()));
            run_worker(cli.address, id, queue, concurrency, lease_ms, poll_ms).await?;
        }
        Command::Submit(a) => {
            let payload = serde_json::from_str(&a.payload).context("payload must be valid JSON")?;
            let retry_policy = if a.retry == "fixed" {
                RetryPolicy::Fixed {
                    delay_ms: a.retry_delay_ms,
                }
            } else {
                RetryPolicy::Exponential {
                    base_ms: a.retry_delay_ms,
                    max_delay_ms: a.max_retry_delay_ms,
                    jitter: a.jitter,
                }
            };
            let (id, dedup) = Client::new(cli.address)
                .submit(SubmitJob {
                    queue: a.queue,
                    payload,
                    priority: a.priority,
                    max_attempts: a.retries,
                    delay_ms: a.delay_ms,
                    idempotency_key: a.idempotency_key,
                    dedup_ms: 86_400_000,
                    retry_policy,
                })
                .await?;
            println!("{id}{}", if dedup { " (deduplicated)" } else { "" });
        }
        Command::Inspect { job_id } => match Client::new(cli.address).inspect(&job_id).await? {
            Some(j) => println!("{}", serde_json::to_string_pretty(&j)?),
            None => println!("job not found"),
        },
        Command::Stats => {
            let stats = Client::new(cli.address).stats().await?;
            println!("QUEUE\tPENDING\tREADY\tLEASED\tRETRYING\tSUCCEEDED\tDEAD");
            for s in stats {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    s.queue, s.pending, s.ready, s.leased, s.retrying, s.succeeded, s.dead
                )
            }
        }
        Command::Dead { command } => {
            let c = Client::new(cli.address);
            match command {
                DeadCommand::List { queue, limit } => {
                    for j in c.dead_list(queue, limit).await? {
                        println!(
                            "{}\t{}\t{}/{}\t{}",
                            j.id,
                            j.queue,
                            j.attempts,
                            j.max_attempts,
                            j.last_error.unwrap_or_default()
                        )
                    }
                }
                DeadCommand::Retry { job_id } => {
                    c.dead_retry(&job_id).await?;
                    println!("retried {job_id}")
                }
                DeadCommand::Purge { job_id } => {
                    c.dead_purge(&job_id).await?;
                    println!("purged {job_id}")
                }
            }
        }
    }
    Ok(())
}
