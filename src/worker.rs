// Author: Torin Etheridge
// Date: 2026-10-04

use crate::Client;
use anyhow::Result;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{watch, Semaphore},
    task::JoinSet,
};
use tracing::{info, warn};

pub async fn run_worker(
    address: String,
    worker_id: String,
    queues: Vec<String>,
    concurrency: usize,
    lease_ms: u64,
    poll_ms: u64,
    shutdown_timeout_ms: u64,
) -> Result<()> {
    let client = Client::new(address);
    let slots = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut jobs = JoinSet::new();
    let mut shutdown = std::pin::pin!(tokio::signal::ctrl_c());
    info!(worker_id, ?queues, concurrency, "worker started");
    loop {
        while jobs.try_join_next().is_some() {}
        let permit = tokio::select! {
            result = slots.clone().acquire_owned() => result?,
            _ = &mut shutdown => break,
        };
        let lease = tokio::select! {
            result = client.lease(&worker_id, queues.clone(), lease_ms) => result,
            _ = &mut shutdown => { drop(permit); break; }
        };
        match lease {
            Ok(Some((job, token))) => {
                let c = client.clone();
                let w = worker_id.clone();
                jobs.spawn(async move {
                    let _permit = permit;
                    let id = job.id.clone();
                    let (done_tx, mut done_rx) = watch::channel(false);
                    let heartbeat_client = c.clone();
                    let heartbeat_id = id.clone();
                    let heartbeat_worker = w.clone();
                    let heartbeat_token = token.clone();
                    let heartbeat = tokio::spawn(async move {
                        let interval = Duration::from_millis((lease_ms / 3).max(250));
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(interval) => {
                                    if let Err(error) = heartbeat_client.renew(&heartbeat_id, &heartbeat_worker, &heartbeat_token, lease_ms).await {
                                        warn!(job_id=%heartbeat_id, %error, "lease renewal failed");
                                        break;
                                    }
                                }
                                _ = done_rx.changed() => break,
                            }
                        }
                    });
                    let result = execute(&job.payload).await;
                    let _ = done_tx.send(true);
                    let _ = heartbeat.await;
                    if let Err(e) = match result {
                        Ok(()) => c.ack(&id, &w, &token).await,
                        Err(e) => c.fail(&id, &w, &token, &e).await,
                    } {
                        warn!(job_id=%id,error=%e,"job disposition failed")
                    } else {
                        info!(job_id=%id,attempt=job.attempts,"job finished")
                    }
                });
            }
            Ok(None) => {
                drop(permit);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_ms.max(10))) => {},
                    _ = &mut shutdown => break,
                }
            }
            Err(e) => {
                drop(permit);
                warn!(error=%e,"lease request failed");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    _ = &mut shutdown => break,
                }
            }
        }
    }
    slots.close();
    let outstanding = jobs.len();
    info!(
        worker_id,
        outstanding, shutdown_timeout_ms, "worker draining"
    );
    let drained = drain_jobs(&mut jobs, Duration::from_millis(shutdown_timeout_ms)).await;
    if !drained {
        warn!(
            worker_id,
            abandoned = jobs.len(),
            "worker drain deadline exceeded; abandoning leases for expiry recovery"
        );
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
    }
    info!(worker_id, drained, "worker stopped");
    Ok(())
}

async fn drain_jobs(jobs: &mut JoinSet<()>, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        while let Some(result) = jobs.join_next().await {
            if let Err(error) = result {
                warn!(%error, "worker job task failed while draining");
            }
        }
    })
    .await
    .is_ok()
}

// The reference worker deliberately has a tiny executor. Real applications use
// the client library and map payloads to their own trusted handlers.
async fn execute(payload: &Value) -> std::result::Result<(), String> {
    let delay = payload
        .get("sleep_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(3_600_000);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    if payload.get("fail").and_then(Value::as_bool) == Some(true) {
        Err(payload
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("injected failure")
            .to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_waits_for_in_flight_tasks() {
        let mut jobs = JoinSet::new();
        jobs.spawn(async { tokio::time::sleep(Duration::from_millis(25)).await });
        assert!(drain_jobs(&mut jobs, Duration::from_secs(1)).await);
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn drain_respects_deadline() {
        let mut jobs = JoinSet::new();
        jobs.spawn(async { tokio::time::sleep(Duration::from_secs(10)).await });
        assert!(!drain_jobs(&mut jobs, Duration::from_millis(10)).await);
        assert_eq!(jobs.len(), 1);
        jobs.abort_all();
    }
}
