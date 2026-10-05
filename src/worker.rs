// Author: Torin Etheridge
// Date: 2026-10-04

use crate::Client;
use anyhow::Result;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::sync::{watch, Semaphore};
use tracing::{info, warn};

pub async fn run_worker(
    address: String,
    worker_id: String,
    queues: Vec<String>,
    concurrency: usize,
    lease_ms: u64,
    poll_ms: u64,
) -> Result<()> {
    let client = Client::new(address);
    let slots = Arc::new(Semaphore::new(concurrency.max(1)));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let signal = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    info!(worker_id, ?queues, concurrency, "worker started");
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let permit =
            tokio::select! {p=slots.clone().acquire_owned()=>p?,_=shutdown_rx.changed()=>break};
        match client.lease(&worker_id, queues.clone(), lease_ms).await {
            Ok(Some((job, token))) => {
                let c = client.clone();
                let w = worker_id.clone();
                tokio::spawn(async move {
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
                tokio::time::sleep(Duration::from_millis(poll_ms.max(10))).await;
            }
            Err(e) => {
                drop(permit);
                warn!(error=%e,"lease request failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    drop(slots);
    tokio::time::sleep(Duration::from_millis(50)).await;
    signal.abort();
    info!(worker_id, "worker stopped");
    Ok(())
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
