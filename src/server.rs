// Author: Torin Etheridge
// Date: 2026-10-04

use crate::{
    metrics::Metrics,
    model::JobState,
    protocol::{Request, Response, MAX_FRAME_BYTES},
    store::{Store, StoreError},
};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::JoinSet,
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, warn};

#[derive(Clone)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub metrics_listen: SocketAddr,
    pub max_connections: usize,
    pub shutdown_timeout: Duration,
}

pub async fn run_server(store: Store, config: ServerConfig) -> Result<()> {
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("bind {}", config.listen))?;
    let metrics_listener = TcpListener::bind(config.metrics_listen)
        .await
        .with_context(|| format!("bind metrics {}", config.metrics_listen))?;
    let metrics = Metrics::default();
    let limit = Arc::new(Semaphore::new(config.max_connections));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    let sweeper_store = store.clone();
    let sweeper_metrics = metrics.clone();
    let mut sweeper_stop = shutdown_rx.clone();
    let sweeper = tokio::spawn(async move {
        loop {
            tokio::select! { _=tokio::time::sleep(Duration::from_millis(250))=>{ match sweeper_store.expire_leases(){Ok(n)=>sweeper_metrics.expired(n),Err(e)=>warn!(error=%e,"lease sweep failed")} let _=sweeper_store.promote(); }, _=sweeper_stop.changed()=>break }
        }
    });
    let ms = store.clone();
    let mm = metrics.clone();
    let mut metrics_stop = shutdown_rx.clone();
    let metrics_task = tokio::spawn(async move {
        loop {
            tokio::select! { accepted=metrics_listener.accept()=>if let Ok((socket,_))=accepted { let s=ms.clone();let m=mm.clone();tokio::spawn(async move{serve_metrics(socket,s,m).await;}); }, _=metrics_stop.changed()=>break }
        }
    });
    info!(listen=%config.listen,metrics=%config.metrics_listen,"relay server started");
    loop {
        while connections.try_join_next().is_some() {}
        tokio::select! {
            accepted=listener.accept()=>{ let (socket,peer)=accepted?; let permit=match limit.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>{warn!(%peer,"connection limit reached");continue}}; let s=store.clone();let m=metrics.clone();let stop=shutdown_rx.clone();connections.spawn(async move{let _permit=permit;if let Err(e)=handle(socket,s,m,stop).await{warn!(%peer,error=%e,"connection ended")}}); },
            _=tokio::signal::ctrl_c()=>{info!("shutdown requested");break;}
        }
    }
    let _ = shutdown_tx.send(true);
    let deadline = config.shutdown_timeout;
    if tokio::time::timeout(deadline, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        warn!(
            remaining = connections.len(),
            "server connection drain deadline exceeded"
        );
        connections.abort_all();
    }
    let _ = tokio::time::timeout(deadline, sweeper).await;
    let _ = tokio::time::timeout(deadline, metrics_task).await;
    Ok(())
}

async fn handle(
    socket: TcpStream,
    store: Store,
    metrics: Metrics,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    socket.set_nodelay(true)?;
    let mut framed = Framed::new(socket, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    loop {
        let frame = tokio::select! {
            result = tokio::time::timeout(Duration::from_secs(30), framed.next()) => result.context("connection idle timeout")?,
            _ = shutdown.changed() => None,
        };
        let Some(frame) = frame else { break };
        let response = match frame {
            Ok(line) => match serde_json::from_str::<Request>(&line) {
                Ok(req) => {
                    metrics.request();
                    dispatch(&store, &metrics, req, *shutdown.borrow())
                }
                Err(e) => Response::Error {
                    code: "malformed_request".into(),
                    message: e.to_string(),
                },
            },
            Err(e) => return Err(e.into()),
        };
        framed.send(serde_json::to_string(&response)?).await?;
    }
    Ok(())
}

fn dispatch(store: &Store, metrics: &Metrics, request: Request, shutting_down: bool) -> Response {
    let result: std::result::Result<Response, StoreError> = (|| {
        Ok(match request {
            Request::Submit { job } => {
                let (id, deduplicated) = store.submit(job)?;
                if !deduplicated {
                    metrics.submitted();
                }
                Response::Submitted {
                    job_id: id,
                    deduplicated,
                }
            }
            Request::Lease {
                worker_id,
                queues,
                lease_ms,
            } => {
                if shutting_down {
                    Response::Error {
                        code: "shutting_down".into(),
                        message: "server is not issuing new leases".into(),
                    }
                } else {
                    match store.lease(&worker_id, &queues, lease_ms)? {
                        Some((job, lease_token)) => {
                            metrics.leased();
                            Response::Job { job, lease_token }
                        }
                        None => Response::NoJob,
                    }
                }
            }
            Request::Renew {
                job_id,
                worker_id,
                lease_token,
                lease_ms,
            } => {
                store.renew(&job_id, &worker_id, &lease_token, lease_ms)?;
                Response::Ok
            }
            Request::Ack {
                job_id,
                worker_id,
                lease_token,
            } => {
                store.ack(&job_id, &worker_id, &lease_token)?;
                metrics.completed();
                Response::Ok
            }
            Request::Fail {
                job_id,
                worker_id,
                lease_token,
                error,
            } => {
                let state = store.fail(&job_id, &worker_id, &lease_token, &error)?;
                metrics.failed(state == JobState::Dead);
                Response::Ok
            }
            Request::Inspect { job_id } => Response::JobInfo {
                job: store.inspect(&job_id)?,
            },
            Request::Stats => Response::Stats {
                queues: store.stats()?,
            },
            Request::DeadList { queue, limit } => Response::DeadJobs {
                jobs: store.dead_list(queue.as_deref(), limit)?,
            },
            Request::DeadRetry { job_id } => {
                store.dead_retry(&job_id)?;
                Response::Ok
            }
            Request::DeadPurge { job_id } => {
                store.dead_purge(&job_id)?;
                Response::Ok
            }
            Request::Ping => Response::Pong,
        })
    })();
    result.unwrap_or_else(|e| Response::Error {
        code: match e {
            StoreError::StaleLease => "stale_lease",
            StoreError::NotFound => "not_found",
            StoreError::Invalid(_) => "invalid",
            _ => "internal",
        }
        .into(),
        message: e.to_string(),
    })
}

async fn serve_metrics(mut socket: TcpStream, store: Store, metrics: Metrics) {
    let mut buf = [0u8; 1024];
    let n = match tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await {
        Ok(Ok(n)) => n,
        _ => return,
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    if !request.starts_with("GET /metrics ") {
        let _ = socket
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            .await;
        return;
    }
    let stats = match store.stats() {
        Ok(stats) => stats,
        Err(error) => {
            warn!(%error, "cannot render metrics from invalid persisted state");
            let _ = socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                .await;
            return;
        }
    };
    let depth = stats.iter().map(|s| s.pending + s.ready + s.retrying).sum();
    let active = stats.iter().map(|s| s.leased).sum();
    let body = metrics.render(depth, active);
    let header=format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
    let _ = socket.write_all(header.as_bytes()).await;
    let _ = socket.write_all(body.as_bytes()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_fences_new_leases() {
        let response = dispatch(
            &Store::memory().unwrap(),
            &Metrics::default(),
            Request::Lease {
                worker_id: "worker".into(),
                queues: vec!["default".into()],
                lease_ms: 1_000,
            },
            true,
        );
        assert!(matches!(response, Response::Error { code, .. } if code == "shutting_down"));
    }
}
