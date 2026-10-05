// Author: Torin Etheridge
// Date: 2026-10-04

use crate::{
    model::{Job, SubmitJob},
    protocol::{QueueStats, Request, Response, MAX_FRAME_BYTES},
};
use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[derive(Clone)]
pub struct Client {
    address: String,
}

impl Client {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
    async fn request(&self, request: Request) -> Result<Response> {
        let stream = TcpStream::connect(&self.address)
            .await
            .with_context(|| format!("connect to {}", self.address))?;
        let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
        framed.send(serde_json::to_string(&request)?).await?;
        let line = framed
            .next()
            .await
            .ok_or_else(|| anyhow!("server closed connection"))??;
        let response: Response = serde_json::from_str(&line).context("decode server response")?;
        if let Response::Error { code, message } = &response {
            return Err(anyhow!("{code}: {message}"));
        }
        Ok(response)
    }
    pub async fn submit(&self, job: SubmitJob) -> Result<(String, bool)> {
        match self.request(Request::Submit { job }).await? {
            Response::Submitted {
                job_id,
                deduplicated,
            } => Ok((job_id, deduplicated)),
            _ => Err(anyhow!("unexpected response")),
        }
    }
    pub async fn lease(
        &self,
        worker_id: &str,
        queues: Vec<String>,
        lease_ms: u64,
    ) -> Result<Option<(Job, String)>> {
        match self
            .request(Request::Lease {
                worker_id: worker_id.into(),
                queues,
                lease_ms,
            })
            .await?
        {
            Response::Job { job, lease_token } => Ok(Some((job, lease_token))),
            Response::NoJob => Ok(None),
            _ => Err(anyhow!("unexpected response")),
        }
    }
    pub async fn ack(&self, id: &str, worker: &str, token: &str) -> Result<()> {
        self.expect_ok(Request::Ack {
            job_id: id.into(),
            worker_id: worker.into(),
            lease_token: token.into(),
        })
        .await
    }
    pub async fn renew(&self, id: &str, worker: &str, token: &str, lease_ms: u64) -> Result<()> {
        self.expect_ok(Request::Renew {
            job_id: id.into(),
            worker_id: worker.into(),
            lease_token: token.into(),
            lease_ms,
        })
        .await
    }
    pub async fn fail(&self, id: &str, worker: &str, token: &str, error: &str) -> Result<()> {
        self.expect_ok(Request::Fail {
            job_id: id.into(),
            worker_id: worker.into(),
            lease_token: token.into(),
            error: error.into(),
        })
        .await
    }
    pub async fn inspect(&self, id: &str) -> Result<Option<Job>> {
        match self.request(Request::Inspect { job_id: id.into() }).await? {
            Response::JobInfo { job } => Ok(job),
            _ => Err(anyhow!("unexpected response")),
        }
    }
    pub async fn stats(&self) -> Result<Vec<QueueStats>> {
        match self.request(Request::Stats).await? {
            Response::Stats { queues } => Ok(queues),
            _ => Err(anyhow!("unexpected response")),
        }
    }
    pub async fn dead_list(&self, queue: Option<String>, limit: u32) -> Result<Vec<Job>> {
        match self.request(Request::DeadList { queue, limit }).await? {
            Response::DeadJobs { jobs } => Ok(jobs),
            _ => Err(anyhow!("unexpected response")),
        }
    }
    pub async fn dead_retry(&self, id: &str) -> Result<()> {
        self.expect_ok(Request::DeadRetry { job_id: id.into() })
            .await
    }
    pub async fn dead_purge(&self, id: &str) -> Result<()> {
        self.expect_ok(Request::DeadPurge { job_id: id.into() })
            .await
    }
    async fn expect_ok(&self, r: Request) -> Result<()> {
        match self.request(r).await? {
            Response::Ok => Ok(()),
            _ => Err(anyhow!("unexpected response")),
        }
    }
}
