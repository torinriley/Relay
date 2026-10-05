// Author: Torin Etheridge
// Date: 2026-10-04

use crate::model::{Job, SubmitJob};
use serde::{Deserialize, Serialize};

pub const MAX_FRAME_BYTES: usize = 1_048_576;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Submit {
        job: SubmitJob,
    },
    Lease {
        worker_id: String,
        queues: Vec<String>,
        lease_ms: u64,
    },
    Renew {
        job_id: String,
        worker_id: String,
        lease_token: String,
        lease_ms: u64,
    },
    Ack {
        job_id: String,
        worker_id: String,
        lease_token: String,
    },
    Fail {
        job_id: String,
        worker_id: String,
        lease_token: String,
        error: String,
    },
    Inspect {
        job_id: String,
    },
    Stats,
    DeadList {
        queue: Option<String>,
        limit: u32,
    },
    DeadRetry {
        job_id: String,
    },
    DeadPurge {
        job_id: String,
    },
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Submitted { job_id: String, deduplicated: bool },
    Job { job: Job, lease_token: String },
    NoJob,
    Ok,
    JobInfo { job: Option<Job> },
    Stats { queues: Vec<QueueStats> },
    DeadJobs { jobs: Vec<Job> },
    Pong,
    Error { code: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub queue: String,
    pub pending: u64,
    pub ready: u64,
    pub leased: u64,
    pub retrying: u64,
    pub succeeded: u64,
    pub dead: u64,
}
