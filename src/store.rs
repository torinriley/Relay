// Author: Torin Etheridge
// Date: 2026-10-04

use crate::{
    model::{valid_queue_name, Job, JobState, Priority, SubmitJob},
    protocol::QueueStats,
};
use rand::random;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("lease is stale or not owned by this worker")]
    StaleLease,
    #[error("job not found")]
    NotFound,
    #[error("internal lock poisoned")]
    LockPoisoned,
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::configure(&conn)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        // Server downtime invalidates process-local ownership. Keeping unexpired
        // leases is safe but slows recovery; eagerly expiring them is at-least-once.
        store.recover_after_restart()?;
        Ok(store)
    }

    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        Ok(store)
    }

    fn configure(conn: &Connection) -> Result<()> {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")?;
        Ok(())
    }

    fn migrate(&self) -> Result<()> {
        self.connection()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                queue TEXT NOT NULL,
                payload TEXT NOT NULL,
                priority INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
                max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
                created_at_ms INTEGER NOT NULL,
                available_at_ms INTEGER NOT NULL,
                lease_owner TEXT,
                lease_token TEXT,
                lease_expires_at_ms INTEGER,
                last_error TEXT,
                retry_policy TEXT NOT NULL,
                idempotency_key TEXT,
                dedup_expires_at_ms INTEGER,
                completed_at_ms INTEGER,
                CHECK ((state = 'leased') = (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS jobs_dedup_active ON jobs(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS jobs_sched ON jobs(state, queue, available_at_ms, priority, created_at_ms);
            CREATE INDEX IF NOT EXISTS jobs_lease_expiry ON jobs(state, lease_expires_at_ms);")?;
        Ok(())
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| StoreError::LockPoisoned)
    }

    pub fn submit(&self, input: SubmitJob) -> Result<(String, bool)> {
        if !valid_queue_name(&input.queue) {
            return Err(StoreError::Invalid(
                "queue names must be 1-128 ASCII letters, digits, '.', '_' or '-'".into(),
            ));
        }
        if input.max_attempts == 0 {
            return Err(StoreError::Invalid("max_attempts must be positive".into()));
        }
        let payload = serde_json::to_string(&input.payload)
            .map_err(|e| StoreError::Invalid(e.to_string()))?;
        if payload.len() > crate::protocol::MAX_FRAME_BYTES / 2 {
            return Err(StoreError::Invalid("payload exceeds limit".into()));
        }
        if input
            .idempotency_key
            .as_ref()
            .is_some_and(|k| k.len() > 256 || k.is_empty())
        {
            return Err(StoreError::Invalid(
                "idempotency key must be 1-256 bytes".into(),
            ));
        }
        let now = now_ms();
        let available = now.saturating_add(input.delay_ms.min(i64::MAX as u64) as i64);
        let state = if input.delay_ms == 0 {
            JobState::Ready
        } else {
            JobState::Pending
        };
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(key) = &input.idempotency_key {
            let existing: Option<String> = tx.query_row(
                "SELECT id FROM jobs WHERE queue=?1 AND idempotency_key=?2 AND dedup_expires_at_ms>?3",
                params![input.queue, key, now], |r| r.get(0)).optional()?;
            if let Some(id) = existing {
                tx.commit()?;
                return Ok((id, true));
            }
            // Expired keys cease deduplicating but their jobs remain inspectable.
            tx.execute("UPDATE jobs SET idempotency_key=NULL, dedup_expires_at_ms=NULL WHERE queue=?1 AND idempotency_key=?2", params![input.queue, key])?;
        }
        let id = format!("job_{}", Uuid::now_v7().simple());
        tx.execute(
            "INSERT INTO jobs(id,queue,payload,priority,state,attempts,max_attempts,created_at_ms,available_at_ms,retry_policy,idempotency_key,dedup_expires_at_ms)
             VALUES(?1,?2,?3,?4,?5,0,?6,?7,?8,?9,?10,?11)",
            params![id, input.queue, payload, input.priority.value(), state.to_string(), input.max_attempts, now, available,
                serde_json::to_string(&input.retry_policy).map_err(|e| StoreError::Invalid(e.to_string()))?, input.idempotency_key,
                now.saturating_add(input.dedup_ms.min(i64::MAX as u64) as i64)])?;
        tx.commit()?;
        Ok((id, false))
    }

    pub fn lease(
        &self,
        worker: &str,
        queues: &[String],
        lease_ms: u64,
    ) -> Result<Option<(Job, String)>> {
        if worker.is_empty() || worker.len() > 128 {
            return Err(StoreError::Invalid("worker id must be 1-128 bytes".into()));
        }
        if queues.is_empty() || queues.len() > 32 || queues.iter().any(|q| !valid_queue_name(q)) {
            return Err(StoreError::Invalid("provide 1-32 valid queues".into()));
        }
        let now = now_ms();
        let expiry = now.saturating_add(lease_ms.clamp(1_000, 3_600_000) as i64);
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_and_promote(&tx, now)?;
        let placeholders = (0..queues.len())
            .map(|i| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        // Aging adds one class per minute, capped at critical. FIFO breaks ties.
        let sql = format!("SELECT id FROM jobs WHERE state='ready' AND available_at_ms<=?1 AND queue IN ({placeholders})
            ORDER BY MIN(3, priority + ((?1-created_at_ms)/60000)) DESC, created_at_ms ASC LIMIT 1");
        let id: Option<String> = {
            let mut stmt = tx.prepare(&sql)?;
            let mut values: Vec<rusqlite::types::Value> = vec![now.into()];
            values.extend(queues.iter().cloned().map(Into::into));
            stmt.query_row(rusqlite::params_from_iter(values), |r| r.get(0))
                .optional()?
        };
        let Some(id) = id else {
            tx.commit()?;
            return Ok(None);
        };
        let token = Uuid::new_v4().to_string();
        let changed = tx.execute("UPDATE jobs SET state='leased', attempts=attempts+1, lease_owner=?2, lease_token=?3, lease_expires_at_ms=?4 WHERE id=?1 AND state='ready'", params![id, worker, token, expiry])?;
        if changed != 1 {
            return Err(StoreError::StaleLease);
        }
        let job = query_job(&tx, &id)?.ok_or(StoreError::NotFound)?;
        tx.commit()?;
        Ok(Some((job, token)))
    }

    pub fn renew(&self, id: &str, worker: &str, token: &str, lease_ms: u64) -> Result<i64> {
        let now = now_ms();
        let expiry = now.saturating_add(lease_ms.clamp(1_000, 3_600_000) as i64);
        let changed = self.connection()?.execute(
            "UPDATE jobs SET lease_expires_at_ms=?4 WHERE id=?1 AND state='leased' AND lease_owner=?2 AND lease_token=?3 AND lease_expires_at_ms>?5",
            params![id, worker, token, expiry, now])?;
        if changed == 1 {
            Ok(expiry)
        } else {
            Err(StoreError::StaleLease)
        }
    }

    pub fn ack(&self, id: &str, worker: &str, token: &str) -> Result<()> {
        let now = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE jobs SET state='succeeded', lease_owner=NULL,lease_token=NULL,lease_expires_at_ms=NULL,completed_at_ms=?4
             WHERE id=?1 AND state='leased' AND lease_owner=?2 AND lease_token=?3 AND lease_expires_at_ms>?4",
            params![id, worker, token, now])?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::StaleLease)
        }
    }

    pub fn fail(&self, id: &str, worker: &str, token: &str, error: &str) -> Result<JobState> {
        let now = now_ms();
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = query_job(&tx, id)?.ok_or(StoreError::NotFound)?;
        if job.state != JobState::Leased
            || job.lease_owner.as_deref() != Some(worker)
            || job.lease_token.as_deref() != Some(token)
            || job.lease_expires_at_ms.is_none_or(|e| e <= now)
        {
            return Err(StoreError::StaleLease);
        }
        let state = if job.attempts >= job.max_attempts {
            JobState::Dead
        } else {
            JobState::Retrying
        };
        let delay = if state == JobState::Retrying {
            job.retry_policy.delay_ms(job.attempts, random::<f64>())
        } else {
            0
        };
        tx.execute("UPDATE jobs SET state=?2, available_at_ms=?3, last_error=?4, lease_owner=NULL,lease_token=NULL,lease_expires_at_ms=NULL,completed_at_ms=?5 WHERE id=?1",
            params![id, state.to_string(), now.saturating_add(delay as i64), truncate(error, 4096), if state == JobState::Dead { Some(now) } else { None }])?;
        tx.commit()?;
        Ok(state)
    }

    pub fn inspect(&self, id: &str) -> Result<Option<Job>> {
        let conn = self.connection()?;
        query_job(&conn, id)
    }

    pub fn expire_leases(&self) -> Result<u64> {
        let now = now_ms();
        let changed = self.connection()?.execute("UPDATE jobs SET state='ready',lease_owner=NULL,lease_token=NULL,lease_expires_at_ms=NULL WHERE state='leased' AND lease_expires_at_ms<=?1", [now])?;
        Ok(changed as u64)
    }

    pub fn promote(&self) -> Result<u64> {
        let now = now_ms();
        let changed = self.connection()?.execute("UPDATE jobs SET state='ready' WHERE state IN ('pending','retrying') AND available_at_ms<=?1", [now])?;
        Ok(changed as u64)
    }

    pub fn dead_list(&self, queue: Option<&str>, limit: u32) -> Result<Vec<Job>> {
        let conn = self.connection()?;
        let sql = if queue.is_some() {
            "SELECT id FROM jobs WHERE state='dead' AND queue=?1 ORDER BY completed_at_ms DESC LIMIT ?2"
        } else {
            "SELECT id FROM jobs WHERE state='dead' ORDER BY completed_at_ms DESC LIMIT ?2"
        };
        let mut stmt = conn.prepare(sql)?;
        let ids = if let Some(queue) = queue {
            stmt.query_map(params![queue, limit.min(1000)], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![limit.min(1000)], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        ids.into_iter()
            .map(|id| query_job(&conn, &id)?.ok_or(StoreError::NotFound))
            .collect()
    }

    pub fn dead_retry(&self, id: &str) -> Result<()> {
        let changed = self.connection()?.execute("UPDATE jobs SET state='ready',attempts=0,available_at_ms=?2,last_error=NULL,completed_at_ms=NULL WHERE id=?1 AND state='dead'", params![id, now_ms()])?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::NotFound)
        }
    }

    pub fn dead_purge(&self, id: &str) -> Result<()> {
        let changed = self
            .connection()?
            .execute("DELETE FROM jobs WHERE id=?1 AND state='dead'", [id])?;
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::NotFound)
        }
    }

    pub fn stats(&self) -> Result<Vec<QueueStats>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare("SELECT queue,state,COUNT(*) FROM jobs GROUP BY queue,state ORDER BY queue")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?,
            ))
        })?;
        let mut out: Vec<QueueStats> = Vec::new();
        for row in rows {
            let (queue, state, count) = row?;
            if out.last().is_none_or(|s| s.queue != queue) {
                out.push(QueueStats {
                    queue: queue.clone(),
                    pending: 0,
                    ready: 0,
                    leased: 0,
                    retrying: 0,
                    succeeded: 0,
                    dead: 0,
                });
            }
            let s = out.last_mut().expect("just inserted");
            match state.as_str() {
                "pending" => s.pending = count,
                "ready" => s.ready = count,
                "leased" => s.leased = count,
                "retrying" => s.retrying = count,
                "succeeded" => s.succeeded = count,
                "dead" => s.dead = count,
                _ => {}
            }
        }
        Ok(out)
    }

    fn recover_after_restart(&self) -> Result<()> {
        self.connection()?.execute("UPDATE jobs SET state='ready',lease_owner=NULL,lease_token=NULL,lease_expires_at_ms=NULL WHERE state='leased'", [])?;
        Ok(())
    }
}

fn expire_and_promote(tx: &rusqlite::Transaction<'_>, now: i64) -> Result<()> {
    tx.execute("UPDATE jobs SET state='ready',lease_owner=NULL,lease_token=NULL,lease_expires_at_ms=NULL WHERE state='leased' AND lease_expires_at_ms<=?1", [now])?;
    tx.execute("UPDATE jobs SET state='ready' WHERE state IN ('pending','retrying') AND available_at_ms<=?1", [now])?;
    Ok(())
}

fn query_job(conn: &Connection, id: &str) -> Result<Option<Job>> {
    conn.query_row("SELECT id,queue,payload,priority,state,attempts,max_attempts,created_at_ms,available_at_ms,lease_owner,lease_token,lease_expires_at_ms,last_error,retry_policy FROM jobs WHERE id=?1", [id], |r| {
        let payload: String = r.get(2)?;
        let state: String = r.get(4)?;
        let policy: String = r.get(13)?;
        Ok(Job { id:r.get(0)?, queue:r.get(1)?, payload:serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null), priority:Priority::from_value(r.get(3)?), state:JobState::from_str(&state).unwrap_or(JobState::Dead), attempts:r.get(5)?, max_attempts:r.get(6)?, created_at_ms:r.get(7)?, available_at_ms:r.get(8)?, lease_owner:r.get(9)?, lease_token:r.get(10)?, lease_expires_at_ms:r.get(11)?, last_error:r.get(12)?, retry_policy:serde_json::from_str(&policy).unwrap_or_default() })
    }).optional().map_err(Into::into)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RetryPolicy;
    fn job() -> SubmitJob {
        SubmitJob {
            queue: "default".into(),
            payload: serde_json::json!({"x":1}),
            priority: Priority::Normal,
            max_attempts: 2,
            delay_ms: 0,
            idempotency_key: None,
            dedup_ms: 1000,
            retry_policy: RetryPolicy::Fixed { delay_ms: 0 },
        }
    }
    #[test]
    fn stale_ack_is_rejected() {
        let s = Store::memory().unwrap();
        let (id, _) = s.submit(job()).unwrap();
        let (_, token) = s.lease("a", &["default".into()], 1000).unwrap().unwrap();
        assert!(matches!(
            s.ack(&id, "a", "wrong"),
            Err(StoreError::StaleLease)
        ));
        s.ack(&id, "a", &token).unwrap();
        assert_eq!(s.inspect(&id).unwrap().unwrap().state, JobState::Succeeded);
    }
    #[test]
    fn concurrent_claim_has_one_winner() {
        let s = Store::memory().unwrap();
        s.submit(job()).unwrap();
        let a = s.clone();
        let b = s.clone();
        let t1 =
            std::thread::spawn(move || a.lease("a", &["default".into()], 1000).unwrap().is_some());
        let t2 =
            std::thread::spawn(move || b.lease("b", &["default".into()], 1000).unwrap().is_some());
        assert_ne!(t1.join().unwrap(), t2.join().unwrap());
    }
    #[test]
    fn idempotency_returns_existing_job() {
        let s = Store::memory().unwrap();
        let mut j = job();
        j.idempotency_key = Some("order-1".into());
        let first = s.submit(j.clone()).unwrap();
        let second = s.submit(j).unwrap();
        assert_eq!(first.0, second.0);
        assert!(!first.1 && second.1);
    }
    #[test]
    fn failures_reach_dead() {
        let s = Store::memory().unwrap();
        let (id, _) = s.submit(job()).unwrap();
        for expected in [JobState::Retrying, JobState::Dead] {
            let (_, t) = s.lease("w", &["default".into()], 1000).unwrap().unwrap();
            assert_eq!(s.fail(&id, "w", &t, "boom").unwrap(), expected);
        }
    }
}
