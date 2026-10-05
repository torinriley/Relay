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
    #[error("persisted job {job_id} violates an invariant: {detail}")]
    Corruption { job_id: String, detail: String },
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
                priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 3),
                state TEXT NOT NULL CHECK(state IN ('pending','ready','leased','succeeded','retrying','dead')),
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
            CREATE INDEX IF NOT EXISTS jobs_ready_priority ON jobs(priority, created_at_ms, queue) WHERE state='ready';
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
            .map(|i| format!("?{}", i + 3))
            .collect::<Vec<_>>()
            .join(",");
        // Read only the oldest row in each priority class, then apply aging to
        // four candidates. This preserves the scheduling rule without sorting
        // the entire ready set for every claim.
        let sql = format!("SELECT id,created_at_ms FROM jobs WHERE state='ready' AND priority=?2 AND queue IN ({placeholders}) ORDER BY created_at_ms ASC LIMIT 1");
        let mut candidates = Vec::with_capacity(4);
        for priority in 0_i64..=3 {
            let candidate: Option<(String, i64)> = {
                let mut stmt = tx.prepare_cached(&sql)?;
                let mut values: Vec<rusqlite::types::Value> = vec![now.into(), priority.into()];
                values.extend(queues.iter().cloned().map(Into::into));
                stmt.query_row(rusqlite::params_from_iter(values), |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?
            };
            if let Some((id, created_at)) = candidate {
                let age_classes = now.saturating_sub(created_at) / 60_000;
                candidates.push((id, (priority + age_classes).min(3), created_at));
            }
        }
        let id = candidates
            .into_iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2)))
            .map(|c| c.0);
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
            let Some(s) = out.last_mut() else {
                return Err(StoreError::Corruption {
                    job_id: format!("queue:{queue}"),
                    detail: "aggregate row could not be assigned to a queue".into(),
                });
            };
            match state.as_str() {
                "pending" => s.pending = count,
                "ready" => s.ready = count,
                "leased" => s.leased = count,
                "retrying" => s.retrying = count,
                "succeeded" => s.succeeded = count,
                "dead" => s.dead = count,
                _ => {
                    return Err(StoreError::Corruption {
                        job_id: format!("queue:{queue}"),
                        detail: format!("unknown aggregate state {state:?}"),
                    })
                }
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
    struct RawJob {
        id: String,
        queue: String,
        payload: String,
        priority: i64,
        state: String,
        attempts: u32,
        max_attempts: u32,
        created_at_ms: i64,
        available_at_ms: i64,
        lease_owner: Option<String>,
        lease_token: Option<String>,
        lease_expires_at_ms: Option<i64>,
        last_error: Option<String>,
        retry_policy: String,
    }
    let raw = conn.query_row("SELECT id,queue,payload,priority,state,attempts,max_attempts,created_at_ms,available_at_ms,lease_owner,lease_token,lease_expires_at_ms,last_error,retry_policy FROM jobs WHERE id=?1", [id], |r| {
        Ok(RawJob { id:r.get(0)?, queue:r.get(1)?, payload:r.get(2)?, priority:r.get(3)?, state:r.get(4)?, attempts:r.get(5)?, max_attempts:r.get(6)?, created_at_ms:r.get(7)?, available_at_ms:r.get(8)?, lease_owner:r.get(9)?, lease_token:r.get(10)?, lease_expires_at_ms:r.get(11)?, last_error:r.get(12)?, retry_policy:r.get(13)? })
    }).optional()?;
    let Some(raw) = raw else { return Ok(None) };
    let corrupt = |detail: String| StoreError::Corruption {
        job_id: raw.id.clone(),
        detail,
    };
    let payload = serde_json::from_str(&raw.payload)
        .map_err(|e| corrupt(format!("invalid payload JSON: {e}")))?;
    let priority = Priority::try_from_value(raw.priority).map_err(corrupt)?;
    let state = JobState::from_str(&raw.state).map_err(corrupt)?;
    let retry_policy = serde_json::from_str(&raw.retry_policy)
        .map_err(|e| corrupt(format!("invalid retry policy JSON: {e}")))?;
    let lease_fields = (
        raw.lease_owner.is_some(),
        raw.lease_token.is_some(),
        raw.lease_expires_at_ms.is_some(),
    );
    if (state == JobState::Leased) != (lease_fields == (true, true, true)) {
        return Err(corrupt("lease fields do not match job state".into()));
    }
    if raw.attempts > raw.max_attempts {
        return Err(corrupt(format!(
            "attempts {} exceed max_attempts {}",
            raw.attempts, raw.max_attempts
        )));
    }
    Ok(Some(Job {
        id: raw.id,
        queue: raw.queue,
        payload,
        priority,
        state,
        attempts: raw.attempts,
        max_attempts: raw.max_attempts,
        created_at_ms: raw.created_at_ms,
        available_at_ms: raw.available_at_ms,
        lease_owner: raw.lease_owner,
        lease_token: raw.lease_token,
        lease_expires_at_ms: raw.lease_expires_at_ms,
        last_error: raw.last_error,
        retry_policy,
    }))
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

    #[test]
    fn corrupt_payload_is_reported_not_replaced() {
        let s = Store::memory().unwrap();
        let (id, _) = s.submit(job()).unwrap();
        s.connection()
            .unwrap()
            .execute("UPDATE jobs SET payload='not-json' WHERE id=?1", [&id])
            .unwrap();
        assert!(matches!(s.inspect(&id), Err(StoreError::Corruption { .. })));
    }

    #[test]
    fn corrupt_state_is_reported_not_mapped_to_dead() {
        let s = Store::memory().unwrap();
        let (id, _) = s.submit(job()).unwrap();
        let conn = s.connection().unwrap();
        conn.execute_batch("PRAGMA ignore_check_constraints=ON")
            .unwrap();
        conn.execute("UPDATE jobs SET state='impossible' WHERE id=?1", [&id])
            .unwrap();
        drop(conn);
        assert!(matches!(s.inspect(&id), Err(StoreError::Corruption { .. })));
    }

    #[test]
    fn indexed_scheduler_preserves_priority_aging() {
        let s = Store::memory().unwrap();
        let mut low = job();
        low.priority = Priority::Low;
        let (low_id, _) = s.submit(low).unwrap();
        s.connection()
            .unwrap()
            .execute(
                "UPDATE jobs SET created_at_ms=created_at_ms-300000 WHERE id=?1",
                [&low_id],
            )
            .unwrap();
        let mut high = job();
        high.priority = Priority::High;
        s.submit(high).unwrap();
        let (leased, _) = s
            .lease("worker", &["default".into()], 1_000)
            .unwrap()
            .unwrap();
        assert_eq!(leased.id, low_id, "aged low-priority work must not starve");
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_operations_preserve_core_invariants(operations in proptest::collection::vec(0u8..6, 1..100)) {
            let s = Store::memory().unwrap();
            let mut submitted = job();
            submitted.max_attempts = 3;
            let (id, _) = s.submit(submitted).unwrap();
            let mut current_token: Option<String> = None;
            let mut previous_attempts = 0;
            for operation in operations {
                match operation {
                    0 => if let Ok(Some((_, token))) = s.lease("worker", &["default".into()], 1_000) { current_token = Some(token); },
                    1 => { let _ = s.ack(&id, "stale", "stale-token"); },
                    2 => { let _ = s.fail(&id, "stale", "stale-token", "late"); },
                    3 => if let Some(token) = current_token.take() { let _ = s.ack(&id, "worker", &token); },
                    4 => if let Some(token) = current_token.take() { let _ = s.fail(&id, "worker", &token, "retry"); },
                    _ => { let _ = s.promote(); },
                }
                let persisted = s.inspect(&id).unwrap().unwrap();
                proptest::prop_assert!(persisted.attempts >= previous_attempts);
                proptest::prop_assert!(persisted.attempts <= persisted.max_attempts);
                let all_lease_fields = persisted.lease_owner.is_some() && persisted.lease_token.is_some() && persisted.lease_expires_at_ms.is_some();
                proptest::prop_assert_eq!(persisted.state == JobState::Leased, all_lease_fields);
                if persisted.state.is_terminal() {
                    proptest::prop_assert!(s.lease("other", &["default".into()], 1_000).unwrap().is_none());
                }
                previous_attempts = persisted.attempts;
            }
        }
    }
}
