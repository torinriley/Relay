// Author: Torin Etheridge
// Date: 2026-10-04

use relay::{
    model::{JobState, Priority, RetryPolicy, SubmitJob},
    store::StoreError,
    Store,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Barrier, Mutex},
    thread,
    time::Duration,
};

fn input() -> SubmitJob {
    SubmitJob {
        queue: "tests".into(),
        payload: serde_json::json!({"ok":true}),
        priority: Priority::Normal,
        max_attempts: 2,
        delay_ms: 0,
        idempotency_key: None,
        dedup_ms: 1000,
        retry_policy: RetryPolicy::Fixed { delay_ms: 0 },
    }
}

#[test]
fn worker_crash_expires_and_stale_owner_is_fenced() {
    let store = Store::memory().unwrap();
    let (id, _) = store.submit(input()).unwrap();
    let (_, old_token) = store
        .lease("crashed", &["tests".into()], 1000)
        .unwrap()
        .unwrap();
    thread::sleep(Duration::from_millis(1050));
    assert_eq!(store.expire_leases().unwrap(), 1);
    let (_, new_token) = store
        .lease("replacement", &["tests".into()], 1000)
        .unwrap()
        .unwrap();
    assert_ne!(old_token, new_token);
    assert!(matches!(
        store.ack(&id, "crashed", &old_token),
        Err(StoreError::StaleLease)
    ));
    store.ack(&id, "replacement", &new_token).unwrap();
}

#[test]
fn restart_recovers_unfinished_but_not_completed_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relay.db");
    let pending;
    let completed;
    {
        let s = Store::open(&path).unwrap();
        let (first, _) = s.submit(input()).unwrap();
        let (second, _) = s.submit(input()).unwrap();
        let (j, t) = s.lease("w", &["tests".into()], 10000).unwrap().unwrap();
        completed = j.id;
        s.ack(&completed, "w", &t).unwrap();
        let (j, _) = s.lease("w", &["tests".into()], 10000).unwrap().unwrap();
        pending = j.id;
        assert!(pending == first || pending == second);
    }
    let recovered = Store::open(&path).unwrap();
    assert_eq!(
        recovered.inspect(&pending).unwrap().unwrap().state,
        JobState::Ready
    );
    assert_eq!(
        recovered.inspect(&completed).unwrap().unwrap().state,
        JobState::Succeeded
    );
}

#[test]
fn delayed_jobs_are_not_leased_early() {
    let s = Store::memory().unwrap();
    let mut j = input();
    j.delay_ms = 100;
    s.submit(j).unwrap();
    assert!(s.lease("w", &["tests".into()], 1000).unwrap().is_none());
    thread::sleep(Duration::from_millis(110));
    assert!(s.lease("w", &["tests".into()], 1000).unwrap().is_some());
}

#[test]
fn many_workers_never_claim_the_same_job() {
    let store = Store::memory().unwrap();
    for _ in 0..200 {
        store.submit(input()).unwrap();
    }
    let claimed = Arc::new(Mutex::new(HashSet::new()));
    let mut workers = Vec::new();
    for worker_number in 0..32 {
        let store = store.clone();
        let claimed = claimed.clone();
        workers.push(thread::spawn(move || loop {
            let worker = format!("worker-{worker_number}");
            let Some((job, token)) = store.lease(&worker, &["tests".into()], 10_000).unwrap()
            else {
                break;
            };
            assert!(
                claimed.lock().unwrap().insert(job.id.clone()),
                "duplicate active claim for {}",
                job.id
            );
            store.ack(&job.id, &worker, &token).unwrap();
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(claimed.lock().unwrap().len(), 200);
}

#[test]
fn renewal_extends_ownership_past_the_original_expiry() {
    let store = Store::memory().unwrap();
    let (id, _) = store.submit(input()).unwrap();
    let (_, token) = store
        .lease("worker", &["tests".into()], 1_000)
        .unwrap()
        .unwrap();
    thread::sleep(Duration::from_millis(600));
    store.renew(&id, "worker", &token, 1_000).unwrap();
    thread::sleep(Duration::from_millis(600));
    assert_eq!(store.expire_leases().unwrap(), 0);
    store.ack(&id, "worker", &token).unwrap();
}

#[test]
fn concurrent_idempotent_submissions_create_one_job() {
    let store = Store::memory().unwrap();
    let barrier = Arc::new(Barrier::new(32));
    let mut producers = Vec::new();
    for _ in 0..32 {
        let store = store.clone();
        let barrier = barrier.clone();
        producers.push(thread::spawn(move || {
            let mut job = input();
            job.idempotency_key = Some("same-business-operation".into());
            barrier.wait();
            store.submit(job).unwrap()
        }));
    }
    let results: Vec<_> = producers.into_iter().map(|p| p.join().unwrap()).collect();
    assert_eq!(
        results.iter().map(|r| &r.0).collect::<HashSet<_>>().len(),
        1
    );
    assert_eq!(results.iter().filter(|r| !r.1).count(), 1);
}

#[test]
fn lost_ack_causes_documented_duplicate_delivery() {
    let store = Store::memory().unwrap();
    store.submit(input()).unwrap();
    let (first, old_token) = store
        .lease("first", &["tests".into()], 1_000)
        .unwrap()
        .unwrap();
    let mut external_effects = 1;
    thread::sleep(Duration::from_millis(1_050));
    store.expire_leases().unwrap();
    let (second, new_token) = store
        .lease("second", &["tests".into()], 1_000)
        .unwrap()
        .unwrap();
    external_effects += 1;
    assert_eq!(first.id, second.id);
    assert!(matches!(
        store.ack(&first.id, "first", &old_token),
        Err(StoreError::StaleLease)
    ));
    store.ack(&second.id, "second", &new_token).unwrap();
    assert_eq!(external_effects, 2);
}

#[test]
fn terminal_jobs_never_reenter_the_scheduler() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("terminal.db");
    let id;
    {
        let store = Store::open(&path).unwrap();
        (id, _) = store.submit(input()).unwrap();
        let (_, token) = store
            .lease("worker", &["tests".into()], 1_000)
            .unwrap()
            .unwrap();
        store.ack(&id, "worker", &token).unwrap();
        store.expire_leases().unwrap();
        store.promote().unwrap();
        assert!(store
            .lease("other", &["tests".into()], 1_000)
            .unwrap()
            .is_none());
    }
    let recovered = Store::open(&path).unwrap();
    assert_eq!(
        recovered.inspect(&id).unwrap().unwrap().state,
        JobState::Succeeded
    );
    assert!(recovered
        .lease("other", &["tests".into()], 1_000)
        .unwrap()
        .is_none());
}

#[test]
fn stale_token_cannot_mutate_any_lease_operation() {
    let store = Store::memory().unwrap();
    let (id, _) = store.submit(input()).unwrap();
    let (_, old_token) = store
        .lease("old", &["tests".into()], 1_000)
        .unwrap()
        .unwrap();
    thread::sleep(Duration::from_millis(1_050));
    store.expire_leases().unwrap();
    let (new_job, new_token) = store
        .lease("new", &["tests".into()], 1_000)
        .unwrap()
        .unwrap();
    let attempts = new_job.attempts;
    assert!(matches!(
        store.renew(&id, "old", &old_token, 1_000),
        Err(StoreError::StaleLease)
    ));
    assert!(matches!(
        store.fail(&id, "old", &old_token, "late"),
        Err(StoreError::StaleLease)
    ));
    assert!(matches!(
        store.ack(&id, "old", &old_token),
        Err(StoreError::StaleLease)
    ));
    assert_eq!(store.inspect(&id).unwrap().unwrap().attempts, attempts);
    store.ack(&id, "new", &new_token).unwrap();
}

#[test]
fn high_volume_restart_recovers_every_unfinished_job() {
    const JOBS: usize = 1_000;
    const CRASHED: usize = 100;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recovery.db");
    let crashed_ids;
    {
        let store = Store::open(&path).unwrap();
        for _ in 0..JOBS {
            store.submit(input()).unwrap();
        }
        let mut ids = HashSet::new();
        for worker in 0..CRASHED {
            let (job, _) = store
                .lease(&format!("crashed-{worker}"), &["tests".into()], 60_000)
                .unwrap()
                .unwrap();
            ids.insert(job.id);
        }
        crashed_ids = ids;
    }
    let store = Store::open(&path).unwrap();
    let mut processed = HashMap::new();
    while let Some((job, token)) = store.lease("recovery", &["tests".into()], 10_000).unwrap() {
        processed.insert(job.id.clone(), job.attempts);
        store.ack(&job.id, "recovery", &token).unwrap();
    }
    assert_eq!(processed.len(), JOBS);
    assert!(processed
        .iter()
        .all(|(id, attempts)| *attempts == if crashed_ids.contains(id) { 2 } else { 1 }));
    let stats = store.stats().unwrap();
    assert_eq!(stats[0].succeeded, JOBS as u64);
    assert_eq!(stats[0].ready + stats[0].leased + stats[0].retrying, 0);
}
