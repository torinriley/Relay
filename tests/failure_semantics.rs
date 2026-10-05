// Author: Torin Etheridge
// Date: 2026-10-04

use relay::{
    model::{JobState, Priority, RetryPolicy, SubmitJob},
    store::StoreError,
    Store,
};
use std::{thread, time::Duration};

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
