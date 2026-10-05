// Author: Torin Etheridge
// Date: 2026-10-04

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone, Default)]
pub struct Metrics(Arc<Inner>);

#[derive(Default)]
struct Inner {
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    dead: AtomicU64,
    leases: AtomicU64,
    expired: AtomicU64,
    requests: AtomicU64,
}

impl Metrics {
    pub fn submitted(&self) {
        self.0.submitted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn completed(&self) {
        self.0.completed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn failed(&self, dead: bool) {
        self.0.failed.fetch_add(1, Ordering::Relaxed);
        if dead {
            self.0.dead.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn leased(&self) {
        self.0.leases.fetch_add(1, Ordering::Relaxed);
    }
    pub fn expired(&self, n: u64) {
        self.0.expired.fetch_add(n, Ordering::Relaxed);
    }
    pub fn request(&self) {
        self.0.requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn render(&self, queue_depth: u64, active_leases: u64) -> String {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        format!(
            concat!(
                "# TYPE relay_jobs_submitted_total counter\nrelay_jobs_submitted_total {}\n",
                "# TYPE relay_jobs_completed_total counter\nrelay_jobs_completed_total {}\n",
                "# TYPE relay_jobs_failed_total counter\nrelay_jobs_failed_total {}\n",
                "# TYPE relay_jobs_dead_total counter\nrelay_jobs_dead_total {}\n",
                "# TYPE relay_jobs_leased_total counter\nrelay_jobs_leased_total {}\n",
                "# TYPE relay_leases_expired_total counter\nrelay_leases_expired_total {}\n",
                "# TYPE relay_requests_total counter\nrelay_requests_total {}\n",
                "# TYPE relay_queue_depth gauge\nrelay_queue_depth {}\n",
                "# TYPE relay_leases_active gauge\nrelay_leases_active {}\n"
            ),
            get(&self.0.submitted),
            get(&self.0.completed),
            get(&self.0.failed),
            get(&self.0.dead),
            get(&self.0.leases),
            get(&self.0.expired),
            get(&self.0.requests),
            queue_depth,
            active_leases
        )
    }
}
