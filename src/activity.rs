//! Pending work is counted across client cancellation and background completion.
use crate::metrics::Metrics;
use std::sync::{Arc, atomic::Ordering};
pub struct Activity(Arc<Metrics>);
impl Activity {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        metrics.work_pending.fetch_add(1, Ordering::SeqCst);
        Self(metrics)
    }
}
impl Drop for Activity {
    fn drop(&mut self) {
        self.0.work_pending.fetch_sub(1, Ordering::SeqCst);
    }
}
