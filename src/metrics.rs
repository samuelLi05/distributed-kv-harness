use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub http_requests: AtomicU64,
    pub http_errors: AtomicU64,
    pub forwarded: AtomicU64,
    pub rpc_requests_sent: AtomicU64,
    pub rpc_requests_received: AtomicU64,
    pub peer_bytes_sent: AtomicU64,
    pub peer_bytes_received: AtomicU64,
    pub peer_connections: AtomicU64,
    pub durable_batches: AtomicU64,
    pub durable_records: AtomicU64,
    pub wal_bytes: AtomicU64,
    pub durable_flush_ns: AtomicU64,
    pub abd_fast_reads: AtomicU64,
    pub abd_read_repairs: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub http_requests: u64,
    pub http_errors: u64,
    pub forwarded: u64,
    pub rpc_requests_sent: u64,
    pub rpc_requests_received: u64,
    pub peer_bytes_sent: u64,
    pub peer_bytes_received: u64,
    pub peer_connections: u64,
    pub durable_batches: u64,
    pub durable_records: u64,
    pub wal_bytes: u64,
    pub durable_flush_ns: u64,
    pub abd_fast_reads: u64,
    pub abd_read_repairs: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> Snapshot {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        Snapshot {
            http_requests: get(&self.http_requests),
            http_errors: get(&self.http_errors),
            forwarded: get(&self.forwarded),
            rpc_requests_sent: get(&self.rpc_requests_sent),
            rpc_requests_received: get(&self.rpc_requests_received),
            peer_bytes_sent: get(&self.peer_bytes_sent),
            peer_bytes_received: get(&self.peer_bytes_received),
            peer_connections: get(&self.peer_connections),
            durable_batches: get(&self.durable_batches),
            durable_records: get(&self.durable_records),
            wal_bytes: get(&self.wal_bytes),
            durable_flush_ns: get(&self.durable_flush_ns),
            abd_fast_reads: get(&self.abd_fast_reads),
            abd_read_repairs: get(&self.abd_read_repairs),
        }
    }
}
