//! Opaque protocol-state records. Only the coordinator interprets their contents.
//! A dedicated writer owns the WAL, fsyncs before publishing state, and survives caller cancellation.
use crate::{config::stable_hash, metrics::Metrics, types::KvError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

const MAX_RECORD: usize = 4 * 1024 * 1024;
const MAX_GROUP: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    pub state: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WalEntry {
    Header { version: u32, identity: Value },
    Commit { records: Vec<Record> },
}

enum Command {
    Commit {
        records: Vec<Record>,
        frame: Vec<u8>,
        reply: oneshot::Sender<Result<(), KvError>>,
    },
    Close(oneshot::Sender<()>),
}

pub struct JournalStore {
    state: Arc<RwLock<HashMap<String, Value>>>,
    failed: Arc<AtomicBool>,
    tx: mpsc::Sender<Command>,
}

impl JournalStore {
    pub fn open(
        directory: impl AsRef<Path>,
        identity: Value,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>, KvError> {
        let directory = directory.as_ref();
        std::fs::create_dir_all(directory)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("state.wal"))?;
        file.try_lock().map_err(|e| {
            KvError::storage(format!(
                "data directory is already locked or cannot be locked: {e}"
            ))
        })?;
        let mut state = HashMap::new();
        if file.metadata()?.len() == 0 {
            file.write_all(&encode(&WalEntry::Header {
                version: 1,
                identity: identity.clone(),
            })?)?;
            file.sync_all()?;
            File::open(directory)?.sync_all()?;
        } else {
            replay(&mut file, &identity, &mut state)?;
        }
        file.seek(SeekFrom::End(0))?;
        let state = Arc::new(RwLock::new(state));
        let failed = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(256);
        let store = Arc::new(Self {
            state: state.clone(),
            failed: failed.clone(),
            tx,
        });
        std::thread::Builder::new()
            .name("kv-journal".into())
            .spawn(move || writer(file, rx, state, failed, metrics))?;
        Ok(store)
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, KvError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(KvError::storage("store is closed or failed"));
        }
        let map = self
            .state
            .read()
            .map_err(|_| KvError::storage("store lock poisoned"))?;
        Ok(map.get(key).cloned())
    }

    /// Atomically publishes all records after their WAL entry is durably synced.
    pub async fn commit(&self, records: Vec<Record>) -> Result<(), KvError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(KvError::storage("store is closed or failed"));
        }
        if records.is_empty() {
            return Ok(());
        }
        let frame = encode(&WalEntry::Commit {
            records: records.clone(),
        })?;
        let (reply, done) = oneshot::channel();
        self.tx
            .send(Command::Commit {
                records,
                frame,
                reply,
            })
            .await
            .map_err(|_| KvError::storage("journal writer stopped"))?;
        // Once enqueued, the writer owns both persistence and publication even if this future is cancelled.
        done.await
            .map_err(|_| KvError::storage("journal writer stopped before acknowledgment"))?
    }

    pub async fn close(&self) {
        let (reply, done) = oneshot::channel();
        if self.tx.send(Command::Close(reply)).await.is_ok() {
            let _ = done.await;
        }
    }
}

fn encode(entry: &WalEntry) -> Result<Vec<u8>, KvError> {
    let payload = serde_json::to_vec(entry)?;
    if payload.len() > MAX_RECORD {
        return Err(KvError::invalid("WAL entry exceeds 4 MiB"));
    }
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&stable_hash(&payload).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn replay(
    file: &mut File,
    identity: &Value,
    map: &mut HashMap<String, Value>,
) -> Result<(), KvError> {
    let mut first = true;
    let mut valid_end = 0;
    loop {
        let mut length = [0; 4];
        match file.read(&mut length[..1])? {
            0 => break,
            1 => (),
            _ => unreachable!(),
        }
        let payload = (|| -> std::io::Result<(u64, Vec<u8>)> {
            file.read_exact(&mut length[1..])?;
            let len = u32::from_be_bytes(length) as usize;
            if len == 0 || len > MAX_RECORD {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid WAL length",
                ));
            }
            let mut checksum = [0; 8];
            file.read_exact(&mut checksum)?;
            let mut data = vec![0; len];
            file.read_exact(&mut data)?;
            Ok((u64::from_be_bytes(checksum), data))
        })();
        let (checksum, payload) = match payload {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !first => {
                // Only an incomplete final frame is recoverable. Never discard a complete corrupt entry.
                file.set_len(valid_end)?;
                file.sync_all()?;
                break;
            }
            Err(e) => {
                return Err(KvError::storage(format!(
                    "invalid WAL at offset {valid_end}: {e}"
                )));
            }
        };
        if stable_hash(&payload) != checksum {
            return Err(KvError::storage(format!(
                "WAL checksum mismatch at offset {valid_end}"
            )));
        }
        let entry: WalEntry = serde_json::from_slice(&payload)
            .map_err(|e| KvError::storage(format!("invalid WAL entry: {e}")))?;
        match entry {
            WalEntry::Header {
                version: 1,
                identity: stored,
            } if first && stored == *identity => (),
            WalEntry::Commit { records } if !first => {
                for record in records {
                    map.insert(record.key, record.state);
                }
            }
            _ => {
                return Err(KvError::storage(
                    "WAL identity, format version or entry sequence does not match this node",
                ));
            }
        }
        first = false;
        valid_end = file.stream_position()?;
    }
    if first {
        return Err(KvError::storage("WAL header is missing"));
    }
    Ok(())
}

fn writer(
    mut file: File,
    mut rx: mpsc::Receiver<Command>,
    state: Arc<RwLock<HashMap<String, Value>>>,
    failed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
) {
    while let Some(command) = rx.blocking_recv() {
        let mut group = Vec::new();
        let mut closing = None;
        match command {
            Command::Close(reply) => {
                drop(file);
                failed.store(true, Ordering::Release);
                let _ = reply.send(());
                return;
            }
            command => group.push(command),
        }
        while group.len() < MAX_GROUP {
            match rx.try_recv() {
                Ok(Command::Close(reply)) => {
                    closing = Some(reply);
                    break;
                }
                Ok(command) => group.push(command),
                Err(_) => break,
            }
        }
        let result = (|| -> Result<(), KvError> {
            if failed.load(Ordering::Acquire) {
                return Err(KvError::storage("journal previously failed"));
            }
            let mut bytes = 0;
            let mut records_written = 0;
            for command in &group {
                if let Command::Commit { frame, records, .. } = command {
                    file.write_all(frame)?;
                    bytes += frame.len() as u64;
                    records_written += records.len() as u64;
                }
            }
            file.sync_all()?;
            let mut map = state
                .write()
                .map_err(|_| KvError::storage("store lock poisoned"))?;
            for command in &group {
                if let Command::Commit { records, .. } = command {
                    for record in records {
                        map.insert(record.key.clone(), record.state.clone());
                    }
                }
            }
            metrics.durable_batches.fetch_add(1, Ordering::Relaxed);
            metrics
                .durable_records
                .fetch_add(records_written, Ordering::Relaxed);
            metrics.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        })();
        if result.is_err() {
            failed.store(true, Ordering::Release);
        }
        for command in group {
            if let Command::Commit { reply, .. } = command {
                let _ = reply.send(result.clone());
            }
        }
        if let Some(reply) = closing {
            drop(file);
            failed.store(true, Ordering::Release);
            let _ = reply.send(());
            return;
        }
    }
    failed.store(true, Ordering::Release);
}
