mod common;
use common::TestDir;
use distributed_kv_harness::{
    metrics::Metrics,
    store::{JournalStore, Record},
};
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

fn record(key: &str, state: serde_json::Value) -> Record {
    Record {
        key: key.into(),
        state,
    }
}

#[tokio::test]
async fn opaque_protocol_records_and_atomic_batches_survive_reopen() {
    let dir = TestDir::new();
    let identity = json!({"node":1,"protocol":"test"});
    let store = JournalStore::open(&dir.0, identity.clone(), Arc::new(Metrics::default())).unwrap();
    assert!(
        JournalStore::open(&dir.0, identity.clone(), Arc::new(Metrics::default())).is_err(),
        "a second writer must not open the WAL"
    );
    let version = json!({"timestamp":[42,3],"state":"invalid","value":{"v":1}});
    store
        .commit(vec![
            record("first", version.clone()),
            record("second", json!({"tombstone":true,"timestamp":[43,3]})),
        ])
        .await
        .unwrap();
    store.close().await;
    assert!(
        JournalStore::open(&dir.0, json!({"node":2}), Arc::new(Metrics::default())).is_err(),
        "wrong node/protocol identities must not reuse data"
    );
    let reopened = JournalStore::open(&dir.0, identity, Arc::new(Metrics::default())).unwrap();
    assert_eq!(reopened.get("first").unwrap(), Some(version));
    assert_eq!(
        reopened.get("second").unwrap(),
        Some(json!({"tombstone":true,"timestamp":[43,3]}))
    );
    reopened.close().await;
}

#[tokio::test]
async fn incomplete_last_entry_is_truncated_and_later_commits_recover() {
    let dir = TestDir::new();
    let identity = json!({"node":1});
    let store = JournalStore::open(&dir.0, identity.clone(), Arc::new(Metrics::default())).unwrap();
    store.commit(vec![record("kept", json!(1))]).await.unwrap();
    store.close().await;
    let path = dir.0.join("state.wal");
    let committed_size = std::fs::metadata(&path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    // Full length/checksum header but only a fragment of the declared payload.
    file.write_all(&100_u32.to_be_bytes()).unwrap();
    file.write_all(&0_u64.to_be_bytes()).unwrap();
    file.write_all(b"partial").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let reopened =
        JournalStore::open(&dir.0, identity.clone(), Arc::new(Metrics::default())).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_size);
    assert_eq!(reopened.get("kept").unwrap(), Some(json!(1)));
    reopened
        .commit(vec![record("later", json!(2))])
        .await
        .unwrap();
    reopened.close().await;
    let final_store = JournalStore::open(&dir.0, identity, Arc::new(Metrics::default())).unwrap();
    assert_eq!(final_store.get("later").unwrap(), Some(json!(2)));
    final_store.close().await;
}

#[tokio::test]
async fn complete_corrupt_entry_fails_closed_instead_of_discarding_acknowledged_data() {
    let dir = TestDir::new();
    let identity = json!({"node":1});
    let store = JournalStore::open(&dir.0, identity.clone(), Arc::new(Metrics::default())).unwrap();
    store
        .commit(vec![record("kept", json!({"v":1}))])
        .await
        .unwrap();
    store.close().await;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.0.join("state.wal"))
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(
        JournalStore::open(&dir.0, identity, Arc::new(Metrics::default()))
            .err()
            .unwrap()
            .message
            .contains("checksum")
    );
}
