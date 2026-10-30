//! Kill a real subprocess at precise journal boundaries; no graceful-close assumptions.
mod common;
use common::TestDir;
use distributed_kv_harness::{
    AbdCoordinator, ClusterConfig, CoordinatorContext, NodeConfig,
    abd::{Message, Register, Tag},
    metrics::Metrics,
    store::{JournalStore, PausePoint},
    transport::Transport,
    types::{BoxFuture, KvError},
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

struct Offline;
impl Transport for Offline {
    fn request(&self, _: u64, _: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async { Err(KvError::unavailable("unused transport")) })
    }
}
fn config() -> Arc<ClusterConfig> {
    Arc::new(ClusterConfig {
        cluster_id: "cutpoint".into(),
        coordinator: "abd".into(),
        nodes: (1..=3)
            .map(|id| NodeConfig {
                id,
                http: format!("127.0.0.1:{}", 22000 + id * 2).parse().unwrap(),
                peer: format!("127.0.0.1:{}", 22001 + id * 2).parse().unwrap(),
            })
            .collect(),
        shards: vec![vec![1, 2, 3]],
        rpc_timeout_ms: 2000,
        benchmark_peer_rtt_ms: 0,
    })
}
fn state(counter: u64) -> Register {
    Register {
        tag: Tag {
            counter,
            writer: 1,
            incarnation: 1,
        },
        value: Some(json!({"version":counter,"whole":"record"})),
    }
}

#[test]
#[ignore = "subprocess helper invoked by sigkill_at_each_journal_boundary"]
fn child_crash_cutpoint() {
    let Ok(directory) = std::env::var("KV_CUTPOINT_DIRECTORY") else {
        return;
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let metrics = Arc::new(Metrics::default());
            let store =
                JournalStore::open(directory, json!({"cutpoint":1}), metrics.clone()).unwrap();
            let coordinator = AbdCoordinator::build(CoordinatorContext {
                node_id: 1,
                config: config(),
                store: store.clone(),
                transport: Arc::new(Offline),
                metrics,
            })
            .unwrap();
            coordinator.initialize().await.unwrap();
            coordinator
                .on_peer(
                    2,
                    serde_json::to_value(Message::Store {
                        key: "x".into(),
                        state: state(1),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
            let phase = std::env::var("KV_CUTPOINT_PHASE")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let _pause = store.faults.pause(
                [
                    PausePoint::BeforeAppend,
                    PausePoint::BeforeSync,
                    PausePoint::AfterSync,
                    PausePoint::BeforeAck,
                ][phase],
            );
            tokio::spawn(async move {
                coordinator
                    .on_peer(
                        2,
                        serde_json::to_value(Message::Store {
                            key: "x".into(),
                            state: state(2),
                        })
                        .unwrap(),
                    )
                    .await
                    .unwrap()
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while !_pause.reached() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            println!("KV_CRASH_READY");
            std::io::stdout().flush().unwrap();
            std::future::pending::<()>().await;
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_at_each_journal_boundary() {
    for phase in 0..4 {
        let dir = TestDir::new();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "child_crash_cutpoint",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("KV_CUTPOINT_DIRECTORY", &dir.0)
            .env("KV_CUTPOINT_PHASE", phase.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (ready, received) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("KV_CRASH_READY") {
                    let _ = ready.send(());
                    break;
                }
            }
        });
        let reached = received.recv_timeout(Duration::from_secs(10));
        child.kill().unwrap();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(reached.is_ok(), "child failed to reach phase {phase}");
        assert!(!status.success());
        let store = JournalStore::open(&dir.0, json!({"cutpoint":1}), Arc::new(Metrics::default()))
            .unwrap();
        let recovered: Register =
            serde_json::from_value(store.get("_abd/data/x").unwrap().unwrap()).unwrap();
        if phase == 0 {
            assert_eq!(recovered, state(1));
        } else if phase >= 2 {
            assert_eq!(recovered, state(2));
        } else {
            assert!(recovered == state(1) || recovered == state(2));
        }
        store
            .commit(vec![distributed_kv_harness::store::Record {
                key: "after-recovery".into(),
                state: json!({"ok":true}),
            }])
            .await
            .unwrap();
        store.close().await;
    }
}
