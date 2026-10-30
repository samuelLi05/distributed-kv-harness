mod common;
use common::{Cluster, http};
use distributed_kv_harness::{
    Operation, Outcome,
    hermes::{Message, Tag},
    history::{Recorder, Verdict, check},
    protocol,
    store::PausePoint,
    transport::LinkFault,
};
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
fn tag(counter: u64, writer: u64) -> Tag {
    Tag {
        counter,
        writer,
        incarnation: 1,
    }
}
async fn peer(c: &Cluster, to: usize, from: u64, message: Message) {
    c.nodes[to]
        .as_ref()
        .unwrap()
        .coordinator
        .on_peer(from, serde_json::to_value(message).unwrap())
        .await
        .unwrap();
}
async fn settle(c: &Cluster) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if c.nodes
                .iter()
                .flatten()
                .all(|n| n.metrics.work_pending.load(Ordering::SeqCst) == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_concurrent_histories_and_durable_restarts() {
    for protocol in ["hermes_static", "hermes_alr_static"] {
        let mut c = Cluster::start(3, protocol, protocol::factory(protocol).unwrap()).await;
        for round in 0..40 {
            let recorder = Arc::new(Recorder::new(24));
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..12 {
                let recorder = recorder.clone();
                let coordinator = c.nodes[i % 3].as_ref().unwrap().coordinator.clone();
                tasks.spawn(async move {
                    tokio::time::sleep(Duration::from_micros(
                        ((i * 67 + round * 139) % 1000) as u64,
                    ))
                    .await;
                    let key = format!("history-{round}");
                    let op = match i % 4 {
                        0 | 1 => Operation::Put {
                            key,
                            value: json!({"writer":i}),
                        },
                        2 => Operation::Get { key },
                        _ => Operation::Delete { key },
                    };
                    let ticket = recorder.begin(op.clone()).unwrap();
                    let result = coordinator.execute(op).await;
                    assert!(result.is_ok(), "{protocol}: {result:?}");
                    recorder.finish(ticket, result.ok()).unwrap();
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
            assert_eq!(
                check(&recorder.snapshot()),
                Verdict::Valid,
                "{protocol}: {}",
                serde_json::to_string(&recorder.snapshot()).unwrap()
            );
        }
        assert_eq!(
            http(c.address(0), "PUT", "/retained", r#"{"v":1}"#)
                .await
                .status,
            201
        );
        assert_eq!(
            http(c.address(1), "PUT", "/deleted", r#"{"v":1}"#)
                .await
                .status,
            201
        );
        assert_eq!(
            http(c.address(2), "DELETE", "/deleted", "").await.status,
            204
        );
        settle(&c).await;
        c.stop_node(1).await;
        c.restart_node(1).await;
        assert_eq!(
            http(c.address(1), "GET", "/retained", "").await.json(),
            json!({"v":1})
        );
        settle(&c).await;
        for i in 0..3 {
            c.stop_node(i).await;
        }
        for i in 0..3 {
            c.restart_node(i).await;
        }
        for i in 0..3 {
            assert_eq!(
                http(c.address(i), "GET", "/retained", "").await.json(),
                json!({"v":1})
            );
            assert_eq!(http(c.address(i), "GET", "/deleted", "").await.status, 404);
        }
        c.shutdown().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_block_all_member_writes_and_alr_refuses_minority_reads() {
    for protocol in ["hermes_static", "hermes_alr_static"] {
        let c = Cluster::start(3, protocol, protocol::factory(protocol).unwrap()).await;
        assert_eq!(
            http(c.address(0), "PUT", "/x", r#"{"old":true}"#)
                .await
                .status,
            201
        );
        settle(&c).await;
        for node in 0..2 {
            c.nodes[node]
                .as_ref()
                .unwrap()
                .faults
                .set(3, LinkFault::Block);
            c.nodes[2]
                .as_ref()
                .unwrap()
                .faults
                .set(node as u64 + 1, LinkFault::Block);
        }
        assert_eq!(
            http(c.address(2), "GET", "/x", "").await.status,
            if protocol == "hermes_static" {
                200
            } else {
                503
            }
        );
        assert_eq!(
            http(c.address(0), "PUT", "/x", r#"{"unknown":true}"#)
                .await
                .status,
            503
        );
        assert_eq!(
            http(c.address(2), "PUT", "/other", r#"{"unknown":true}"#)
                .await
                .status,
            503
        );
        for node in c.nodes.iter().flatten() {
            node.faults.clear();
        }
        assert_eq!(
            http(c.address(1), "PUT", "/x", r#"{"healed":true}"#)
                .await
                .status,
            201
        );
        settle(&c).await;
        for i in 0..3 {
            assert_eq!(
                http(c.address(i), "GET", "/x", "").await.json(),
                json!({"healed":true})
            );
        }
        c.shutdown().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_validation_cannot_expose_a_newer_invalidation_and_reads_replay_original_tag() {
    for protocol in ["hermes_static", "hermes_alr_static"] {
        let c = Cluster::start(3, protocol, protocol::factory(protocol).unwrap()).await;
        peer(
            &c,
            0,
            2,
            Message::Inv {
                key: "x".into(),
                tag: tag(1, 2),
                value: Some(json!({"old":true})),
            },
        )
        .await;
        peer(
            &c,
            0,
            3,
            Message::Inv {
                key: "x".into(),
                tag: tag(2, 3),
                value: Some(json!({"new":true})),
            },
        )
        .await;
        peer(
            &c,
            0,
            2,
            Message::Val {
                key: "x".into(),
                tag: tag(1, 2),
            },
        )
        .await;
        c.nodes[0].as_ref().unwrap().faults.set(2, LinkFault::Block);
        assert_eq!(
            http(c.address(0), "GET", "/x", "").await.status,
            503,
            "invalid state must not be read"
        );
        c.nodes[0].as_ref().unwrap().faults.clear();
        assert_eq!(
            http(c.address(0), "GET", "/x", "").await.json(),
            json!({"new":true})
        );
        settle(&c).await;
        for node in c.nodes.iter().flatten() {
            let disk = node.store.get("_hermes/data/x").unwrap().unwrap();
            assert_eq!(disk["tag"], serde_json::to_value(tag(2, 3)).unwrap());
            assert_eq!(
                disk["valid"], false,
                "validation is volatile; boot must replay"
            );
        }
        c.shutdown().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_at_wal_boundaries_and_nonowner_ingress() {
    for point in [
        PausePoint::BeforeAppend,
        PausePoint::BeforeSync,
        PausePoint::AfterSync,
        PausePoint::BeforeAck,
    ] {
        let c = Cluster::start(
            6,
            "hermes_static",
            protocol::factory("hermes_static").unwrap(),
        )
        .await;
        let key = c.key_for(1);
        let node = c.nodes[0].as_ref().unwrap();
        let pause = node.store.faults.pause(point);
        let task = tokio::spawn({
            let coordinator = node.coordinator.clone();
            let key = key.clone();
            async move {
                coordinator
                    .execute(Operation::Put {
                        key,
                        value: json!({"cancelled":true}),
                    })
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pause.reached() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        drop(pause);
        settle(&c).await;
        assert_eq!(
            http(c.address(5), "GET", &format!("/{key}"), "")
                .await
                .json(),
            json!({"cancelled":true})
        );
        assert_eq!(
            http(c.address(4), "DELETE", &format!("/{key}"), "")
                .await
                .status,
            204
        );
        assert_eq!(
            http(c.address(3), "GET", &format!("/{key}"), "")
                .await
                .status,
            404
        );
        c.shutdown().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alr_sync_batches_queued_reads_without_a_timer_or_value_fanout() {
    let c = Cluster::start(
        3,
        "hermes_alr_static",
        protocol::factory("hermes_alr_static").unwrap(),
    )
    .await;
    assert_eq!(
        http(c.address(0), "PUT", "/x", r#"{"v":1}"#).await.status,
        201
    );
    settle(&c).await;
    c.nodes[0]
        .as_ref()
        .unwrap()
        .faults
        .set(2, LinkFault::Delay(Duration::from_millis(20)));
    c.nodes[0]
        .as_ref()
        .unwrap()
        .faults
        .set(3, LinkFault::Delay(Duration::from_millis(20)));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..100 {
        let coordinator = c.nodes[0].as_ref().unwrap().coordinator.clone();
        tasks.spawn(async move {
            coordinator
                .execute(Operation::Get { key: "x".into() })
                .await
                .unwrap()
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), Outcome::Value(json!({"v":1})));
    }
    let diagnostics = c.nodes[0].as_ref().unwrap().coordinator.diagnostics();
    assert_eq!(diagnostics["synced_reads"], 100);
    assert!(
        diagnostics["sync_batches"].as_u64().unwrap() < 10,
        "{diagnostics}"
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_invalidation_ack_is_replayed_with_the_same_tag_and_unknown_epochs_fail() {
    let c = Cluster::start(
        3,
        "hermes_alr_static",
        protocol::factory("hermes_alr_static").unwrap(),
    )
    .await;
    c.nodes[0]
        .as_ref()
        .unwrap()
        .faults
        .set(2, LinkFault::DropResponse);
    assert_eq!(
        http(c.address(0), "PUT", "/x", r#"{"unknown":true}"#)
            .await
            .status,
        503
    );
    let before = c.nodes[0]
        .as_ref()
        .unwrap()
        .store
        .get("_hermes/data/x")
        .unwrap()
        .unwrap()["tag"]
        .clone();
    c.nodes[0].as_ref().unwrap().faults.clear();
    assert_eq!(
        http(c.address(0), "GET", "/x", "").await.json(),
        json!({"unknown":true})
    );
    settle(&c).await;
    for node in c.nodes.iter().flatten() {
        assert_eq!(
            node.store.get("_hermes/data/x").unwrap().unwrap()["tag"],
            before
        );
    }
    assert!(
        c.nodes[0]
            .as_ref()
            .unwrap()
            .coordinator
            .on_peer(
                2,
                serde_json::to_value(Message::Sync {
                    key: "x".into(),
                    epoch: 1
                })
                .unwrap()
            )
            .await
            .is_err()
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_validation_wakes_readers_before_replay_is_started() {
    let c = Cluster::start(
        3,
        "hermes_static",
        protocol::factory("hermes_static").unwrap(),
    )
    .await;
    for i in [1, 2, 0] {
        peer(
            &c,
            i,
            2,
            Message::Inv {
                key: "x".into(),
                tag: tag(1, 2),
                value: Some(json!({"v":1})),
            },
        )
        .await;
    }
    c.nodes[0].as_ref().unwrap().faults.set(2, LinkFault::Block);
    c.nodes[0].as_ref().unwrap().faults.set(3, LinkFault::Block);
    let read = tokio::spawn({
        let coordinator = c.nodes[0].as_ref().unwrap().coordinator.clone();
        async move {
            coordinator
                .execute(Operation::Get { key: "x".into() })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(3)).await;
    peer(
        &c,
        0,
        2,
        Message::Val {
            key: "x".into(),
            tag: tag(1, 2),
        },
    )
    .await;
    assert_eq!(read.await.unwrap().unwrap(), Outcome::Value(json!({"v":1})));
    assert_eq!(
        c.nodes[0].as_ref().unwrap().coordinator.diagnostics()["replays"],
        0
    );
    c.shutdown().await;
}
