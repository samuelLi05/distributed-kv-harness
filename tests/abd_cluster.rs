mod common;
use common::{Cluster, http};
use distributed_kv_harness::{
    AbdCoordinator, Operation, Outcome,
    history::{Entry, Verdict, check},
    transport::LinkFault,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_histories_and_durable_restarts_for_both_modes() {
    for protocol in ["abd_basic", "abd"] {
        let mut cluster = Cluster::start(3, protocol, AbdCoordinator::build).await;
        for round in 0..8 {
            let key = format!("history-{round}");
            let clock = Arc::new(AtomicU64::new(1));
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..12 {
                let clock = clock.clone();
                let key = key.clone();
                let address = cluster.address(i % 3);
                tasks.spawn(async move {
                    let operation = match i % 3 {
                        0 => Operation::Put {
                            key: key.clone(),
                            value: json!({"write":i}),
                        },
                        1 => Operation::Get { key: key.clone() },
                        _ => Operation::Delete { key: key.clone() },
                    };
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let (method, body) = match &operation {
                        Operation::Put { value, .. } => ("PUT", value.to_string()),
                        Operation::Get { .. } => ("GET", String::new()),
                        _ => ("DELETE", String::new()),
                    };
                    let response = http(address, method, &format!("/{key}"), &body).await;
                    let outcome = match response.status {
                        200 => Some(Outcome::Value(response.json())),
                        404 => Some(Outcome::Missing),
                        201 => Some(Outcome::Written),
                        204 => Some(Outcome::Deleted),
                        other => panic!("unexpected status {other}"),
                    };
                    Entry {
                        operation,
                        start,
                        end: Some(clock.fetch_add(1, Ordering::SeqCst)),
                        outcome,
                    }
                });
            }
            let mut history = Vec::new();
            while let Some(result) = tasks.join_next().await {
                history.push(result.unwrap());
            }
            assert_eq!(
                check(&history),
                Verdict::Valid,
                "{}",
                serde_json::to_string_pretty(&history).unwrap()
            );
        }
        assert_eq!(
            http(cluster.address(0), "PUT", "/retained", r#"{"v":1}"#)
                .await
                .status,
            201
        );
        assert_eq!(
            http(cluster.address(1), "PUT", "/deleted", r#"{"v":1}"#)
                .await
                .status,
            201
        );
        assert_eq!(
            http(cluster.address(2), "DELETE", "/deleted", "")
                .await
                .status,
            204
        );
        cluster.stop_node(1).await;
        assert_eq!(
            http(cluster.address(0), "GET", "/retained", "")
                .await
                .json(),
            json!({"v":1})
        );
        assert_eq!(
            http(cluster.address(0), "PUT", "/retained", r#"{"v":2}"#)
                .await
                .status,
            201
        );
        cluster.restart_node(1).await;
        for index in 0..3 {
            cluster.stop_node(index).await;
        }
        for index in 0..3 {
            cluster.restart_node(index).await;
        }
        for index in 0..3 {
            assert_eq!(
                http(cluster.address(index), "GET", "/retained", "")
                    .await
                    .json(),
                json!({"v":2})
            );
            assert_eq!(
                http(cluster.address(index), "GET", "/deleted", "")
                    .await
                    .status,
                404
            );
        }
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_refuse_minority_reads_and_heal_without_resurrection() {
    let cluster = Cluster::start(3, "abd", AbdCoordinator::build).await;
    assert_eq!(
        http(cluster.address(0), "PUT", "/x", r#"{"old":true}"#)
            .await
            .status,
        201
    );
    for other in [1, 2] {
        cluster.nodes[2]
            .as_ref()
            .unwrap()
            .faults
            .set(other, LinkFault::Block);
        cluster.nodes[(other - 1) as usize]
            .as_ref()
            .unwrap()
            .faults
            .set(3, LinkFault::Block);
    }
    assert_eq!(
        http(cluster.address(0), "DELETE", "/x", "").await.status,
        204
    );
    assert_eq!(
        http(cluster.address(1), "PUT", "/new", r#"{"v":3}"#)
            .await
            .status,
        201
    );
    for key in ["x", "new", "missing"] {
        assert_eq!(
            http(cluster.address(2), "GET", &format!("/{key}"), "")
                .await
                .status,
            503
        );
    }
    assert_eq!(
        http(cluster.address(2), "PUT", "/new", r#"{"minority":true}"#)
            .await
            .status,
        503
    );
    for node in &cluster.nodes {
        node.as_ref().unwrap().faults.clear();
    }
    assert_eq!(http(cluster.address(2), "GET", "/x", "").await.status, 404);
    assert_eq!(
        http(cluster.address(2), "GET", "/new", "").await.json(),
        json!({"v":3})
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonowner_ingress_and_cancelled_stale_deliveries() {
    use distributed_kv_harness::abd::{Message, Register, Tag};
    let cluster = Cluster::start(6, "abd", AbdCoordinator::build).await;
    let key = cluster.key_for(4);
    assert_eq!(
        http(cluster.address(0), "PUT", &format!("/{key}"), r#"{"v":1}"#)
            .await
            .status,
        201
    );
    assert_eq!(
        http(cluster.address(1), "GET", &format!("/{key}"), "")
            .await
            .json(),
        json!({"v":1})
    );
    let high = Register {
        tag: Tag {
            counter: 100,
            writer: 4,
            incarnation: 1,
        },
        value: None,
    };
    let low = Register {
        tag: Tag {
            counter: 2,
            writer: 4,
            incarnation: 1,
        },
        value: Some(json!({"stale":true})),
    };
    for index in 3..6 {
        let coordinator = cluster.nodes[index].as_ref().unwrap().coordinator.clone();
        let message = serde_json::to_value(Message::Store {
            key: key.clone(),
            state: high.clone(),
        })
        .unwrap();
        let pending = tokio::spawn(async move { coordinator.on_peer(1, message).await });
        // Cancellation can occur at any point; retry the same update before testing stale delivery.
        tokio::task::yield_now().await;
        pending.abort();
        let coordinator = &cluster.nodes[index].as_ref().unwrap().coordinator;
        coordinator
            .on_peer(
                1,
                serde_json::to_value(Message::Store {
                    key: key.clone(),
                    state: high.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        coordinator
            .on_peer(
                1,
                serde_json::to_value(Message::Store {
                    key: key.clone(),
                    state: low.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{key}"), "")
            .await
            .status,
        404
    );
    assert_eq!(
        http(
            cluster.address(0),
            "PUT",
            &format!("/{key}"),
            r#"{"recreated":true}"#
        )
        .await
        .status,
        201
    );
    assert_eq!(
        http(cluster.address(1), "GET", &format!("/{key}"), "")
            .await
            .json(),
        json!({"recreated":true})
    );
    cluster.shutdown().await;
}
