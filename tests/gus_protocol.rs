mod common;
use common::{Cluster, http};
use distributed_kv_harness::{
    Operation, Outcome,
    gus::{Message, Tag, Version},
    protocol,
    store::PausePoint,
    transport::LinkFault,
};
use serde_json::json;
use std::time::Duration;
fn tag(counter: u64, writer: u64) -> Tag {
    Tag {
        counter,
        writer,
        incarnation: 1,
    }
}
async fn peer(cluster: &Cluster, to: usize, from: u64, message: Message) -> serde_json::Value {
    cluster.nodes[to]
        .as_ref()
        .unwrap()
        .coordinator
        .on_peer(from, serde_json::to_value(message).unwrap())
        .await
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_writer_retags_and_late_offers_cannot_resurrect_a_value() {
    let mut c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    peer(
        &c,
        0,
        2,
        Message::Offer {
            key: "x".into(),
            id: tag(100, 2),
            value: Some(json!({"old":true})),
        },
    )
    .await;
    c.nodes[2].as_ref().unwrap().faults.set(2, LinkFault::Block);
    assert_eq!(
        http(c.address(2), "PUT", "/x", r#"{"new":true}"#)
            .await
            .status,
        201
    );
    assert_eq!(
        c.nodes[2].as_ref().unwrap().coordinator.diagnostics()["retags"],
        1
    );
    c.nodes[2].as_ref().unwrap().faults.clear();
    peer(
        &c,
        0,
        2,
        Message::Offer {
            key: "x".into(),
            id: tag(100, 2),
            value: Some(json!({"old":true})),
        },
    )
    .await;
    for i in 0..3 {
        assert_eq!(
            http(c.address(i), "GET", "/x", "").await.json(),
            json!({"new":true})
        );
    }
    for i in 0..3 {
        c.stop_node(i).await;
    }
    for i in 0..3 {
        c.restart_node(i).await;
    }
    for i in 0..3 {
        assert_eq!(
            http(c.address(i), "GET", "/x", "").await.json(),
            json!({"new":true})
        );
    }
    c.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_before_offer_is_repaired_without_retagging_the_wrong_value() {
    let c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    let id = tag(1, 2);
    let final_tag = tag(8, 2);
    let version = Version {
        key: "x".into(),
        id,
        tag: final_tag,
        value: Some(json!({"right":true})),
        installed: true,
        observed: Tag::default(),
    };
    assert_eq!(
        peer(
            &c,
            0,
            2,
            Message::Commit {
                key: "x".into(),
                id,
                tag: final_tag,
                value: None
            }
        )
        .await["kind"],
        "need_value"
    );
    assert_eq!(
        peer(
            &c,
            0,
            2,
            Message::Commit {
                key: "x".into(),
                id,
                tag: final_tag,
                value: Some(version.clone())
            }
        )
        .await["kind"],
        "committed"
    );
    peer(
        &c,
        0,
        2,
        Message::Offer {
            key: "x".into(),
            id,
            value: version.value.clone(),
        },
    )
    .await;
    assert_eq!(
        peer(&c, 0, 2, Message::Query { key: "x".into() }).await["data"],
        serde_json::to_value(final_tag).unwrap()
    );
    assert_eq!(
        http(c.address(0), "GET", "/x", "").await.json(),
        json!({"right":true})
    );
    c.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_writer_finishes_durable_installation_at_each_wal_boundary() {
    for point in [
        PausePoint::BeforeAppend,
        PausePoint::BeforeSync,
        PausePoint::AfterSync,
        PausePoint::BeforeAck,
    ] {
        let mut c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
        let node = c.nodes[0].as_ref().unwrap();
        let pause = node.store.faults.pause(point);
        let coordinator = node.coordinator.clone();
        let task = tokio::spawn(async move {
            coordinator
                .execute(Operation::Put {
                    key: "x".into(),
                    value: json!({"cancelled":true}),
                })
                .await
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
        tokio::time::timeout(Duration::from_secs(5), async {
            while node
                .metrics
                .work_pending
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        c.stop_node(0).await;
        c.restart_node(0).await;
        assert_eq!(
            http(c.address(0), "GET", "/x", "").await.json(),
            json!({"cancelled":true})
        );
        c.shutdown().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonowner_forwarding_and_many_changing_value_histories() {
    use distributed_kv_harness::history::{Recorder, Verdict, check};
    let c = Cluster::start(6, "gus", protocol::factory("gus").unwrap()).await;
    let key = c.key_for(1);
    assert!(
        c.nodes[0]
            .as_ref()
            .unwrap()
            .coordinator
            .on_peer(
                6,
                serde_json::to_value(Message::Offer {
                    key: key.clone(),
                    id: tag(1, 6),
                    value: Some(json!({"outside_group":true}))
                })
                .unwrap()
            )
            .await
            .is_err()
    );
    assert_eq!(
        http(
            c.address(5),
            "PUT",
            &format!("/{key}"),
            r#"{"nonowner":true}"#
        )
        .await
        .status,
        201
    );
    assert_eq!(
        http(c.address(3), "GET", &format!("/{key}"), "")
            .await
            .json(),
        json!({"nonowner":true})
    );
    for round in 0..50 {
        let recorder = std::sync::Arc::new(Recorder::new(24));
        let key = format!("changing-{round}");
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..12 {
            let recorder = recorder.clone();
            let coordinator = c.nodes[i % 3].as_ref().unwrap().coordinator.clone();
            let key = key.clone();
            tasks.spawn(async move {
                tokio::time::sleep(Duration::from_micros(((i * 97 + round * 41) % 1000) as u64))
                    .await;
                let operation = match i % 4 {
                    0 | 1 => Operation::Put {
                        key,
                        value: json!({"round":round,"writer":i}),
                    },
                    2 => Operation::Get { key },
                    _ => Operation::Delete { key },
                };
                let ticket = recorder.begin(operation.clone()).unwrap();
                let result = coordinator.execute(operation).await;
                assert!(result.is_ok(), "{result:?}");
                recorder.finish(ticket, result.ok()).unwrap();
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(check(&recorder.snapshot()), Verdict::Valid, "round {round}");
    }
    assert_eq!(
        c.nodes[0]
            .as_ref()
            .unwrap()
            .coordinator
            .execute(Operation::Get {
                key: "never".into()
            })
            .await
            .unwrap(),
        Outcome::Missing
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_recovery_cannot_promote_another_writers_unfinished_speculative_tag() {
    let c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    peer(
        &c,
        2,
        3,
        Message::Offer {
            key: "x".into(),
            id: tag(100, 3),
            value: Some(json!({"prior":true})),
        },
    )
    .await;
    let node = c.nodes[0].as_ref().unwrap();
    node.faults
        .set(2, LinkFault::Delay(Duration::from_millis(100)));
    c.nodes[1].as_ref().unwrap().faults.set(3, LinkFault::Block);
    let pause = node.store.faults.pause(PausePoint::BeforeAppend);
    let writer = tokio::spawn({
        let coordinator = node.coordinator.clone();
        async move {
            coordinator
                .execute(Operation::Put {
                    key: "x".into(),
                    value: json!({"final":true}),
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
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if peer(&c, 1, 1, Message::Query { key: "x".into() }).await["data"]["counter"]
                .as_u64()
                .unwrap()
                > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let reader = tokio::spawn({
        let coordinator = c.nodes[1].as_ref().unwrap().coordinator.clone();
        async move {
            coordinator
                .execute(Operation::Get { key: "x".into() })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !reader.is_finished(),
        "an unfinished proposal must not become readable by promoting it at its writer"
    );
    drop(pause);
    assert_eq!(writer.await.unwrap().unwrap(), Outcome::Written);
    assert_eq!(
        reader.await.unwrap().unwrap(),
        Outcome::Value(json!({"final":true}))
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reboot_does_not_reuse_an_unpublished_speculative_tag() {
    let mut c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    let old = tag(1, 1);
    peer(
        &c,
        1,
        1,
        Message::Offer {
            key: "x".into(),
            id: old,
            value: Some(json!({"old":true})),
        },
    )
    .await;
    c.stop_node(0).await;
    c.restart_node(0).await;
    assert_eq!(
        http(c.address(0), "PUT", "/x", r#"{"new":true}"#)
            .await
            .status,
        201
    );
    peer(
        &c,
        1,
        1,
        Message::Offer {
            key: "x".into(),
            id: old,
            value: Some(json!({"old":true})),
        },
    )
    .await;
    for i in 0..3 {
        assert_eq!(
            http(c.address(i), "GET", "/x", "").await.json(),
            json!({"new":true})
        );
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_loss_duplication_and_reordering_preserve_unknown_history() {
    use distributed_kv_harness::{
        ClusterConfig, CoordinatorContext, NodeConfig,
        history::{Recorder, Verdict, check},
        metrics::Metrics,
        simulation::{Network, Step},
        store::JournalStore,
    };
    use std::sync::{Arc, atomic::Ordering};
    let dir = common::TestDir::new();
    let network = Network::default();
    let config = Arc::new(ClusterConfig {
        cluster_id: "gus-schedule".into(),
        coordinator: "gus".into(),
        nodes: (1..=3)
            .map(|id| NodeConfig {
                id,
                http: format!("127.0.0.1:{}", 22000 + id * 2).parse().unwrap(),
                peer: format!("127.0.0.1:{}", 22001 + id * 2).parse().unwrap(),
            })
            .collect(),
        shards: vec![vec![1, 2, 3]],
        rpc_timeout_ms: 300,
        benchmark_peer_rtt_ms: 0,
    });
    let mut coordinators = Vec::new();
    let mut stores = Vec::new();
    let mut counters = Vec::new();
    for id in 1..=3 {
        let metrics = Arc::new(Metrics::default());
        let store = JournalStore::open(
            dir.0.join(id.to_string()),
            json!({"node":id}),
            metrics.clone(),
        )
        .unwrap();
        let c = protocol::factory("gus").unwrap()(CoordinatorContext {
            node_id: id,
            config: config.clone(),
            store: store.clone(),
            transport: network.transport(id),
            metrics: metrics.clone(),
        })
        .unwrap();
        c.initialize().await.unwrap();
        network.register(id, &c);
        coordinators.push(c);
        stores.push(store);
        counters.push(metrics);
    }
    let recorder = Arc::new(Recorder::new(24));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..8 {
        let c = coordinators[i % 2].clone();
        let recorder = recorder.clone();
        tasks.spawn(async move {
            let operation = match i % 3 {
                0 => Operation::Put {
                    key: "x".into(),
                    value: json!({"writer":i}),
                },
                1 => Operation::Get { key: "x".into() },
                _ => Operation::Delete { key: "x".into() },
            };
            let ticket = recorder.begin(operation.clone()).unwrap();
            let result = c.execute(operation).await;
            recorder.finish(ticket, result.ok()).unwrap();
        });
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut count = 0;
        loop {
            while let Some(result) = tasks.try_join_next() {
                result.unwrap();
            }
            if tasks.is_empty()
                && counters
                    .iter()
                    .all(|m| m.work_pending.load(Ordering::SeqCst) == 0)
            {
                break;
            }
            let pending = network.pending();
            if let Some((id, _, to, payload)) = pending.last() {
                network
                    .step(Step {
                        id: *id,
                        copies: if *to == 3 {
                            0
                        } else {
                            1 + usize::from(count % 3 == 0)
                        },
                        drop_reply: payload["kind"] == "offer" && count % 5 == 0,
                    })
                    .await
                    .unwrap();
                count += 1;
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        check(&recorder.snapshot()),
        Verdict::Valid,
        "{}",
        serde_json::to_string(&recorder.snapshot()).unwrap()
    );
    let trace = network.trace();
    assert!(trace.iter().any(|s| s.copies == 0));
    assert!(trace.iter().any(|s| s.copies == 2));
    assert_eq!(
        serde_json::from_slice::<Vec<Step>>(&serde_json::to_vec(&trace).unwrap()).unwrap(),
        trace
    );
    drop(coordinators);
    for store in stores {
        store.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_offer_after_read_copy_preserves_eligibility_and_original_acceptance() {
    let mut c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    let id = tag(1, 2);
    peer(
        &c,
        2,
        2,
        Message::Offer {
            key: "x".into(),
            id,
            value: Some(json!({"copied":true})),
        },
    )
    .await;
    let version = Version {
        key: "x".into(),
        id,
        tag: id,
        value: Some(json!({"copied":true})),
        installed: true,
        observed: Tag::default(),
    };
    peer(
        &c,
        0,
        3,
        Message::Repair {
            key: "x".into(),
            version,
        },
    )
    .await;
    let reply = peer(
        &c,
        0,
        2,
        Message::Offer {
            key: "x".into(),
            id,
            value: Some(json!({"copied":true})),
        },
    )
    .await;
    assert_eq!(
        reply["data"],
        serde_json::to_value(Tag::default()).unwrap(),
        "a delayed original offer must not turn prior acceptance into a stale ACK"
    );
    let records = c.nodes[0]
        .as_ref()
        .unwrap()
        .store
        .records("_gus/version/")
        .unwrap();
    assert!(
        records
            .into_iter()
            .map(|(_, v)| serde_json::from_value::<Version>(v).unwrap())
            .any(|v| v.tag == id && v.installed)
    );
    c.stop_node(0).await;
    c.restart_node(0).await;
    assert_eq!(
        http(c.address(0), "GET", "/x", "").await.json(),
        json!({"copied":true})
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_gus_storage_generation_fails_closed() {
    use distributed_kv_harness::{RunningNode, store::Record};
    let mut c = Cluster::start(3, "gus", protocol::factory("gus").unwrap()).await;
    c.nodes[0]
        .as_ref()
        .unwrap()
        .store
        .commit(vec![Record {
            key: "_gus/format".into(),
            state: json!(1),
        }])
        .await
        .unwrap();
    c.stop_node(0).await;
    let result = RunningNode::start(c.config.clone(), 1, c.directory.0.join("0")).await;
    assert!(
        matches!(result,Err(error) if error.message.contains("unsupported experimental Gus state"))
    );
    c.shutdown().await;
}
