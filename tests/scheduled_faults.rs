mod common;
use common::{Cluster, TestDir};
use distributed_kv_harness::{
    AbdCoordinator, ClusterConfig, Coordinator, CoordinatorContext, NodeConfig, Operation, Outcome,
    abd::{Message, Register, Tag},
    metrics::Metrics,
    simulation::{Network, Step},
    store::{JournalStore, PausePoint},
    types::{BoxFuture, KvError},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

struct Echo;
impl Coordinator for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }
    fn execute(&self, _: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        Box::pin(async { Ok(Outcome::Missing) })
    }
    fn on_peer(&self, _: u64, v: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move { Ok(v) })
    }
}
async fn wait_pending(network: &Network, count: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while network.pending().len() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn saved_message_schedule_replays_loss_duplication_and_reordering() {
    let script = vec![
        Step {
            id: 1,
            copies: 2,
            drop_reply: false,
        },
        Step {
            id: 0,
            copies: 0,
            drop_reply: false,
        },
    ];
    let saved = serde_json::to_string(&script).unwrap();
    for _ in 0..2 {
        let network = Network::default();
        let peer: Arc<dyn Coordinator> = Arc::new(Echo);
        network.register(2, &peer);
        let endpoint = network.transport(1);
        let first = tokio::spawn({
            let endpoint = endpoint.clone();
            async move { endpoint.request(2, json!({"n":1})).await }
        });
        wait_pending(&network, 1).await;
        let second = tokio::spawn(async move { endpoint.request(2, json!({"n":2})).await });
        wait_pending(&network, 2).await;
        for step in serde_json::from_str::<Vec<Step>>(&saved).unwrap() {
            network.step(step).await.unwrap();
        }
        assert!(first.await.unwrap().is_err());
        assert_eq!(second.await.unwrap().unwrap(), json!({"n":2}));
        assert_eq!(network.trace(), script);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_survives_cancellation_at_each_wal_boundary() {
    let cluster = Cluster::start(3, "abd", AbdCoordinator::build).await;
    for (i, point) in [
        PausePoint::BeforeAppend,
        PausePoint::BeforeSync,
        PausePoint::AfterSync,
        PausePoint::BeforeAck,
    ]
    .into_iter()
    .enumerate()
    {
        let node = cluster.nodes[0].as_ref().unwrap();
        let key = format!("cancel-{i}");
        let state = Register {
            tag: Tag {
                counter: 100,
                writer: 1,
                incarnation: 1,
            },
            value: None,
        };
        let pause = node.store.faults.pause(point);
        let high = tokio::spawn({
            let c = node.coordinator.clone();
            let key = key.clone();
            let state = state.clone();
            async move {
                c.on_peer(
                    2,
                    serde_json::to_value(Message::Store { key, state }).unwrap(),
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !pause.reached() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        high.abort();
        let _ = high.await;
        let low = tokio::spawn({
            let c = node.coordinator.clone();
            let key = key.clone();
            async move {
                c.on_peer(
                    2,
                    serde_json::to_value(Message::Store {
                        key,
                        state: Register {
                            tag: Tag {
                                counter: 2,
                                writer: 1,
                                incarnation: 1,
                            },
                            value: Some(json!({"stale":true})),
                        },
                    })
                    .unwrap(),
                )
                .await
            }
        });
        drop(pause);
        low.await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_value::<Register>(
                node.store
                    .get(&format!("_abd/data/{key}"))
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            state
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_write_with_lost_acks_is_recovered_by_read_repair() {
    let network = Network::default();
    let dir = TestDir::new();
    let config = Arc::new(ClusterConfig {
        cluster_id: "simulation".into(),
        coordinator: "abd".into(),
        nodes: (1..=3)
            .map(|id| NodeConfig {
                id,
                http: format!("127.0.0.1:{}", 20000 + id * 2).parse().unwrap(),
                peer: format!("127.0.0.1:{}", 20001 + id * 2).parse().unwrap(),
            })
            .collect(),
        shards: vec![vec![1, 2, 3]],
        rpc_timeout_ms: 3000,
    });
    let mut coordinators = Vec::new();
    let mut stores = Vec::new();
    for node in 1..=3 {
        let metrics = Arc::new(Metrics::default());
        let store = JournalStore::open(
            dir.0.join(node.to_string()),
            json!({"node":node}),
            metrics.clone(),
        )
        .unwrap();
        let c = AbdCoordinator::build(CoordinatorContext {
            node_id: node,
            config: config.clone(),
            store: store.clone(),
            transport: network.transport(node),
            metrics,
        })
        .unwrap();
        c.initialize().await.unwrap();
        network.register(node, &c);
        coordinators.push(c);
        stores.push(store);
    }
    let writer = tokio::spawn({
        let c = coordinators[0].clone();
        async move {
            c.execute(Operation::Put {
                key: "x".into(),
                value: json!({"unknown":true}),
            })
            .await
        }
    });
    wait_pending(&network, 2).await;
    let query = network
        .pending()
        .into_iter()
        .find(|(_, _, to, _)| *to == 2)
        .unwrap()
        .0;
    network
        .step(Step {
            id: query,
            copies: 1,
            drop_reply: false,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let deliveries = network
                .pending()
                .into_iter()
                .filter(|(_, _, _, v)| v["kind"] == "store")
                .collect::<Vec<_>>();
            if deliveries.len() == 2 {
                while stores[0].get("_abd/data/x").unwrap().is_none() {
                    tokio::task::yield_now().await;
                }
                for (id, _, to, _) in deliveries {
                    network
                        .step(Step {
                            id,
                            copies: if to == 2 { 2 } else { 0 },
                            drop_reply: true,
                        })
                        .await
                        .unwrap();
                }
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(writer.await.unwrap().is_err());
    let reader = tokio::spawn({
        let c = coordinators[2].clone();
        async move { c.execute(Operation::Get { key: "x".into() }).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !reader.is_finished() {
            if let Some((id, _, _, _)) = network.pending().first().cloned() {
                network
                    .step(Step {
                        id,
                        copies: 1,
                        drop_reply: false,
                    })
                    .await
                    .unwrap();
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reader.await.unwrap().unwrap(),
        Outcome::Value(json!({"unknown":true}))
    );
    drop(coordinators);
    for store in stores {
        store.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_nonowner_never_reuses_an_unpublished_proposal_tag() {
    use distributed_kv_harness::history::{Recorder, Verdict, check};
    let network = Network::default();
    let dir = TestDir::new();
    let config = Arc::new(ClusterConfig {
        cluster_id: "incarnation-test".into(),
        coordinator: "abd".into(),
        nodes: (1..=4)
            .map(|id| NodeConfig {
                id,
                http: format!("127.0.0.1:{}", 24000 + id * 2).parse().unwrap(),
                peer: format!("127.0.0.1:{}", 24001 + id * 2).parse().unwrap(),
            })
            .collect(),
        shards: vec![vec![1, 2, 3]],
        rpc_timeout_ms: 3000,
    });
    let mut coordinators = Vec::new();
    let mut stores = Vec::new();
    for node in 1..=4 {
        let metrics = Arc::new(Metrics::default());
        let store = JournalStore::open(
            dir.0.join(node.to_string()),
            json!({"node":node}),
            metrics.clone(),
        )
        .unwrap();
        let coordinator = AbdCoordinator::build(CoordinatorContext {
            node_id: node,
            config: config.clone(),
            store: store.clone(),
            transport: network.transport(node),
            metrics,
        })
        .unwrap();
        coordinator.initialize().await.unwrap();
        network.register(node, &coordinator);
        coordinators.push(coordinator);
        stores.push(store);
    }
    let recorder = Recorder::new(3);
    let old_operation = Operation::Put {
        key: "x".into(),
        value: json!({"v":1}),
    };
    let ticket = recorder.begin(old_operation.clone()).unwrap();
    let old = tokio::spawn({
        let c = coordinators[3].clone();
        async move { c.execute(old_operation).await }
    });
    wait_pending(&network, 3).await;
    for (id, _, _, _) in network.pending().into_iter().take(2) {
        network
            .step(Step {
                id,
                copies: 1,
                drop_reply: false,
            })
            .await
            .unwrap();
    }
    let proposals = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let proposals: Vec<_> = network
                .pending()
                .into_iter()
                .filter(|(_, _, _, v)| v["kind"] == "store")
                .collect();
            if proposals.len() == 3 {
                break proposals;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let first_tag = proposals[0].3["state"]["tag"].clone();
    let boundary = network.pending().iter().map(|p| p.0).max().unwrap();
    old.abort();
    let _ = old.await;
    recorder.finish(ticket, None).unwrap();
    drop(coordinators.pop().unwrap());
    stores.pop().unwrap().close().await;
    let metrics = Arc::new(Metrics::default());
    let store = JournalStore::open(dir.0.join("4"), json!({"node":4}), metrics.clone()).unwrap();
    let coordinator = AbdCoordinator::build(CoordinatorContext {
        node_id: 4,
        config: config.clone(),
        store: store.clone(),
        transport: network.transport(4),
        metrics,
    })
    .unwrap();
    coordinator.initialize().await.unwrap();
    network.register(4, &coordinator);
    let operation = Operation::Put {
        key: "x".into(),
        value: json!({"v":2}),
    };
    let ticket = recorder.begin(operation.clone()).unwrap();
    let writer = tokio::spawn({
        let c = coordinator.clone();
        async move { c.execute(operation).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let queries: Vec<_> = network
                .pending()
                .into_iter()
                .filter(|(id, _, _, v)| *id > boundary && v["kind"] == "query_tag")
                .collect();
            if queries.len() == 3 {
                for (id, _, _, _) in queries.into_iter().take(2) {
                    network
                        .step(Step {
                            id,
                            copies: 1,
                            drop_reply: false,
                        })
                        .await
                        .unwrap();
                }
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let proposals: Vec<_> = network
                .pending()
                .into_iter()
                .filter(|(id, _, _, v)| *id > boundary && v["kind"] == "store")
                .collect();
            if proposals.len() == 3 {
                assert_ne!(proposals[0].3["state"]["tag"], first_tag);
                assert_eq!(
                    proposals[0].3["state"]["tag"]["counter"],
                    first_tag["counter"]
                );
                assert_eq!(proposals[0].3["state"]["tag"]["incarnation"], 2);
                for (id, _, _, _) in proposals {
                    network
                        .step(Step {
                            id,
                            copies: 1,
                            drop_reply: false,
                        })
                        .await
                        .unwrap();
                }
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(writer.await.unwrap().unwrap(), Outcome::Written);
    recorder.finish(ticket, Some(Outcome::Written)).unwrap();
    for (id, _, _, _) in proposals {
        network
            .step(Step {
                id,
                copies: 2,
                drop_reply: false,
            })
            .await
            .unwrap();
    }
    let ticket = recorder.begin(Operation::Get { key: "x".into() }).unwrap();
    let reader = tokio::spawn({
        let c = coordinator.clone();
        async move { c.execute(Operation::Get { key: "x".into() }).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !reader.is_finished() {
            if let Some((id, _, _, _)) = network.pending().first().cloned() {
                network
                    .step(Step {
                        id,
                        copies: 1,
                        drop_reply: false,
                    })
                    .await
                    .unwrap();
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
    let outcome = reader.await.unwrap().unwrap();
    assert_eq!(outcome, Outcome::Value(json!({"v":2})));
    recorder.finish(ticket, Some(outcome)).unwrap();
    assert_eq!(check(&recorder.snapshot()), Verdict::Valid);
    assert!(recorder.begin(Operation::Get { key: "x".into() }).is_err());
    drop(coordinator);
    drop(coordinators);
    for store in stores {
        store.close().await;
    }
    store.close().await;
}
