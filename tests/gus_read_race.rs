mod common;
use distributed_kv_harness::{
    ClusterConfig, Coordinator, CoordinatorContext, NodeConfig, Operation, Outcome,
    gus::{Tag, Version},
    history::{Recorder, Verdict, check},
    metrics::Metrics,
    protocol,
    simulation::{Network, Step},
    store::{JournalStore, PausePoint},
};
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::task::JoinHandle;
async fn message(net: &Network, from: u64, to: u64, kinds: &[&str]) -> u64 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((id, _, _, _)) = net.pending().into_iter().find(|(_, f, t, v)| {
                *f == from && *t == to && kinds.iter().any(|k| v["kind"] == *k)
            }) {
                return id;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
async fn deliver(net: &Network, id: u64, drop_reply: bool) {
    net.step(Step {
        id,
        copies: 1,
        drop_reply,
    })
    .await
    .unwrap();
}
fn operation(c: Arc<dyn Coordinator>, rec: Arc<Recorder>, op: Operation) -> JoinHandle<Outcome> {
    let ticket = rec.begin(op.clone()).unwrap();
    tokio::spawn(async move {
        let result = c.execute(op).await.unwrap();
        rec.finish(ticket, Some(result.clone())).unwrap();
        result
    })
}
async fn pump<T>(net: &Network, task: &JoinHandle<T>, hold_offer: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !task.is_finished() {
            if let Some((id, _, _, _)) = net.pending().into_iter().find(|(_, from, to, v)| {
                !(hold_offer && *from == 3 && *to == 2 && v["kind"] == "offer")
            }) {
                deliver(net, id, false).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_read_transfer_cannot_make_one_write_visible_on_both_sides_of_a_retag() {
    let dir = common::TestDir::new();
    let net = Network::default();
    let config = Arc::new(ClusterConfig {
        cluster_id: "gus-read-race".into(),
        coordinator: "gus".into(),
        nodes: (1..=3)
            .map(|id| NodeConfig {
                id,
                http: format!("127.0.0.1:{}", 44000 + id * 2).parse().unwrap(),
                peer: format!("127.0.0.1:{}", 44001 + id * 2).parse().unwrap(),
            })
            .collect(),
        shards: vec![vec![1, 2, 3]],
        rpc_timeout_ms: 5000,
        benchmark_peer_rtt_ms: 0,
    });
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    let mut metrics = Vec::new();
    for id in 1..=3 {
        let counter = Arc::new(Metrics::default());
        let store = JournalStore::open(
            dir.0.join(id.to_string()),
            json!({"node":id}),
            counter.clone(),
        )
        .unwrap();
        let node = protocol::factory("gus").unwrap()(CoordinatorContext {
            node_id: id,
            config: config.clone(),
            store: store.clone(),
            transport: net.transport(id),
            metrics: counter.clone(),
        })
        .unwrap();
        node.initialize().await.unwrap();
        net.register(id, &node);
        nodes.push(node);
        stores.push(store);
        metrics.push(counter);
    }
    let history = Arc::new(Recorder::new(24));
    let a = operation(
        nodes[0].clone(),
        history.clone(),
        Operation::Put {
            key: "x".into(),
            value: json!({"v":"A"}),
        },
    );
    // B accepts A's speculative value, but A does not receive this ACK.
    deliver(&net, message(&net, 1, 2, &["offer"]).await, true).await;
    let read_b = operation(
        nodes[1].clone(),
        history.clone(),
        Operation::Get { key: "x".into() },
    );
    deliver(&net, message(&net, 2, 3, &["query"]).await, false).await;
    let stale_transfer = message(&net, 2, 3, &["commit", "repair"]).await;
    // C's later write X completes via A while B still holds the older speculative A.
    let x = operation(
        nodes[2].clone(),
        history.clone(),
        Operation::Put {
            key: "x".into(),
            value: json!({"v":"X"}),
        },
    );
    deliver(&net, message(&net, 3, 1, &["offer"]).await, false).await;
    assert_eq!(x.await.unwrap(), Outcome::Written);
    // Lose the other X offer; later view messages may arrive on a reconnected link.
    let lost = message(&net, 3, 2, &["offer"]).await;
    net.step(Step {
        id: lost,
        copies: 0,
        drop_reply: false,
    })
    .await
    .unwrap();
    let pause = stores[0].faults.pause(PausePoint::BeforeAppend);
    deliver(&net, message(&net, 1, 3, &["offer"]).await, false).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pause.reached() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A has now chosen its larger final tag, but has not persisted/broadcast it.
    // A read based on C's earlier query must not install the obsolete tag at C.
    deliver(&net, stale_transfer, false).await;
    let speculative = Tag {
        counter: 1,
        writer: 1,
        incarnation: 1,
    };
    let wrongly_installed = stores[2]
        .records("_gus/version/")
        .unwrap()
        .into_iter()
        .map(|(_, v)| serde_json::from_value::<Version>(v).unwrap())
        .any(|v| v.id == speculative && v.tag == speculative && v.installed);
    assert!(
        !wrongly_installed,
        "stale read repair promoted a rejected speculative tag after the receiver advanced"
    );
    pump(&net, &read_b, true).await;
    assert_eq!(read_b.await.unwrap(), Outcome::Value(json!({"v":"X"})));
    let read_x = operation(
        nodes[2].clone(),
        history.clone(),
        Operation::Get { key: "x".into() },
    );
    pump(&net, &read_x, true).await;
    assert_eq!(read_x.await.unwrap(), Outcome::Value(json!({"v":"X"})));
    drop(pause);
    pump(&net, &a, false).await;
    assert_eq!(a.await.unwrap(), Outcome::Written);
    let read_a = operation(
        nodes[1].clone(),
        history.clone(),
        Operation::Get { key: "x".into() },
    );
    pump(&net, &read_a, false).await;
    assert_eq!(read_a.await.unwrap(), Outcome::Value(json!({"v":"A"})));
    assert_eq!(
        check(&history.snapshot()),
        Verdict::Valid,
        "{}",
        serde_json::to_string(&history.snapshot()).unwrap()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while metrics
            .iter()
            .any(|m| m.work_pending.load(Ordering::SeqCst) > 0)
        {
            if let Some((id, _, _, _)) = net.pending().first() {
                deliver(&net, *id, false).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
    drop(nodes);
    for store in stores {
        store.close().await;
    }
}
