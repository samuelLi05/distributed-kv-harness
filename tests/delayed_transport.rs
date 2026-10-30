mod common;
use distributed_kv_harness::{Operation, Outcome, protocol};
use std::time::{Duration, Instant};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_rtt_delays_requests_without_serializing_inflight_operations() {
    let c = common::Cluster::start_delayed(3, "abd", protocol::factory("abd").unwrap(), 20).await;
    let coordinator = c.nodes[0].as_ref().unwrap().coordinator.clone();
    let first = Instant::now();
    assert_eq!(
        coordinator
            .execute(Operation::Get { key: "x".into() })
            .await
            .unwrap(),
        Outcome::Missing
    );
    assert!(first.elapsed() >= Duration::from_millis(18));
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let c = coordinator.clone();
        tasks.spawn(async move { c.execute(Operation::Get { key: "x".into() }).await.unwrap() });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), Outcome::Missing);
    }
    assert!(
        start.elapsed() < Duration::from_millis(180),
        "per-frame blocking delays would serialize this workload: {:?}",
        start.elapsed()
    );
    c.shutdown().await;
}
