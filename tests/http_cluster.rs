mod common;
use common::{Cluster, http};
use distributed_kv_harness::transport::LinkFault;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn required_http_contract_and_object_validation() {
    let cluster = Cluster::local(1).await;
    let address = cluster.address(0);
    assert_eq!(http(address, "GET", "/missing", "").await.status, 404);
    assert_eq!(
        http(
            address,
            "PUT",
            "/example",
            r#"{"nested":{"ok":true},"items":[1,"two"]}"#
        )
        .await
        .status,
        201
    );
    assert_eq!(
        http(address, "GET", "/example", "").await.json(),
        json!({"nested":{"ok":true},"items":[1,"two"]})
    );
    for invalid in ["[]", "null", "42", "true", "\"text\"", "{broken"] {
        assert_eq!(http(address, "PUT", "/example", invalid).await.status, 400);
    }
    assert_eq!(
        http(address, "PUT", "/example", r#"{"replacement":1}"#)
            .await
            .status,
        201
    );
    assert_eq!(
        http(address, "GET", "/example", "").await.json(),
        json!({"replacement":1})
    );
    assert_eq!(
        http(address, "PUT", "/space%20key", r#"{"v":1}"#)
            .await
            .status,
        201
    );
    assert_eq!(
        http(address, "GET", "/space%20key", "").await.json(),
        json!({"v":1})
    );
    assert_eq!(http(address, "DELETE", "/example", "").await.status, 204);
    assert_eq!(http(address, "GET", "/example", "").await.status, 404);
    assert_eq!(http(address, "DELETE", "/missing", "").await.status, 204);
    let oversized = json!({"data": "x".repeat(1024 * 1024)}).to_string();
    assert_eq!(http(address, "PUT", "/big", &oversized).await.status, 413);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_ingress_routes_to_each_owner_and_concurrent_requests_complete() {
    let cluster = Cluster::local(3).await;
    for owner in 1..=3 {
        let key = cluster.key_for(owner);
        assert_eq!(
            http(
                cluster.address(0),
                "PUT",
                &format!("/{key}"),
                &json!({"owner":owner}).to_string()
            )
            .await
            .status,
            201
        );
        for ingress in 0..3 {
            assert_eq!(
                http(cluster.address(ingress), "GET", &format!("/{key}"), "")
                    .await
                    .json(),
                json!({"owner":owner})
            );
        }
    }
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..48 {
        let address = cluster.address(i % 3);
        tasks.spawn(async move {
            let path = format!("/parallel-{i}");
            assert_eq!(
                http(address, "PUT", &path, &json!({"index":i}).to_string())
                    .await
                    .status,
                201
            );
            (path, i)
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (path, i) = result.unwrap();
        assert_eq!(
            http(cluster.address((i + 1) % 3), "GET", &path, "")
                .await
                .json(),
            json!({"index":i})
        );
    }
    let metrics = cluster.nodes[0].as_ref().unwrap().metrics.snapshot();
    assert!(metrics.forwarded > 0);
    assert!(
        metrics.peer_connections <= 2,
        "connections should be reused, not opened per request"
    );
    assert!(metrics.rpc_requests_sent > metrics.peer_connections);
    cluster.shutdown().await;
}

#[tokio::test]
async fn failed_owner_returns_unavailable_and_restart_recovers_values_and_deletes() {
    let mut cluster = Cluster::local(3).await;
    let key = cluster.key_for(2);
    let deleted = (0..10000)
        .map(|i| format!("deleted-{i}"))
        .find(|k| cluster.config.replicas(k)[0] == 2)
        .unwrap();
    assert_eq!(
        http(
            cluster.address(0),
            "PUT",
            &format!("/{key}"),
            r#"{"durable":true}"#
        )
        .await
        .status,
        201
    );
    assert_eq!(
        http(cluster.address(0), "PUT", &format!("/{deleted}"), "{}")
            .await
            .status,
        201
    );
    assert_eq!(
        http(cluster.address(0), "DELETE", &format!("/{deleted}"), "")
            .await
            .status,
        204
    );
    cluster.stop_node(1).await;
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{key}"), "")
            .await
            .status,
        503
    );
    let healthy_key = cluster.key_for(1);
    assert_eq!(
        http(cluster.address(0), "PUT", &format!("/{healthy_key}"), "{}")
            .await
            .status,
        201
    );
    cluster.restart_node(1).await;
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{key}"), "")
            .await
            .json(),
        json!({"durable":true})
    );
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{deleted}"), "")
            .await
            .status,
        404
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn faults_cover_partition_latency_and_unknown_write_outcome() {
    let cluster = Cluster::local(2).await;
    let key = cluster.key_for(2);
    let faults = cluster.nodes[0].as_ref().unwrap().faults.clone();
    faults.set(2, LinkFault::Block);
    assert_eq!(
        http(cluster.address(0), "PUT", &format!("/{key}"), "{}")
            .await
            .status,
        503
    );
    faults.set(2, LinkFault::Delay(Duration::from_secs(3)));
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{key}"), "")
            .await
            .status,
        503
    );
    faults.set(2, LinkFault::DropResponse);
    assert_eq!(
        http(
            cluster.address(0),
            "PUT",
            &format!("/{key}"),
            r#"{"applied":true}"#
        )
        .await
        .status,
        503
    );
    faults.clear();
    assert_eq!(
        http(cluster.address(0), "GET", &format!("/{key}"), "")
            .await
            .json(),
        json!({"applied":true})
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn local_coordinator_rejects_a_fake_replication_configuration() {
    let mut cluster = Cluster::local(2).await;
    cluster.stop_node(0).await;
    let mut config = (*cluster.config).clone();
    config.shards = vec![vec![1, 2]];
    let node = distributed_kv_harness::RunningNode::start(
        std::sync::Arc::new(config),
        1,
        cluster.directory.0.join("replicated"),
    )
    .await;
    assert!(node.is_err());
    cluster.shutdown().await;
}
