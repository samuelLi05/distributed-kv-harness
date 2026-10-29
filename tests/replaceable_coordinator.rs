mod common;
use common::{Cluster, http};
use distributed_kv_harness::{
    Coordinator, CoordinatorContext, KvError, Operation, Outcome,
    metrics::Metrics,
    transport::{TcpTransport, Transport},
    types::{BoxFuture, NodeId},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Notify;

// A different protocol supplied through the public factory seam. Its held response proves RPC
// multiplexing without a fragile latency threshold or modification to the HTTP/store/transport.
struct Probe {
    context: CoordinatorContext,
    started: Notify,
    release: Notify,
}
fn build_probe(context: CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError> {
    Ok(Arc::new(Probe {
        context,
        started: Notify::new(),
        release: Notify::new(),
    }))
}
impl Coordinator for Probe {
    fn name(&self) -> &'static str {
        "probe"
    }
    fn execute(&self, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        Box::pin(async move {
            let destination = self
                .context
                .config
                .nodes
                .iter()
                .find(|n| n.id != self.context.node_id)
                .unwrap()
                .id;
            let value = self
                .context
                .transport
                .request(destination, json!({"command":operation.key()}))
                .await?;
            Ok(Outcome::Value(value))
        })
    }
    fn on_peer(&self, from: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            let command = payload["command"].as_str().unwrap_or("");
            match command {
                "hold" => {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                "wait_started" => self.started.notified().await,
                "release" => self.release.notify_one(),
                _ => (),
            }
            Ok(json!({"command":command,"from":from,"node":self.context.node_id}))
        })
    }
}

#[tokio::test]
async fn replacement_coordinator_and_out_of_order_replies_work_end_to_end() {
    let cluster = Cluster::start(2, "probe", build_probe).await;
    let address = cluster.address(0);
    let held = tokio::spawn(async move { http(address, "GET", "/hold", "").await });
    assert_eq!(http(address, "GET", "/wait_started", "").await.status, 200);
    assert!(!held.is_finished());
    assert_eq!(
        http(address, "GET", "/echo", "").await.json(),
        json!({"command":"echo","from":1,"node":2})
    );
    assert!(
        !held.is_finished(),
        "the echo reply must arrive while the first request remains pending"
    );
    assert_eq!(http(address, "GET", "/release", "").await.status, 200);
    assert_eq!(held.await.unwrap().status, 200);
    assert_eq!(
        cluster.nodes[0]
            .as_ref()
            .unwrap()
            .metrics
            .snapshot()
            .peer_connections,
        1
    );
    cluster.shutdown().await;
}

#[tokio::test]
async fn peer_handshake_rejects_conflicting_placement_configuration() {
    let cluster = Cluster::local(2).await;
    let mut wrong = (*cluster.config).clone();
    wrong.shards.reverse();
    let transport = TcpTransport::new(Arc::new(wrong), 1, Arc::new(Metrics::default()));
    let error = transport
        .request(2, json!({"op":"get","key":"missing"}))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        distributed_kv_harness::types::ErrorKind::Protocol
    );
    cluster.shutdown().await;
}
