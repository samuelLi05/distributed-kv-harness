#![allow(dead_code)]
use distributed_kv_harness::{
    ClusterConfig, CoordinatorFactory, LocalCoordinator, NodeConfig, RunningNode,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
pub struct TestDir(pub PathBuf);
impl TestDir {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "kv-harness-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct Cluster {
    pub nodes: Vec<Option<RunningNode>>,
    pub config: Arc<ClusterConfig>,
    pub directory: TestDir,
}
impl Cluster {
    pub async fn local(count: usize) -> Self {
        Self::start(count, "local", LocalCoordinator::build).await
    }
    pub async fn start(count: usize, protocol: &str, factory: CoordinatorFactory) -> Self {
        let directory = TestDir::new();
        let mut configs = Vec::new();
        let mut listeners = Vec::new();
        for i in 0..count {
            let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
            configs.push(NodeConfig {
                id: i as u64 + 1,
                http: http.local_addr().unwrap(),
                peer: peer.local_addr().unwrap(),
            });
            listeners.push((http, peer));
        }
        let config = Arc::new(ClusterConfig {
            cluster_id: "integration-test-v1".into(),
            coordinator: protocol.into(),
            nodes: configs,
            shards: (0..count * 4)
                .map(|i| vec![(i % count) as u64 + 1])
                .collect(),
            rpc_timeout_ms: 2000,
        });
        let mut nodes = Vec::new();
        for (i, (http, peer)) in listeners.into_iter().enumerate() {
            nodes.push(Some(
                RunningNode::with_listeners(
                    config.clone(),
                    i as u64 + 1,
                    directory.0.join(i.to_string()),
                    http,
                    peer,
                    factory,
                )
                .await
                .unwrap(),
            ));
        }
        Self {
            nodes,
            config,
            directory,
        }
    }
    pub fn address(&self, index: usize) -> std::net::SocketAddr {
        self.config.nodes[index].http
    }
    pub fn key_for(&self, node_id: u64) -> String {
        (0..10000)
            .map(|i| format!("key-{i}"))
            .find(|key| self.config.replicas(key)[0] == node_id)
            .unwrap()
    }
    pub async fn stop_node(&mut self, index: usize) {
        self.nodes[index].take().unwrap().shutdown().await.unwrap();
    }
    pub async fn restart_node(&mut self, index: usize) {
        self.nodes[index] = Some(
            RunningNode::start(
                self.config.clone(),
                index as u64 + 1,
                self.directory.0.join(index.to_string()),
            )
            .await
            .unwrap(),
        );
    }
    pub async fn shutdown(mut self) {
        for node in &mut self.nodes {
            if let Some(node) = node.take() {
                node.shutdown().await.unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}
impl Response {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

pub async fn http(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> Response {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new(); stream.read_to_end(&mut bytes).await.unwrap();
        let end = bytes.windows(4).position(|w| w == b"\r\n\r\n").expect("HTTP header");
        let header = std::str::from_utf8(&bytes[..end]).unwrap();
        assert!(!header.to_lowercase().contains("transfer-encoding: chunked"), "test helper expects a fixed-size Axum response");
        Response { status: header.split_whitespace().nth(1).unwrap().parse().unwrap(), body: bytes[end + 4..].to_vec() }
    }).await.expect("HTTP request exceeded test deadline")
}
