mod common;
use common::TestDir;
use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::JoinSet,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_loop_driver_accounts_for_overload_and_client_queue_time() {
    let dir = TestDir::new();
    let mut nodes = Vec::new();
    let mut listeners = Vec::new();
    let mut servers = JoinSet::new();
    let body = Arc::new(serde_json::to_vec(&json!({"v":"x".repeat(1016)})).unwrap());
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        nodes.push(json!({"id":id,"http":listener.local_addr().unwrap(),"peer":peer.local_addr().unwrap()}));
        listeners.push(peer);
        let body = body.clone();
        servers.spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let body = body.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).await.unwrap() == 0 {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let stream = reader.get_mut();
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(header.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
                });
            }
        });
    }
    let config = dir.0.join("config.json");
    let spec = dir.0.join("spec.json");
    let samples = dir.0.join("samples.jsonl");
    let summary = dir.0.join("summary.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"cluster_id":"driver-test","coordinator":"abd","nodes":nodes,"shards":[[1,2,3]],"rpc_timeout_ms":1000})).unwrap()).unwrap();
    std::fs::write(&spec,serde_json::to_vec(&json!({"config":config,"mode":"run","rate":1000,"seconds":0.1,"keys":1,"active_keys":1,"mutation_percent":0,"delete_percent":0,"payload_bytes":1024,"ingress":"random","workers":1,"seed":1})).unwrap()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_kv-bench"))
        .args([spec.as_os_str(), samples.as_os_str(), summary.as_os_str()])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if status.is_err() {
        child.kill().unwrap();
        let _ = child.wait();
    }
    assert!(status.unwrap().success());
    let result: Value = serde_json::from_slice(&std::fs::read(summary).unwrap()).unwrap();
    assert_eq!(result["offered"], 100);
    assert!(result["rejected"].as_u64().unwrap() > 50);
    assert_eq!(result["errors"], result["rejected"]);
    let raw = std::fs::read_to_string(samples).unwrap();
    assert_eq!(raw.lines().count(), 100);
    let mut saw_queue = false;
    for line in raw.lines() {
        let sample: Value = serde_json::from_str(line).unwrap();
        if let Some(dispatch) = sample["dispatched_ns"].as_u64() {
            let scheduled = sample["scheduled_ns"].as_u64().unwrap();
            assert!(dispatch >= scheduled && sample["completed_ns"].as_u64().unwrap() >= dispatch);
            saw_queue |= dispatch - scheduled > 20_000_000;
        }
    }
    assert!(saw_queue);
    servers.abort_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warmup_is_excluded_and_values_change_while_resources_drain() {
    use common::{Cluster, http};
    let cluster = Cluster::start(
        3,
        "abd",
        distributed_kv_harness::protocol::factory("abd").unwrap(),
    )
    .await;
    let dir = TestDir::new();
    let config = dir.0.join("config.json");
    let spec = dir.0.join("spec.json");
    let samples = dir.0.join("samples.jsonl");
    let summary = dir.0.join("summary.json");
    std::fs::write(&config, serde_json::to_vec(&*cluster.config).unwrap()).unwrap();
    std::fs::write(&spec,serde_json::to_vec(&json!({"config":config,"mode":"run","rate":100,"seconds":0.1,"warmup_seconds":0.1,"capture_resources":true,"run_id":42,"keys":1,"active_keys":1,"mutation_percent":100,"delete_percent":0,"payload_bytes":1024,"ingress":"owner","workers":1,"seed":1})).unwrap()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_kv-bench"))
        .args([spec.as_os_str(), samples.as_os_str(), summary.as_os_str()])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if status.is_err() {
        child.kill().unwrap();
        let _ = child.wait();
    }
    assert!(status.unwrap().success());
    let result: Value = serde_json::from_slice(&std::fs::read(summary).unwrap()).unwrap();
    assert_eq!(result["offered"], 10);
    assert_eq!(result["warmup_offered"], 10);
    assert_eq!(result["errors"], 0);
    assert_eq!(result["warmup_errors"], 0);
    assert_eq!(result["settled"], true);
    let mut requests = 0;
    for (a, b) in result["resources_before"]
        .as_array()
        .unwrap()
        .iter()
        .zip(result["resources_after"].as_array().unwrap())
    {
        requests += b["metrics"]["http_requests"].as_u64().unwrap()
            - a["metrics"]["http_requests"].as_u64().unwrap();
        assert_eq!(b["metrics"]["work_pending"], 0);
    }
    assert_eq!(requests, 10, "warmup must not enter the measured counters");
    let stored = http(cluster.address(0), "GET", "/bench-key-0", "")
        .await
        .json();
    let value = stored["v"].as_str().unwrap();
    assert!(value.starts_with("000000000000002a00000000000000020000000000000009"));
    assert_eq!(
        std::fs::read_to_string(samples).unwrap().lines().count(),
        10
    );
    cluster.shutdown().await;
}
