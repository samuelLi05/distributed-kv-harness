//! Scheduled open-loop load generator. Python stdlib orchestration records server resources.
use distributed_kv_harness::{ClusterConfig, http_client::Client};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::{BufWriter, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    config: String,
    mode: String,
    rate: u64,
    seconds: f64,
    keys: usize,
    active_keys: usize,
    mutation_percent: u64,
    delete_percent: u64,
    payload_bytes: usize,
    ingress: String,
    workers: usize,
    seed: u64,
}
#[derive(Clone)]
struct Work {
    seq: u64,
    key: usize,
    node: SocketAddr,
    method: &'static str,
    scheduled_ns: u64,
}
#[derive(Serialize)]
struct Sample {
    seq: u64,
    key: usize,
    method: &'static str,
    scheduled_ns: u64,
    dispatched_ns: Option<u64>,
    completed_ns: u64,
    status: u16,
    error: Option<String>,
}
fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}
fn percentile(values: &mut [u64], p: usize) -> u64 {
    values.sort_unstable();
    if values.is_empty() {
        0
    } else {
        values[(values.len() - 1) * p / 100]
    }
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: kv-bench SPEC.json SAMPLES.jsonl SUMMARY.json".into());
    }
    let spec: Spec = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    if !matches!(spec.mode.as_str(), "preload" | "run")
        || !matches!(spec.ingress.as_str(), "owner" | "random")
        || spec.workers == 0
        || spec.workers > 1024
        || spec.keys == 0
        || spec.active_keys == 0
        || spec.active_keys > spec.keys
        || spec.mutation_percent > 100
        || spec.delete_percent > 100
        || spec.payload_bytes < 16
        || spec.payload_bytes > 1024 * 1024
        || spec.rate == 0
        || !spec.seconds.is_finite()
        || spec.seconds <= 0.0
        || spec.seconds > 3600.0
    {
        return Err("invalid benchmark specification".into());
    }
    let config = Arc::new(ClusterConfig::load(&spec.config)?);
    if config.shards.iter().any(|r| r.len() != 3) {
        return Err("benchmark requires RF=3".into());
    }
    let count = if spec.mode == "preload" {
        spec.keys as u64
    } else {
        (spec.rate as f64 * spec.seconds).floor() as u64
    };
    if count == 0 || count > 2_000_000 {
        return Err("benchmark requires 1..=2,000,000 arrivals".into());
    }
    // Fixed serialized object size; payload construction is outside the measured interval.
    let body = Arc::new(serde_json::to_vec(
        &json!({"v":"x".repeat(spec.payload_bytes-8)}),
    )?);
    assert_eq!(body.len(), spec.payload_bytes);
    let (work_tx, work_rx) = mpsc::channel::<Work>(spec.workers * 2);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let (result_tx, mut result_rx) = mpsc::channel::<Sample>(spec.workers * 4);
    let collector = tokio::spawn(async move {
        let mut samples = Vec::new();
        while let Some(sample) = result_rx.recv().await {
            samples.push(sample);
        }
        samples
    });
    let usage_before = distributed_kv_harness::resources::usage();
    let start = Instant::now();
    let mut workers = JoinSet::new();
    for _ in 0..spec.workers {
        let rx = work_rx.clone();
        let results = result_tx.clone();
        let body = body.clone();
        let allow_missing = spec.delete_percent > 0;
        workers.spawn(async move {
            let mut clients = BTreeMap::new();
            loop {
                let Some(work) = rx.lock().await.recv().await else {
                    break;
                };
                let dispatched_ns = ns(start);
                let client = clients
                    .entry(work.node)
                    .or_insert_with(|| Client::new(work.node));
                let result = timeout(
                    Duration::from_secs(5),
                    client.request(
                        work.method,
                        &format!("/bench-key-{}", work.key),
                        if work.method == "PUT" { &body } else { &[] },
                    ),
                )
                .await;
                let (status, error) = match result {
                    Ok(Ok((status, data))) => {
                        let valid = match work.method {
                            "PUT" => status == 201,
                            "DELETE" => status == 204,
                            _ => {
                                (status == 404 && allow_missing) || (status == 200 && data == *body)
                            }
                        };
                        (
                            status,
                            if valid {
                                None
                            } else {
                                Some(format!(
                                    "invalid response: {}",
                                    String::from_utf8_lossy(&data[..data.len().min(256)])
                                ))
                            },
                        )
                    }
                    Ok(Err(error)) => (0, Some(error.to_string())),
                    Err(_) => {
                        clients.remove(&work.node);
                        (0, Some("client timeout; mutation outcome unknown".into()))
                    }
                };
                if results
                    .send(Sample {
                        seq: work.seq,
                        key: work.key,
                        method: work.method,
                        scheduled_ns: work.scheduled_ns,
                        dispatched_ns: Some(dispatched_ns),
                        completed_ns: ns(start),
                        status,
                        error,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
    }
    let mut seed = spec.seed.max(1);
    for seq in 0..count {
        let scheduled_ns = if spec.mode == "preload" {
            ns(start)
        } else {
            (u128::from(seq) * 1_000_000_000 / u128::from(spec.rate)) as u64
        };
        if spec.mode == "run" {
            sleep_until(start + Duration::from_nanos(scheduled_ns)).await;
        }
        let key = if spec.mode == "preload" {
            seq as usize
        } else {
            random(&mut seed) as usize % spec.active_keys
        };
        let key_name = format!("bench-key-{key}");
        let node = if spec.ingress == "owner" {
            config
                .node(config.replicas(&key_name)[random(&mut seed) as usize % 3])?
                .http
        } else {
            config.nodes[random(&mut seed) as usize % config.nodes.len()].http
        };
        let method = if spec.mode == "preload" || random(&mut seed) % 100 < spec.mutation_percent {
            if spec.mode != "preload" && random(&mut seed) % 100 < spec.delete_percent {
                "DELETE"
            } else {
                "PUT"
            }
        } else {
            "GET"
        };
        let work = Work {
            seq,
            key,
            node,
            method,
            scheduled_ns,
        };
        if spec.mode == "preload" {
            work_tx.send(work).await?;
        } else if let Err(error) = work_tx.try_send(work) {
            let work = error.into_inner();
            result_tx
                .send(Sample {
                    seq,
                    key,
                    method,
                    scheduled_ns: work.scheduled_ns,
                    dispatched_ns: None,
                    completed_ns: ns(start),
                    status: 0,
                    error: Some("load generator queue full".into()),
                })
                .await?;
        }
    }
    drop(work_tx);
    drop(result_tx);
    while let Some(result) = workers.join_next().await {
        result?;
    }
    let mut samples = collector.await?;
    let elapsed = start.elapsed().as_secs_f64();
    let usage_after = distributed_kv_harness::resources::usage();
    samples.sort_by_key(|s| s.seq);
    assert_eq!(samples.len(), count as usize);
    let mut groups = BTreeMap::<&str, Vec<&Sample>>::new();
    for sample in &samples {
        groups.entry(sample.method).or_default().push(sample);
    }
    let mut operations = BTreeMap::new();
    for (method, group) in groups {
        let mut service = group
            .iter()
            .filter_map(|s| s.dispatched_ns.map(|d| s.completed_ns - d))
            .collect::<Vec<_>>();
        let mut scheduled = group
            .iter()
            .map(|s| s.completed_ns - s.scheduled_ns)
            .collect::<Vec<_>>();
        let mut queue = group
            .iter()
            .filter_map(|s| s.dispatched_ns.map(|d| d - s.scheduled_ns))
            .collect::<Vec<_>>();
        operations.insert(method,json!({"offered":group.len(),"errors":group.iter().filter(|s|s.error.is_some()).count(),"service_ns":{"p50":percentile(&mut service,50),"p95":percentile(&mut service,95),"p99":percentile(&mut service,99)},"scheduled_ns":{"p50":percentile(&mut scheduled,50),"p95":percentile(&mut scheduled,95),"p99":percentile(&mut scheduled,99)},"queue_ns":{"p50":percentile(&mut queue,50),"p99":percentile(&mut queue,99)}}));
    }
    let duration = if spec.mode == "preload" {
        elapsed
    } else {
        spec.seconds
    };
    let summary = json!({"spec":spec,"elapsed_seconds":elapsed,"window_seconds":duration,"offered":count,
        "successes":samples.iter().filter(|s|s.error.is_none()).count(),"errors":samples.iter().filter(|s|s.error.is_some()).count(),
        "rejected":samples.iter().filter(|s|s.dispatched_ns.is_none()).count(),
        "successful_completions_in_window":samples.iter().filter(|s|s.error.is_none() && s.completed_ns as f64<=duration*1e9).count(),
        "driver_cpu_seconds":usage_before.zip(usage_after).map(|(a,b)|b.cpu_seconds-a.cpu_seconds),
        "driver_peak_rss_bytes":usage_after.map(|u|u.peak_rss_bytes),
        "operations":operations});
    let mut output = BufWriter::new(std::fs::File::create(&args[2])?);
    for sample in samples {
        serde_json::to_writer(&mut output, &sample)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    std::fs::write(&args[3], serde_json::to_vec_pretty(&summary)?)?;
    println!("{}", serde_json::to_string(&summary)?);
    if summary["errors"].as_u64().unwrap() > 0 && summary["spec"]["mode"] == "preload" {
        return Err("preload failed".into());
    }
    Ok(())
}
