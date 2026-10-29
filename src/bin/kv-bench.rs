//! Open-loop arrivals with changing values, connection-preserving warmup and causal-work drain.
use distributed_kv_harness::{ClusterConfig, http_client::Client};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufWriter, Write},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
    #[serde(default)]
    warmup_seconds: f64,
    #[serde(default)]
    capture_resources: bool,
    #[serde(default)]
    run_id: u64,
}
struct Work {
    seq: u64,
    key: usize,
    node: SocketAddr,
    method: &'static str,
    scheduled_ns: u64,
    origin: Instant,
    phase: &'static str,
}
#[derive(Serialize)]
struct Sample {
    seq: u64,
    key: usize,
    method: &'static str,
    phase: &'static str,
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
async fn resources(config: &ClusterConfig) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let mut result = Vec::new();
    for node in &config.nodes {
        let (status, body) = timeout(
            Duration::from_secs(10),
            Client::new(node.http).request("GET", "/__harness/status", &[]),
        )
        .await??;
        if status != 200 {
            return Err("status request failed".into());
        }
        result.push(serde_json::from_slice(&body)?);
    }
    Ok(result)
}
async fn settle(config: &ClusterConfig) -> Result<(Vec<Value>, bool), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_millis(config.rpc_timeout_ms * 3);
    let mut quiet = 0;
    loop {
        let statuses = resources(config).await?;
        if statuses.iter().all(|s| s["metrics"]["work_pending"] == 0) {
            quiet += 1;
        } else {
            quiet = 0;
        }
        if quiet >= 5 {
            return Ok((statuses, true));
        }
        if Instant::now() >= deadline {
            return Ok((statuses, false));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
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
        || spec.payload_bytes < 64
        || spec.payload_bytes > 1024 * 1024
        || spec.rate == 0
        || !spec.seconds.is_finite()
        || spec.seconds <= 0.0
        || spec.seconds > 3600.0
        || !spec.warmup_seconds.is_finite()
        || spec.warmup_seconds < 0.0
        || spec.warmup_seconds > 60.0
    {
        return Err("invalid benchmark specification".into());
    }
    let config = Arc::new(ClusterConfig::load(&spec.config)?);
    let descriptor = distributed_kv_harness::protocol::descriptor(&config.coordinator)?;
    if config.shards.iter().any(|r| r.len() != descriptor.replicas) {
        return Err("replica count does not match protocol".into());
    }
    let count = if spec.mode == "preload" {
        spec.keys as u64
    } else {
        (spec.rate as f64 * spec.seconds).floor() as u64
    };
    let warm_count = if spec.mode == "run" {
        (spec.rate as f64 * spec.warmup_seconds).floor() as u64
    } else {
        0
    };
    if count == 0 || count + warm_count > 2_000_000 {
        return Err("benchmark requires 1..=2,000,000 total arrivals".into());
    }
    let body = Arc::new(serde_json::to_vec(
        &json!({"v":"x".repeat(spec.payload_bytes-8)}),
    )?);
    assert_eq!(body.len(), spec.payload_bytes);
    let (work_tx, work_rx) = mpsc::channel::<Work>(spec.workers * 2);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let outstanding = Arc::new(AtomicU64::new(0));
    let (result_tx, mut result_rx) = mpsc::channel::<Sample>(spec.workers * 4);
    let collector = tokio::spawn(async move {
        let mut samples = Vec::new();
        while let Some(s) = result_rx.recv().await {
            samples.push(s);
        }
        samples
    });
    let mut workers = JoinSet::new();
    for _ in 0..spec.workers {
        let rx = work_rx.clone();
        let results = result_tx.clone();
        let body = body.clone();
        let outstanding = outstanding.clone();
        let allow_missing = spec.delete_percent > 0;
        let run_id = spec.run_id;
        workers.spawn(async move {
            let mut clients = BTreeMap::new();
            loop {
                let Some(work) = rx.lock().await.recv().await else {
                    break;
                };
                let dispatched_ns = ns(work.origin);
                let mut payload = Vec::new();
                if work.method == "PUT" {
                    payload = (*body).clone();
                    let identity = format!(
                        "{run_id:016x}{:016x}{:016x}",
                        if work.phase == "warmup" { 1u64 } else { 2u64 },
                        work.seq
                    );
                    payload[6..54].copy_from_slice(identity.as_bytes());
                }
                let client = clients
                    .entry(work.node)
                    .or_insert_with(|| Client::new(work.node));
                let response = timeout(
                    Duration::from_secs(5),
                    client.request(work.method, &format!("/bench-key-{}", work.key), &payload),
                )
                .await;
                let (status, error) = match response {
                    Ok(Ok((status, data))) => {
                        let valid = match work.method {
                            "PUT" => status == 201,
                            "DELETE" => status == 204,
                            _ => {
                                (status == 404 && allow_missing)
                                    || (status == 200
                                        && data.len() == body.len()
                                        && serde_json::from_slice::<Value>(&data).ok().is_some_and(
                                            |v| {
                                                v.as_object().is_some_and(|o| o.len() == 1)
                                                    && v["v"]
                                                        .as_str()
                                                        .is_some_and(|s| s.len() == body.len() - 8)
                                            },
                                        ))
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
                    Ok(Err(e)) => (0, Some(e.to_string())),
                    Err(_) => {
                        clients.remove(&work.node);
                        (0, Some("client timeout; mutation outcome unknown".into()))
                    }
                };
                let sample = Sample {
                    seq: work.seq,
                    key: work.key,
                    method: work.method,
                    phase: work.phase,
                    scheduled_ns: work.scheduled_ns,
                    dispatched_ns: Some(dispatched_ns),
                    completed_ns: ns(work.origin),
                    status,
                    error,
                };
                let sent = results.send(sample).await;
                outstanding.fetch_sub(1, Ordering::SeqCst);
                if sent.is_err() {
                    break;
                }
            }
        });
    }
    let mut measured_start = Instant::now();
    let mut before = Vec::new();
    let mut before_quiet = true;
    let mut usage_before = None;
    for (phase, arrivals) in [("warmup", warm_count), ("measure", count)] {
        if phase == "measure" {
            while outstanding.load(Ordering::SeqCst) > 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if spec.capture_resources {
                (before, before_quiet) = settle(&config).await?;
            }
            usage_before = distributed_kv_harness::resources::usage();
        }
        let start = Instant::now();
        if phase == "measure" {
            measured_start = start;
        }
        let mut seed = spec.seed.max(1);
        for seq in 0..arrivals {
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
                let owners = config.replicas(&key_name);
                config
                    .node(owners[random(&mut seed) as usize % owners.len()])?
                    .http
            } else {
                config.nodes[random(&mut seed) as usize % config.nodes.len()].http
            };
            let method =
                if spec.mode == "preload" || random(&mut seed) % 100 < spec.mutation_percent {
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
                origin: start,
                phase,
            };
            outstanding.fetch_add(1, Ordering::SeqCst);
            if spec.mode == "preload" {
                work_tx.send(work).await?;
            } else if let Err(e) = work_tx.try_send(work) {
                let work = e.into_inner();
                outstanding.fetch_sub(1, Ordering::SeqCst);
                result_tx
                    .send(Sample {
                        seq,
                        key,
                        method,
                        phase,
                        scheduled_ns: work.scheduled_ns,
                        dispatched_ns: None,
                        completed_ns: ns(start),
                        status: 0,
                        error: Some("load generator queue full".into()),
                    })
                    .await?;
            }
        }
    }
    drop(work_tx);
    drop(result_tx);
    while let Some(result) = workers.join_next().await {
        result?;
    }
    let mut all = collector.await?;
    let warm_errors = all
        .iter()
        .filter(|s| s.phase == "warmup" && s.error.is_some())
        .count();
    all.retain(|s| s.phase == "measure");
    let mut samples = all;
    samples.sort_by_key(|s| s.seq);
    assert_eq!(samples.len(), count as usize);
    let elapsed = measured_start.elapsed().as_secs_f64();
    let (after, after_quiet) = if spec.capture_resources {
        settle(&config).await?
    } else {
        (Vec::new(), true)
    };
    let usage_after = distributed_kv_harness::resources::usage();
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
    let summary = json!({"spec":spec,"protocol":descriptor,"elapsed_seconds":elapsed,"window_seconds":duration,"offered":count,
        "successes":samples.iter().filter(|s|s.error.is_none()).count(),"errors":samples.iter().filter(|s|s.error.is_some()).count(),
        "rejected":samples.iter().filter(|s|s.dispatched_ns.is_none()).count(),"warmup_offered":warm_count,"warmup_errors":warm_errors,
        "successful_completions_in_window":samples.iter().filter(|s|s.error.is_none() && s.completed_ns as f64<=duration*1e9).count(),
        "driver_cpu_seconds":usage_before.zip(usage_after).map(|(a,b)|b.cpu_seconds-a.cpu_seconds),"driver_peak_rss_bytes":usage_after.map(|u|u.peak_rss_bytes),
        "resources_before":before,"resources_after":after,"settled":before_quiet && after_quiet,"operations":operations});
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
