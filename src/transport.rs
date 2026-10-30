//! Persistent multiplexed RPCs; requests are never automatically retried after a connection failure.
use crate::{
    config::ClusterConfig,
    coordinator::Coordinator,
    metrics::Metrics,
    types::{BoxFuture, ErrorKind, KvError, NodeId},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, interval, timeout},
};

const MAX_FRAME: usize = 2 * 1024 * 1024;
const MAX_PENDING: usize = 256;
const WIRE_VERSION: u32 = 1;

pub trait Transport: Send + Sync {
    fn request(&self, to: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>>;
}

#[derive(Clone, Debug, Default)]
pub enum LinkFault {
    #[default]
    Healthy,
    Block,
    Delay(Duration),
    /// Deliver the request, then discard the reply. A timed-out mutation can have taken effect.
    DropResponse,
}

#[derive(Default)]
pub struct FaultController {
    rules: RwLock<HashMap<NodeId, LinkFault>>,
}
impl FaultController {
    pub fn set(&self, to: NodeId, fault: LinkFault) {
        self.rules.write().expect("fault lock").insert(to, fault);
    }
    pub fn clear(&self) {
        self.rules.write().expect("fault lock").clear();
    }
    fn get(&self, to: NodeId) -> LinkFault {
        self.rules
            .read()
            .expect("fault lock")
            .get(&to)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize)]
struct Hello {
    wire_version: u32,
    from: NodeId,
    to: NodeId,
    config: ClusterConfig,
}
#[derive(Serialize, Deserialize)]
struct Request {
    id: u64,
    payload: Value,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    id: u64,
    result: Result<Value, KvError>,
}
struct Call {
    id: u64,
    payload: Value,
    reply: oneshot::Sender<Result<Value, KvError>>,
    expires: Instant,
    _activity: crate::activity::Activity,
}
struct PendingReply {
    reply: oneshot::Sender<Result<Value, KvError>>,
    expires: Instant,
    _activity: crate::activity::Activity,
}

pub struct TcpTransport {
    links: HashMap<NodeId, mpsc::Sender<Call>>,
    tasks: Vec<JoinHandle<()>>,
    next_id: AtomicU64,
    deadline: Duration,
    one_way_delay: Duration,
    metrics: Arc<Metrics>,
    pub faults: Arc<FaultController>,
}

impl TcpTransport {
    pub fn new(config: Arc<ClusterConfig>, from: NodeId, metrics: Arc<Metrics>) -> Arc<Self> {
        let mut links = HashMap::new();
        let mut tasks = Vec::new();
        for node in &config.nodes {
            if node.id == from {
                continue;
            }
            let (tx, rx) = mpsc::channel(MAX_PENDING);
            links.insert(node.id, tx);
            tasks.push(tokio::spawn(peer_loop(
                config.clone(),
                from,
                node.id,
                metrics.clone(),
                rx,
            )));
        }
        Arc::new(Self {
            links,
            tasks,
            next_id: AtomicU64::new(1),
            deadline: Duration::from_millis(config.rpc_timeout_ms),
            one_way_delay: Duration::from_micros(config.benchmark_peer_rtt_ms * 500),
            faults: Arc::new(FaultController::default()),
            metrics,
        })
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Transport for TcpTransport {
    fn request(&self, to: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            let tx = self.links.get(&to).ok_or_else(|| {
                KvError::invalid("transport destination must be a configured remote peer")
            })?;
            let fault = self.faults.get(to);
            if matches!(fault, LinkFault::Block) {
                return Err(KvError::unavailable("link blocked by test fault"));
            }
            let expires = Instant::now() + self.deadline;
            timeout(self.deadline, async {
                if !self.one_way_delay.is_zero() {
                    tokio::time::sleep(self.one_way_delay).await;
                }
                if let LinkFault::Delay(duration) = fault {
                    tokio::time::sleep(duration).await;
                }
                let id = self
                    .next_id
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .map_err(|_| KvError::unavailable("RPC id space exhausted"))?;
                let (reply, done) = oneshot::channel();
                tx.send(Call {
                    id,
                    payload,
                    reply,
                    expires,
                    _activity: crate::activity::Activity::new(self.metrics.clone()),
                })
                .await
                .map_err(|_| KvError::unavailable("peer worker stopped"))?;
                let result = done
                    .await
                    .map_err(|_| KvError::unavailable("peer connection ended"))?;
                if matches!(fault, LinkFault::DropResponse) {
                    return std::future::pending::<Result<Value, KvError>>().await;
                }
                result
            })
            .await
            .map_err(|_| {
                KvError::new(
                    ErrorKind::Timeout,
                    "peer request timed out; mutation outcome may be unknown",
                )
            })?
        })
    }
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    metrics: &Metrics,
) -> Result<(), KvError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_FRAME {
        return Err(KvError::invalid("peer frame exceeds 2 MiB"));
    }
    writer
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|e| KvError::unavailable(e.to_string()))?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|e| KvError::unavailable(e.to_string()))?;
    metrics
        .peer_bytes_sent
        .fetch_add(4 + bytes.len() as u64, Ordering::Relaxed);
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    metrics: &Metrics,
) -> Result<T, KvError> {
    let len = reader
        .read_u32()
        .await
        .map_err(|e| KvError::unavailable(e.to_string()))? as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(KvError::protocol("invalid peer frame length"));
    }
    let mut data = vec![0; len];
    reader
        .read_exact(&mut data)
        .await
        .map_err(|e| KvError::unavailable(e.to_string()))?;
    metrics
        .peer_bytes_received
        .fetch_add(4 + len as u64, Ordering::Relaxed);
    Ok(serde_json::from_slice(&data)?)
}

async fn peer_loop(
    config: Arc<ClusterConfig>,
    from: NodeId,
    to: NodeId,
    metrics: Arc<Metrics>,
    mut rx: mpsc::Receiver<Call>,
) {
    while let Some(first) = rx.recv().await {
        if first.reply.is_closed() {
            continue;
        }
        let connected = timeout(Duration::from_millis(config.rpc_timeout_ms), async {
            let mut stream = TcpStream::connect(config.node(to)?.peer)
                .await
                .map_err(|e| KvError::unavailable(e.to_string()))?;
            stream
                .set_nodelay(true)
                .map_err(|e| KvError::unavailable(e.to_string()))?;
            write_frame(
                &mut stream,
                &Hello {
                    wire_version: WIRE_VERSION,
                    from,
                    to,
                    config: (*config).clone(),
                },
                &metrics,
            )
            .await?;
            let accepted: Result<(), KvError> = read_frame(&mut stream, &metrics).await?;
            accepted?;
            Ok::<_, KvError>(stream)
        })
        .await
        .unwrap_or_else(|_| {
            Err(KvError::new(
                ErrorKind::Timeout,
                "peer connection handshake timed out",
            ))
        });
        match connected {
            Ok(stream) => {
                metrics.peer_connections.fetch_add(1, Ordering::Relaxed);
                run_connection(stream, first, &mut rx, metrics.clone()).await;
            }
            Err(error) => {
                let _ = first.reply.send(Err(error));
            }
        }
    }
}

struct AbortReader(JoinHandle<()>);
impl Drop for AbortReader {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn send_call<W: AsyncWrite + Unpin>(
    writer: &mut W,
    call: Call,
    pending: &mut HashMap<u64, PendingReply>,
    metrics: &Metrics,
) -> Result<(), KvError> {
    if call.reply.is_closed() {
        return Ok(());
    }
    if pending.len() >= MAX_PENDING {
        let _ = call.reply.send(Err(KvError::new(
            ErrorKind::Overloaded,
            "peer has too many pending requests",
        )));
        return Ok(());
    }
    let request = Request {
        id: call.id,
        payload: call.payload,
    };
    pending.insert(
        call.id,
        PendingReply {
            reply: call.reply,
            expires: call.expires,
            _activity: call._activity,
        },
    );
    write_frame(writer, &request, metrics).await?;
    metrics.rpc_requests_sent.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn run_connection(
    stream: TcpStream,
    first: Call,
    rx: &mut mpsc::Receiver<Call>,
    metrics: Arc<Metrics>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (received, mut incoming) = mpsc::channel(MAX_PENDING);
    let reader_metrics = metrics.clone();
    let _reader_task = AbortReader(tokio::spawn(async move {
        loop {
            let result = read_frame::<_, Reply>(&mut reader, &reader_metrics).await;
            let failed = result.is_err();
            if result.is_ok() {
                reader_metrics
                    .rpc_replies_received
                    .fetch_add(1, Ordering::Relaxed);
            }
            if received.send(result).await.is_err() || failed {
                break;
            }
        }
    }));
    let mut pending = HashMap::new();
    let mut failure = send_call(&mut writer, first, &mut pending, &metrics)
        .await
        .err();
    let mut tick = interval(Duration::from_millis(100));
    while failure.is_none() {
        tokio::select! {
            call = rx.recv() => match call {
                Some(call) => { failure = send_call(&mut writer, call, &mut pending, &metrics).await.err(); }
                None => break,
            },
            reply = incoming.recv() => match reply {
                Some(Ok(reply)) => { if let Some(done) = pending.remove(&reply.id) { let _ = done.reply.send(reply.result); } }
                Some(Err(error)) => { failure = Some(error); }
                None => break,
            },
            _ = tick.tick() => { pending.retain(|_, call| call.expires>Instant::now()); }
        }
    }
    let error = failure.unwrap_or_else(|| {
        KvError::unavailable("peer connection stopped; mutation outcome may be unknown")
    });
    for (_, reply) in pending {
        let _ = reply.reply.send(Err(error.clone()));
    }
}

pub async fn serve_peers(
    listener: TcpListener,
    node_id: NodeId,
    config: Arc<ClusterConfig>,
    coordinator: Arc<dyn Coordinator>,
    metrics: Arc<Metrics>,
    mut stop: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    let capacity = Arc::new(Semaphore::new(128));
    loop {
        tokio::select! {
            _ = stop.wait_for(|stopping| *stopping) => break,
            accepted = listener.accept() => if let Ok((stream, _)) = accepted {
                let Ok(permit) = capacity.clone().try_acquire_owned() else { continue; };
                let config = config.clone(); let coordinator = coordinator.clone(); let metrics = metrics.clone();
                connections.spawn(async move { let _permit = permit; serve_connection(stream, node_id, config, coordinator, metrics).await; });
            },
            _ = connections.join_next(), if !connections.is_empty() => (),
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn serve_connection(
    mut stream: TcpStream,
    node_id: NodeId,
    config: Arc<ClusterConfig>,
    coordinator: Arc<dyn Coordinator>,
    metrics: Arc<Metrics>,
) {
    let deadline = Duration::from_millis(config.rpc_timeout_ms);
    let hello = match timeout(deadline, read_frame::<_, Hello>(&mut stream, &metrics)).await {
        Ok(Ok(hello)) => hello,
        _ => return,
    };
    let accepted = if hello.wire_version == WIRE_VERSION
        && hello.to == node_id
        && hello.from != node_id
        && config.node(hello.from).is_ok()
        && hello.config == *config
    {
        Ok(())
    } else {
        Err(KvError::protocol(
            "peer identity, wire version or cluster configuration mismatch",
        ))
    };
    if !matches!(
        timeout(deadline, write_frame(&mut stream, &accepted, &metrics)).await,
        Ok(Ok(()))
    ) || accepted.is_err()
    {
        return;
    }
    let _ = stream.set_nodelay(true);
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let mut handlers = JoinSet::new();
    let capacity = Arc::new(Semaphore::new(128));
    loop {
        // This read is never cancelled to service another message. Partial frame reads stay intact.
        let request = match read_frame::<_, Request>(&mut reader, &metrics).await {
            Ok(request) => request,
            Err(_) => break,
        };
        metrics
            .rpc_requests_received
            .fetch_add(1, Ordering::Relaxed);
        let Ok(permit) = capacity.clone().acquire_owned().await else {
            break;
        };
        let coordinator = coordinator.clone();
        let writer = writer.clone();
        let metrics = metrics.clone();
        let activity = crate::activity::Activity::new(metrics.clone());
        let config = config.clone();
        handlers.spawn(async move {
            let _activity = activity;
            let _permit = permit;
            let result = timeout(deadline, coordinator.on_peer(hello.from, request.payload))
                .await
                .unwrap_or_else(|_| {
                    Err(KvError::new(
                        ErrorKind::Timeout,
                        "peer handler timed out; mutation outcome may be unknown",
                    ))
                });
            // Delay each response independently, before taking the connection writer.
            // This models propagation delay without serializing concurrent RPCs.
            if config.benchmark_peer_rtt_ms > 0 {
                tokio::time::sleep(Duration::from_micros(config.benchmark_peer_rtt_ms * 500)).await;
            }
            let mut writer = writer.lock().await;
            if matches!(
                timeout(
                    deadline,
                    write_frame(
                        &mut *writer,
                        &Reply {
                            id: request.id,
                            result
                        },
                        &metrics
                    )
                )
                .await,
                Ok(Ok(()))
            ) {
                metrics.rpc_replies_sent.fetch_add(1, Ordering::Relaxed);
            }
        });
        while handlers.try_join_next().is_some() {}
    }
    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
}
