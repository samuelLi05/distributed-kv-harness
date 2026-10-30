//! Fixed-membership Hermes coherence experiment. ALL three members acknowledge INV.
//! Durable INV survives restart; VAL is volatile and recovery replays the original tag.
//! The ALR variant prices post-read quorum checks in a static epoch; it is not a membership service.
use crate::{
    activity::Activity,
    config::stable_hash,
    coordinator::{Coordinator, CoordinatorContext},
    routing::ReplicaRouter,
    store::Record,
    types::{BoxFuture, KvError, Operation, Outcome},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, Notify, Semaphore, mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};
const STRIPES: usize = 32;
const BOOT: &str = "_hermes/boot";
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Tag {
    pub counter: u64,
    pub writer: u64,
    pub incarnation: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Register {
    pub tag: Tag,
    pub value: Option<Value>,
    pub valid: bool,
}
impl Default for Register {
    fn default() -> Self {
        Self {
            tag: Tag::default(),
            value: None,
            valid: true,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    Forward {
        operation: Operation,
    },
    Probe {
        key: String,
    },
    Inv {
        key: String,
        tag: Tag,
        value: Option<Value>,
    },
    Val {
        key: String,
        tag: Tag,
    },
    Sync {
        key: String,
        epoch: u64,
    },
}
impl Message {
    fn key(&self) -> &str {
        match self {
            Self::Forward { operation } => operation.key(),
            Self::Probe { key }
            | Self::Inv { key, .. }
            | Self::Val { key, .. }
            | Self::Sync { key, .. } => key,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Reply {
    Pong,
    Outcome(Outcome),
    Ack(Tag),
    Synced,
}
#[derive(Default)]
struct KeyState {
    register: Register,
    gate: Arc<Mutex<()>>,
    changed: Arc<Notify>,
    invalid_since: Option<tokio::time::Instant>,
}
struct Apply {
    message: Message,
    reply: oneshot::Sender<Result<Tag, KvError>>,
    _activity: Activity,
}
struct Read {
    key: String,
    value: Option<Value>,
    reply: oneshot::Sender<Result<Outcome, KvError>>,
    _activity: Activity,
}
#[derive(Default)]
struct Stats {
    writes: AtomicU64,
    local_reads: AtomicU64,
    replays: AtomicU64,
    validations: AtomicU64,
    stale_vals: AtomicU64,
    sync_batches: AtomicU64,
    synced_reads: AtomicU64,
    background_errors: AtomicU64,
}
struct Core {
    context: CoordinatorContext,
    state: RwLock<HashMap<String, KeyState>>,
    apply: Vec<mpsc::Sender<Apply>>,
    reads: HashMap<Vec<u64>, mpsc::Sender<Read>>,
    incarnation: AtomicU64,
    counter: AtomicU64,
    capacity: Arc<Semaphore>,
    background: Arc<Semaphore>,
    router: ReplicaRouter,
    stats: Stats,
    alr: bool,
}
pub struct HermesCoordinator {
    core: Arc<Core>,
}
fn slot(key: &str) -> usize {
    stable_hash(key.as_bytes()) as usize % STRIPES
}
fn disk_key(key: &str) -> String {
    format!("_hermes/data/{key}")
}
impl HermesCoordinator {
    pub fn build(context: CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError> {
        if !matches!(
            context.config.coordinator.as_str(),
            "hermes_static" | "hermes_alr_static"
        ) || context.config.shards.iter().any(|r| r.len() != 3)
        {
            return Err(KvError::invalid("Hermes experiments require fixed RF=3"));
        }
        let alr = context.config.coordinator == "hermes_alr_static";
        let mut apply = Vec::new();
        let mut workers = Vec::new();
        for _ in 0..STRIPES {
            let (tx, rx) = mpsc::channel(8);
            apply.push(tx);
            workers.push(rx);
        }
        let mut reads = HashMap::new();
        let mut readers = Vec::new();
        if alr {
            for (index, group) in context.config.shards.iter().enumerate() {
                if group.contains(&context.node_id) && !reads.contains_key(group) {
                    let (tx, rx) = mpsc::channel(256);
                    reads.insert(group.clone(), tx);
                    readers.push((format!("__group-key-{index}"), group.clone(), rx));
                }
            }
        }
        let core = Arc::new(Core {
            context,
            state: RwLock::new(HashMap::new()),
            apply,
            reads,
            incarnation: AtomicU64::new(0),
            counter: AtomicU64::new(0),
            capacity: Arc::new(Semaphore::new(256)),
            background: Arc::new(Semaphore::new(512)),
            router: ReplicaRouter::default(),
            stats: Stats::default(),
            alr,
        });
        for rx in workers {
            let weak = Arc::downgrade(&core);
            tokio::spawn(apply_worker(weak, rx));
        }
        for (_, group, rx) in readers {
            let weak = Arc::downgrade(&core);
            tokio::spawn(read_worker(weak, group, rx));
        }
        Ok(Arc::new(Self { core }))
    }
}
async fn apply_worker(core: Weak<Core>, mut rx: mpsc::Receiver<Apply>) {
    while let Some(command) = rx.recv().await {
        let Some(core) = core.upgrade() else { break };
        let result = core.install(command.message).await;
        let _ = command.reply.send(result);
    }
}
async fn read_worker(core: Weak<Core>, group: Vec<u64>, mut rx: mpsc::Receiver<Read>) {
    while let Some(first) = rx.recv().await {
        let Some(core) = core.upgrade() else { break };
        let mut batch = vec![first];
        while batch.len() < 128 {
            match rx.try_recv() {
                Ok(read) => batch.push(read),
                Err(_) => break,
            }
        }
        core.stats.sync_batches.fetch_add(1, Ordering::Relaxed);
        core.stats
            .synced_reads
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        let result = core.sync(&group, &batch[0].key).await;
        for read in batch {
            let outcome = result
                .clone()
                .map(|_| read.value.map_or(Outcome::Missing, Outcome::Value));
            let _ = read.reply.send(outcome);
        }
    }
}
impl Core {
    fn snapshot(&self, key: &str) -> Register {
        self.state
            .read()
            .unwrap()
            .get(key)
            .map_or(Register::default(), |s| s.register.clone())
    }
    fn gate(&self, key: &str) -> Arc<Mutex<()>> {
        self.state
            .write()
            .unwrap()
            .entry(key.into())
            .or_default()
            .gate
            .clone()
    }
    async fn apply(self: &Arc<Self>, message: Message) -> Result<Tag, KvError> {
        let stripe = slot(message.key());
        let (reply, done) = oneshot::channel();
        self.apply[stripe]
            .send(Apply {
                message,
                reply,
                _activity: Activity::new(self.context.metrics.clone()),
            })
            .await
            .map_err(|_| KvError::storage("Hermes apply stopped"))?;
        done.await
            .map_err(|_| KvError::storage("Hermes apply stopped before ACK"))?
    }
    async fn install(&self, message: Message) -> Result<Tag, KvError> {
        let current = self.snapshot(message.key());
        match message {
            Message::Inv { key, tag, value } => {
                if tag == current.tag && value != current.value {
                    return Err(KvError::protocol("same Hermes tag has different values"));
                }
                if tag <= current.tag {
                    return Ok(current.tag);
                }
                let register = Register {
                    tag,
                    value,
                    valid: false,
                };
                self.context
                    .store
                    .commit(vec![Record {
                        key: disk_key(&key),
                        state: serde_json::to_value(&register)?,
                    }])
                    .await?;
                let mut states = self.state.write().unwrap();
                let state = states.entry(key).or_default();
                state.register = register;
                state.invalid_since = Some(tokio::time::Instant::now());
                state.changed.notify_waiters();
                Ok(tag)
            }
            Message::Val { key, tag } => {
                if current.tag == tag {
                    let mut states = self.state.write().unwrap();
                    let state = states.entry(key).or_default();
                    state.register.valid = true;
                    state.invalid_since = None;
                    state.changed.notify_waiters();
                    self.stats.validations.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.stats.stale_vals.fetch_add(1, Ordering::Relaxed);
                }
                Ok(current.tag)
            }
            _ => Err(KvError::protocol("invalid Hermes apply command")),
        }
    }
    fn call(
        self: &Arc<Self>,
        node: u64,
        message: Message,
    ) -> BoxFuture<'_, Result<Reply, KvError>> {
        Box::pin(async move {
            if node == self.context.node_id {
                self.replica(node, message).await
            } else {
                let value = timeout(
                    Duration::from_millis(self.context.config.rpc_timeout_ms),
                    self.context
                        .transport
                        .request(node, serde_json::to_value(message)?),
                )
                .await
                .map_err(|_| KvError::unavailable("Hermes peer timeout"))??;
                serde_json::from_value(value).map_err(KvError::from)
            }
        })
    }
    async fn all_remote(
        self: &Arc<Self>,
        key: &str,
        message: Message,
        tag: Tag,
    ) -> Result<(), KvError> {
        let mut calls = JoinSet::new();
        for &node in self.context.config.replicas(key) {
            if node != self.context.node_id {
                let core = self.clone();
                let message = message.clone();
                calls.spawn(async move { core.call(node, message).await });
            }
        }
        let mut count = 0;
        while let Some(result) = calls.join_next().await {
            match result {
                Ok(Ok(Reply::Ack(ack))) if ack >= tag => count += 1,
                _ => {
                    return Err(KvError::unavailable(
                        "Hermes requires all members; mutation outcome unknown",
                    ));
                }
            }
        }
        if count == 2 {
            Ok(())
        } else {
            Err(KvError::unavailable("Hermes member unavailable"))
        }
    }
    fn validate_remotes(self: &Arc<Self>, key: &str, tag: Tag) {
        for &node in self.context.config.replicas(key) {
            if node != self.context.node_id {
                let Ok(permit) = self.background.clone().try_acquire_owned() else {
                    self.stats.background_errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let core = self.clone();
                let message = Message::Val {
                    key: key.into(),
                    tag,
                };
                let activity = Activity::new(self.context.metrics.clone());
                tokio::spawn(async move {
                    let _permit = permit;
                    let _activity = activity;
                    if core.call(node, message).await.is_err() {
                        core.stats.background_errors.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        }
    }
    async fn repair(self: &Arc<Self>, key: &str) -> Result<Register, KvError> {
        let changed = self
            .state
            .write()
            .unwrap()
            .entry(key.into())
            .or_default()
            .changed
            .clone();
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (state, invalid_since) = {
                let states = self.state.read().unwrap();
                let state = states.get(key).unwrap();
                (state.register.clone(), state.invalid_since)
            };
            if state.valid {
                return Ok(state);
            }
            // A live invalidation normally receives VAL shortly. Waiting changes
            // recovery timing only. Recovered invalidations replay immediately.
            if let Some(since) = invalid_since {
                let grace =
                    Duration::from_millis(20 + 2 * self.context.config.benchmark_peer_rtt_ms);
                if let Some(remaining) = grace.checked_sub(since.elapsed()) {
                    let _ = timeout(remaining, notified).await;
                    continue;
                }
            }
            self.stats.replays.fetch_add(1, Ordering::Relaxed);
            self.all_remote(
                key,
                Message::Inv {
                    key: key.into(),
                    tag: state.tag,
                    value: state.value.clone(),
                },
                state.tag,
            )
            .await?;
            self.apply(Message::Val {
                key: key.into(),
                tag: state.tag,
            })
            .await?;
            self.validate_remotes(key, state.tag);
        }
    }
    async fn read_local(self: &Arc<Self>, key: &str) -> Result<Register, KvError> {
        let state = self.snapshot(key);
        if state.valid {
            return Ok(state);
        }
        let gate = self.gate(key);
        let _guard = gate.lock().await;
        self.repair(key).await
    }
    async fn sync(self: &Arc<Self>, group: &[u64], key: &str) -> Result<(), KvError> {
        let mut calls = JoinSet::new();
        for &node in group {
            if node != self.context.node_id {
                let core = self.clone();
                let message = Message::Sync {
                    key: key.to_string(),
                    epoch: 0,
                };
                calls.spawn(async move { core.call(node, message).await });
            }
        }
        while let Some(result) = calls.join_next().await {
            if matches!(result, Ok(Ok(Reply::Synced))) {
                return Ok(());
            }
        }
        Err(KvError::unavailable("ALR membership check lacks a quorum"))
    }
    async fn execute_owned(self: &Arc<Self>, operation: Operation) -> Result<Outcome, KvError> {
        let key = operation.key().to_string();
        match operation {
            Operation::Get { .. } => {
                let state = self.read_local(&key).await?;
                if !self.alr {
                    self.stats.local_reads.fetch_add(1, Ordering::Relaxed);
                    return Ok(state.value.map_or(Outcome::Missing, Outcome::Value));
                }
                let (reply, done) = oneshot::channel();
                self.reads
                    .get(self.context.config.replicas(&key))
                    .ok_or_else(|| KvError::protocol("ALR queue missing"))?
                    .try_send(Read {
                        key: key.clone(),
                        value: state.value,
                        reply,
                        _activity: Activity::new(self.context.metrics.clone()),
                    })
                    .map_err(|_| KvError::unavailable("ALR queue full"))?;
                done.await
                    .map_err(|_| KvError::unavailable("ALR worker stopped"))?
            }
            mutation => {
                let gate = self.gate(&key);
                let _guard = gate.lock().await;
                let prior = self.repair(&key).await?;
                let old = self
                    .counter
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        n.max(prior.tag.counter).checked_add(1)
                    })
                    .map_err(|_| KvError::unavailable("Hermes timestamp exhausted"))?;
                let tag = Tag {
                    counter: old.max(prior.tag.counter) + 1,
                    writer: self.context.node_id,
                    incarnation: self.incarnation.load(Ordering::Acquire),
                };
                let (value, outcome) = match mutation {
                    Operation::Put { value, .. } => (Some(value), Outcome::Written),
                    Operation::Delete { .. } => (None, Outcome::Deleted),
                    _ => unreachable!(),
                };
                let inv = Message::Inv {
                    key: key.clone(),
                    tag,
                    value,
                };
                let (local, remote) =
                    tokio::join!(self.apply(inv.clone()), self.all_remote(&key, inv, tag));
                local?;
                remote?;
                self.apply(Message::Val {
                    key: key.clone(),
                    tag,
                })
                .await?;
                self.validate_remotes(&key, tag);
                self.stats.writes.fetch_add(1, Ordering::Relaxed);
                Ok(outcome)
            }
        }
    }
    async fn execute(self: &Arc<Self>, operation: Operation) -> Result<Outcome, KvError> {
        let key = operation.key().to_string();
        if self
            .context
            .config
            .replicas(&key)
            .contains(&self.context.node_id)
        {
            self.execute_owned(operation).await
        } else {
            let value = self
                .router
                .forward(
                    &self.context,
                    &key,
                    serde_json::to_value(Message::Forward { operation })?,
                )
                .await?;
            match serde_json::from_value(value)? {
                Reply::Outcome(outcome) => Ok(outcome),
                _ => Err(KvError::protocol("invalid Hermes forwarded outcome")),
            }
        }
    }
    fn dispatch(self: &Arc<Self>, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        Box::pin(async move {
            operation.validate()?;
            let permit = self
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| KvError::unavailable("Hermes admission full"))?;
            let core = self.clone();
            let activity = Activity::new(core.context.metrics.clone());
            let (done, wait) = oneshot::channel();
            tokio::spawn(async move {
                let _permit = permit;
                let _activity = activity;
                let _ = done.send(core.execute(operation).await);
            });
            timeout(
                Duration::from_millis(self.context.config.rpc_timeout_ms),
                wait,
            )
            .await
            .map_err(|_| KvError::unavailable("Hermes deadline; mutation outcome unknown"))?
            .map_err(|_| KvError::unavailable("Hermes task stopped"))?
        })
    }
    async fn replica(self: &Arc<Self>, from: u64, message: Message) -> Result<Reply, KvError> {
        Operation::Get {
            key: message.key().into(),
        }
        .validate()?;
        let owners = self.context.config.replicas(message.key());
        if !owners.contains(&self.context.node_id) {
            return Err(KvError::protocol("Hermes message sent to nonowner"));
        }
        match message {
            Message::Forward { operation } => Ok(Reply::Outcome(self.dispatch(operation).await?)),
            Message::Probe { .. } => Ok(Reply::Pong),
            Message::Sync { epoch: 0, .. } if owners.contains(&from) => Ok(Reply::Synced),
            Message::Inv {
                ref key,
                tag,
                ref value,
            } => {
                if tag.counter == 0 || !owners.contains(&from) || !owners.contains(&tag.writer) {
                    return Err(KvError::protocol("invalid Hermes INV"));
                }
                if let Some(value) = value {
                    Operation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    }
                    .validate()?;
                }
                Ok(Reply::Ack(self.apply(message).await?))
            }
            Message::Val { tag, .. } => {
                if tag.counter == 0 || !owners.contains(&from) || !owners.contains(&tag.writer) {
                    return Err(KvError::protocol("invalid Hermes VAL"));
                }
                Ok(Reply::Ack(self.apply(message).await?))
            }
            _ => Err(KvError::protocol("invalid Hermes epoch")),
        }
    }
}
impl Coordinator for HermesCoordinator {
    fn name(&self) -> &'static str {
        if self.core.alr {
            "hermes_alr_static"
        } else {
            "hermes_static"
        }
    }
    fn diagnostics(&self) -> Value {
        let s = &self.core.stats;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        json!({"writes":get(&s.writes),"local_reads":get(&s.local_reads),"replays":get(&s.replays),"validations":get(&s.validations),"stale_vals":get(&s.stale_vals),"sync_batches":get(&s.sync_batches),"synced_reads":get(&s.synced_reads),"background_errors":get(&s.background_errors),"epoch":0,"membership":"immutable_all_three","replay_grace_ms":20+2*self.core.context.config.benchmark_peer_rtt_ms})
    }
    fn initialize(&self) -> BoxFuture<'_, Result<(), KvError>> {
        Box::pin(async move {
            let old = self
                .core
                .context
                .store
                .get(BOOT)?
                .map(|v| {
                    v.as_u64()
                        .ok_or_else(|| KvError::storage("invalid Hermes incarnation"))
                })
                .transpose()?
                .unwrap_or(0);
            let next = old
                .checked_add(1)
                .ok_or_else(|| KvError::storage("Hermes incarnation exhausted"))?;
            self.core
                .context
                .store
                .commit(vec![Record {
                    key: BOOT.into(),
                    state: json!(next),
                }])
                .await?;
            let records = self.core.context.store.records("_hermes/data/")?;
            let mut state = self.core.state.write().unwrap();
            for (key, value) in records {
                let mut register: Register = serde_json::from_value(value)?;
                register.valid = false;
                state
                    .entry(key.strip_prefix("_hermes/data/").unwrap().into())
                    .or_default()
                    .register = register;
            }
            self.core.incarnation.store(next, Ordering::Release);
            Ok(())
        })
    }
    fn execute(&self, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        self.core.dispatch(operation)
    }
    fn on_peer(&self, from: u64, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            Ok(serde_json::to_value(
                self.core
                    .replica(from, serde_json::from_value(payload)?)
                    .await?,
            )?)
        })
    }
}
