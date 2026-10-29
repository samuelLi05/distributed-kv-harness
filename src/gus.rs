//! RF=3 Gus experiment. Published speculative-write/view rules with durable local
//! installation and the n=3 value-transfer read slow path. No GC or live membership.
use crate::{
    activity::Activity,
    config::stable_hash,
    coordinator::{Coordinator, CoordinatorContext},
    store::Record,
    types::{BoxFuture, KvError, Operation, Outcome},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};
const STRIPES: usize = 32;
const BOOT: &str = "_gus/boot";
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Tag {
    pub counter: u64,
    pub writer: u64,
    pub incarnation: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Version {
    pub key: String,
    pub id: Tag,
    pub tag: Tag,
    pub value: Option<Value>,
    pub installed: bool,
    pub observed: Tag,
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
    Query {
        key: String,
    },
    Offer {
        key: String,
        id: Tag,
        value: Option<Value>,
    },
    Commit {
        key: String,
        id: Tag,
        tag: Tag,
        #[serde(default)]
        value: Option<Version>,
    },
    Fetch {
        key: String,
        tag: Tag,
    },
    View {
        key: String,
        tag: Tag,
    },
}
impl Message {
    fn key(&self) -> &str {
        match self {
            Self::Forward { operation } => operation.key(),
            Self::Probe { key }
            | Self::Query { key }
            | Self::Offer { key, .. }
            | Self::Commit { key, .. }
            | Self::Fetch { key, .. }
            | Self::View { key, .. } => key,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Reply {
    Pong,
    Outcome(Outcome),
    Tag(Tag),
    Offer(Tag),
    Committed,
    NeedValue,
    Busy,
    Retagged,
    Version(Option<Version>),
    Viewed,
}
#[derive(Default)]
struct KeyState {
    gate: Arc<tokio::sync::RwLock<()>>,
    latest: Tag,
    versions: BTreeMap<Tag, Version>,
    offers: BTreeMap<Tag, Version>,
    views: BTreeMap<Tag, BTreeSet<u64>>,
}
struct Apply {
    version: Version,
    offer: bool,
    reply: oneshot::Sender<Result<Version, KvError>>,
    _activity: Activity,
}
#[derive(Default)]
struct Stats {
    fast_writes: AtomicU64,
    retags: AtomicU64,
    fast_reads: AtomicU64,
    transfers: AtomicU64,
    views: AtomicU64,
    background_errors: AtomicU64,
}
struct Core {
    context: CoordinatorContext,
    state: RwLock<HashMap<String, KeyState>>,
    apply: Vec<mpsc::Sender<Apply>>,
    incarnation: AtomicU64,
    counter: AtomicU64,
    capacity: Arc<Semaphore>,
    background: Arc<Semaphore>,
    router: crate::routing::ReplicaRouter,
    active: RwLock<BTreeSet<Tag>>,
    stats: Stats,
}
pub struct GusCoordinator {
    core: Arc<Core>,
}
struct Active {
    core: Arc<Core>,
    id: Tag,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.core.active.write().unwrap().remove(&self.id);
    }
}
fn slot(key: &str) -> usize {
    stable_hash(key.as_bytes()) as usize % STRIPES
}
fn disk_key(v: &Version) -> String {
    format!(
        "_gus/version/{}/{:016x}-{:016x}-{:016x}/{:016x}-{:016x}-{:016x}",
        v.key.len(),
        v.id.counter,
        v.id.writer,
        v.id.incarnation,
        v.tag.counter,
        v.tag.writer,
        v.tag.incarnation
    ) + "/"
        + &v.key
}
impl GusCoordinator {
    pub fn build(context: CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError> {
        if context.config.coordinator != "gus" || context.config.shards.iter().any(|r| r.len() != 3)
        {
            return Err(KvError::invalid("Gus requires RF=3"));
        }
        let mut apply = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..STRIPES {
            let (tx, rx) = mpsc::channel(8);
            apply.push(tx);
            receivers.push(rx);
        }
        let core = Arc::new(Core {
            context,
            state: RwLock::new(HashMap::new()),
            apply,
            incarnation: AtomicU64::new(0),
            counter: AtomicU64::new(0),
            capacity: Arc::new(Semaphore::new(256)),
            background: Arc::new(Semaphore::new(512)),
            router: crate::routing::ReplicaRouter::default(),
            active: RwLock::new(BTreeSet::new()),
            stats: Stats::default(),
        });
        for rx in receivers {
            let weak = Arc::downgrade(&core);
            tokio::spawn(async move {
                worker(weak, rx).await;
            });
        }
        Ok(Arc::new(Self { core }))
    }
}
async fn worker(core: std::sync::Weak<Core>, mut rx: mpsc::Receiver<Apply>) {
    while let Some(command) = rx.recv().await {
        let Some(core) = core.upgrade() else { break };
        let result = core.install(command.version, command.offer).await;
        let _ = command.reply.send(result);
    }
}
impl Core {
    fn latest(&self, key: &str) -> Tag {
        self.state
            .read()
            .unwrap()
            .get(key)
            .map_or(Tag::default(), |s| s.latest)
    }
    fn version(&self, key: &str, tag: Tag) -> Option<Version> {
        self.state
            .read()
            .unwrap()
            .get(key)
            .and_then(|s| s.versions.get(&tag))
            .cloned()
    }
    fn receipt(&self, key: &str, tag: Tag, node: u64) {
        if tag.counter > 0 {
            self.state
                .write()
                .unwrap()
                .entry(key.into())
                .or_default()
                .views
                .entry(tag)
                .or_default()
                .insert(node);
        }
    }
    fn background(self: &Arc<Self>, to: u64, message: Message) {
        // A bounded permit is acquired before spawn, so timeouts cannot create an unbounded task queue.
        let Ok(permit) = self.background.clone().try_acquire_owned() else {
            self.stats.background_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let activity = Activity::new(self.context.metrics.clone());
        let core = self.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _activity = activity;
            if core.call(to, message).await.is_err() {
                core.stats.background_errors.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    fn advertise(self: &Arc<Self>, key: &str, tag: Tag) {
        for &node in self.context.config.replicas(key) {
            if node != self.context.node_id {
                self.stats.views.fetch_add(1, Ordering::Relaxed);
                self.background(
                    node,
                    Message::View {
                        key: key.into(),
                        tag,
                    },
                );
            }
        }
    }
    async fn apply(self: &Arc<Self>, version: Version, offer: bool) -> Result<Version, KvError> {
        let (reply, done) = oneshot::channel();
        let stripe = slot(&version.key);
        self.apply[stripe]
            .send(Apply {
                version,
                offer,
                reply,
                _activity: Activity::new(self.context.metrics.clone()),
            })
            .await
            .map_err(|_| KvError::storage("Gus apply stopped"))?;
        done.await
            .map_err(|_| KvError::storage("Gus apply stopped before ACK"))?
    }
    async fn install(
        self: &Arc<Self>,
        mut version: Version,
        offer: bool,
    ) -> Result<Version, KvError> {
        let current = self.latest(&version.key);
        if offer {
            if let Some(prior) = self
                .state
                .read()
                .unwrap()
                .get(&version.key)
                .and_then(|s| s.offers.get(&version.id))
                .cloned()
            {
                if prior.value != version.value {
                    return Err(KvError::protocol(
                        "Gus operation ID reused with different value",
                    ));
                }
                return Ok(prior);
            }
            version.observed = current;
            version.installed = version.tag > current;
        } else {
            version.installed = true;
        }
        if let Some(prior) = self.version(&version.key, version.tag) {
            if prior.id != version.id || prior.value != version.value {
                return Err(KvError::protocol("conflicting Gus tag"));
            }
            if prior.installed && (!offer || prior == version) {
                return Ok(prior);
            }
        }
        self.context
            .store
            .commit(vec![Record {
                key: disk_key(&version),
                state: serde_json::to_value(&version)?,
            }])
            .await?;
        {
            let mut states = self.state.write().unwrap();
            let state = states.entry(version.key.clone()).or_default();
            if offer {
                state.offers.insert(version.id, version.clone());
            }
            if version.installed {
                state.latest = state.latest.max(version.tag);
                state.versions.insert(version.tag, version.clone());
                state
                    .views
                    .entry(version.tag)
                    .or_default()
                    .insert(self.context.node_id);
            }
        }
        if version.installed {
            self.advertise(&version.key, version.tag);
        }
        Ok(version)
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
                let reply = timeout(
                    Duration::from_millis(self.context.config.rpc_timeout_ms),
                    self.context
                        .transport
                        .request(node, serde_json::to_value(message)?),
                )
                .await
                .map_err(|_| KvError::unavailable("Gus peer timeout"))??;
                serde_json::from_value(reply).map_err(KvError::from)
            }
        })
    }
    async fn one_remote(
        self: &Arc<Self>,
        key: &str,
        message: Message,
    ) -> Result<(u64, Reply), KvError> {
        let mut calls = JoinSet::new();
        // Owned tasks continue sending to the other replica after quorum completion.
        for &node in self.context.config.replicas(key) {
            if node != self.context.node_id {
                let core = self.clone();
                let message = message.clone();
                let activity = Activity::new(self.context.metrics.clone());
                let (done, wait) = oneshot::channel();
                tokio::spawn(async move {
                    let _activity = activity;
                    let _ = done.send(core.call(node, message).await);
                });
                calls.spawn(async move {
                    (
                        node,
                        wait.await
                            .unwrap_or_else(|_| Err(KvError::unavailable("Gus call stopped"))),
                    )
                });
            }
        }
        while let Some(Ok((node, result))) = calls.join_next().await {
            if let Ok(reply) = result {
                return Ok((node, reply));
            }
        }
        Err(KvError::unavailable(
            "Gus majority unavailable; mutation may have taken effect",
        ))
    }
    async fn replica(self: &Arc<Self>, from: u64, message: Message) -> Result<Reply, KvError> {
        Operation::Get {
            key: message.key().into(),
        }
        .validate()?;
        if !self
            .context
            .config
            .replicas(message.key())
            .contains(&self.context.node_id)
        {
            return Err(KvError::protocol("Gus message sent to nonowner"));
        }
        match message {
            Message::Forward { operation } => Ok(Reply::Outcome(self.dispatch(operation).await?)),
            Message::Probe { .. } => Ok(Reply::Pong),
            Message::Query { key } => Ok(Reply::Tag(self.latest(&key))),
            Message::Fetch { key, tag } => Ok(Reply::Version(self.version(&key, tag))),
            Message::View { key, tag } => {
                if !self.context.config.replicas(&key).contains(&from) {
                    return Err(KvError::protocol("view from nonowner"));
                }
                self.receipt(&key, tag, from);
                Ok(Reply::Viewed)
            }
            Message::Offer { key, id, value } => {
                if id.writer != from || id.counter == 0 {
                    return Err(KvError::protocol("invalid Gus offer identity"));
                }
                if let Some(value) = &value {
                    Operation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    }
                    .validate()?;
                }
                let version = self
                    .apply(
                        Version {
                            key,
                            id,
                            tag: id,
                            value,
                            installed: false,
                            observed: Tag::default(),
                        },
                        true,
                    )
                    .await?;
                Ok(Reply::Offer(version.observed))
            }
            Message::Commit {
                key,
                id,
                tag,
                value,
            } => {
                if tag.counter == 0
                    || tag < id
                    || tag.writer != id.writer
                    || tag.incarnation != id.incarnation
                {
                    return Err(KvError::protocol("invalid Gus commit tag"));
                }
                if from != id.writer
                    && id.writer == self.context.node_id
                    && self.active.read().unwrap().contains(&id)
                {
                    return Ok(Reply::Busy);
                }
                if from != id.writer
                    && self
                        .state
                        .read()
                        .unwrap()
                        .get(&key)
                        .is_some_and(|s| s.versions.values().any(|v| v.id == id && v.tag > tag))
                {
                    return Ok(Reply::Retagged);
                }
                let found = self.state.read().unwrap().get(&key).and_then(|s| {
                    s.offers
                        .get(&id)
                        .cloned()
                        .or_else(|| s.versions.values().find(|v| v.id == id).cloned())
                });
                let Some(mut version) = found.or(value) else {
                    return Ok(Reply::NeedValue);
                };
                if version.key != key || version.id != id {
                    return Err(KvError::protocol("invalid Gus commit value"));
                }
                version.tag = tag;
                self.apply(version, false).await?;
                Ok(Reply::Committed)
            }
        }
    }
    async fn commit_remote(self: &Arc<Self>, key: &str, version: Version) -> Result<(), KvError> {
        let mut calls = JoinSet::new();
        for &node in self.context.config.replicas(key) {
            if node != self.context.node_id {
                let core = self.clone();
                let version = version.clone();
                let activity = Activity::new(self.context.metrics.clone());
                let (done, wait) = oneshot::channel();
                tokio::spawn(async move {
                    let _activity = activity;
                    let message = Message::Commit {
                        key: version.key.clone(),
                        id: version.id,
                        tag: version.tag,
                        value: None,
                    };
                    let result = async {
                        match core.call(node, message).await? {
                            Reply::Committed => Ok(()),
                            Reply::NeedValue => match core
                                .call(
                                    node,
                                    Message::Commit {
                                        key: version.key.clone(),
                                        id: version.id,
                                        tag: version.tag,
                                        value: Some(version),
                                    },
                                )
                                .await?
                            {
                                Reply::Committed => Ok(()),
                                _ => Err(KvError::protocol("invalid Gus repair ACK")),
                            },
                            _ => Err(KvError::protocol("invalid Gus commit ACK")),
                        }
                    }
                    .await;
                    let _ = done.send(result);
                });
                calls.spawn(async move {
                    wait.await
                        .unwrap_or_else(|_| Err(KvError::unavailable("Gus commit stopped")))
                });
            }
        }
        while let Some(Ok(result)) = calls.join_next().await {
            if result.is_ok() {
                return result;
            }
        }
        Err(KvError::unavailable("Gus retag quorum unavailable"))
    }
    async fn execute_owned(self: &Arc<Self>, operation: Operation) -> Result<Outcome, KvError> {
        let key = operation.key().to_string();
        // The paper assumes sequential operations for each local writer. Reads also wait
        // for that writer, so they cannot promote its unfinished speculative value.
        let gate = self
            .state
            .write()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .gate
            .clone();
        let _guards = if matches!(operation, Operation::Get { .. }) {
            (Some(gate.read().await), None)
        } else {
            (None, Some(gate.write().await))
        };
        match operation {
            Operation::Get { .. } => {
                loop {
                    let local = self.latest(&key);
                    let (node, reply) = self
                        .one_remote(&key, Message::Query { key: key.clone() })
                        .await?;
                    let Reply::Tag(remote) = reply else {
                        return Err(KvError::protocol("invalid Gus query reply"));
                    };
                    self.receipt(&key, remote, node);
                    let target = local.max(remote);
                    if target.counter == 0 {
                        self.stats.fast_reads.fetch_add(1, Ordering::Relaxed);
                        return Ok(Outcome::Missing);
                    }
                    let fast = {
                        let states = self.state.read().unwrap();
                        states.get(&key).and_then(|s| {
                            s.versions
                                .range(target..)
                                .rev()
                                .find(|(tag, _)| {
                                    s.views.get(tag).is_some_and(|v| {
                                        v.contains(&self.context.node_id) && v.len() >= 2
                                    })
                                })
                                .map(|(_, v)| v.clone())
                        })
                    };
                    if let Some(v) = fast {
                        self.stats.fast_reads.fetch_add(1, Ordering::Relaxed);
                        return Ok(v.value.map_or(Outcome::Missing, Outcome::Value));
                    }
                    // n=3 optimization: fetch the quorum's newer durable value and persist
                    // it at the reader before counting this reader as a second durable copy.
                    let version = if remote >= local {
                        match self
                            .call(
                                node,
                                Message::Fetch {
                                    key: key.clone(),
                                    tag: remote,
                                },
                            )
                            .await?
                        {
                            Reply::Version(Some(v)) => v,
                            _ => return Err(KvError::unavailable("Gus value not available")),
                        }
                    } else {
                        self.version(&key, local)
                            .ok_or_else(|| KvError::storage("Gus latest value missing"))?
                    };
                    if remote < local {
                        // The local value lacks proof of a second copy. Propagate it to the
                        // contacted replica rather than treating a speculative tag as safe.
                        match self
                            .call(
                                node,
                                Message::Commit {
                                    key: key.clone(),
                                    id: version.id,
                                    tag: version.tag,
                                    value: Some(version.clone()),
                                },
                            )
                            .await?
                        {
                            Reply::Committed => (),
                            Reply::Busy | Reply::Retagged => {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                                continue;
                            }
                            _ => return Err(KvError::protocol("Gus read recovery failed")),
                        }
                    } else {
                        self.apply(version.clone(), false).await?;
                    }
                    self.receipt(&key, version.tag, node);
                    self.stats.transfers.fetch_add(1, Ordering::Relaxed);
                    return Ok(version.value.map_or(Outcome::Missing, Outcome::Value));
                }
            }
            mutation => {
                let prior = self.latest(&key);
                let old = self
                    .counter
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        n.max(prior.counter).checked_add(1)
                    })
                    .map_err(|_| KvError::unavailable("Gus timestamp exhausted"))?;
                let id = Tag {
                    counter: old.max(prior.counter) + 1,
                    writer: self.context.node_id,
                    incarnation: self.incarnation.load(Ordering::Acquire),
                };
                self.active.write().unwrap().insert(id);
                let _active = Active {
                    core: self.clone(),
                    id,
                };
                let (value, outcome) = match mutation {
                    Operation::Put { value, .. } => (Some(value), Outcome::Written),
                    Operation::Delete { .. } => (None, Outcome::Deleted),
                    _ => unreachable!(),
                };
                let (_, reply) = self
                    .one_remote(
                        &key,
                        Message::Offer {
                            key: key.clone(),
                            id,
                            value: value.clone(),
                        },
                    )
                    .await?;
                let Reply::Offer(observed) = reply else {
                    return Err(KvError::protocol("invalid Gus offer ACK"));
                };
                let mut version = Version {
                    key: key.clone(),
                    id,
                    tag: id,
                    value,
                    installed: true,
                    observed: prior,
                };
                if observed >= id {
                    self.stats.retags.fetch_add(1, Ordering::Relaxed);
                    version.tag.counter = observed
                        .counter
                        .checked_add(1)
                        .ok_or_else(|| KvError::unavailable("Gus timestamp exhausted"))?;
                    // Fix the tag before sending it; this task survives the HTTP future.
                    self.apply(version.clone(), false).await?;
                    self.commit_remote(&key, version).await?;
                } else {
                    self.stats.fast_writes.fetch_add(1, Ordering::Relaxed);
                    self.apply(version, false).await?;
                }
                Ok(outcome)
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
                .map_err(|_| KvError::unavailable("Gus admission full"))?;
            let core = self.clone();
            let activity = Activity::new(core.context.metrics.clone());
            let (done, wait) = oneshot::channel();
            tokio::spawn(async move {
                let _activity = activity;
                let _permit = permit;
                let _ = done.send(core.execute(operation).await);
            });
            timeout(
                Duration::from_millis(self.context.config.rpc_timeout_ms),
                wait,
            )
            .await
            .map_err(|_| KvError::unavailable("Gus deadline; mutation outcome unknown"))?
            .map_err(|_| KvError::unavailable("Gus task stopped"))?
        })
    }
    async fn execute(self: &Arc<Self>, operation: Operation) -> Result<Outcome, KvError> {
        let key = operation.key().to_string();
        let owners = self.context.config.replicas(&key);
        if owners.contains(&self.context.node_id) {
            return self.execute_owned(operation).await;
        }
        let reply = self
            .router
            .forward(
                &self.context,
                &key,
                serde_json::to_value(Message::Forward { operation })?,
            )
            .await?;
        match serde_json::from_value(reply)? {
            Reply::Outcome(outcome) => Ok(outcome),
            _ => Err(KvError::protocol("invalid Gus forwarded outcome")),
        }
    }
}
impl Coordinator for GusCoordinator {
    fn name(&self) -> &'static str {
        "gus"
    }
    fn diagnostics(&self) -> Value {
        let s = &self.core.stats;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let states = self.core.state.read().unwrap();
        json!({"fast_writes":get(&s.fast_writes),"retags":get(&s.retags),"fast_reads":get(&s.fast_reads),"read_transfers":get(&s.transfers),"view_messages":get(&s.views),"background_errors":get(&s.background_errors),"retained_versions":states.values().map(|s|s.versions.len()+s.offers.len()).sum::<usize>()})
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
                        .ok_or_else(|| KvError::storage("invalid Gus incarnation"))
                })
                .transpose()?
                .unwrap_or(0);
            let next = old
                .checked_add(1)
                .ok_or_else(|| KvError::storage("Gus incarnation exhausted"))?;
            self.core
                .context
                .store
                .commit(vec![Record {
                    key: BOOT.into(),
                    state: json!(next),
                }])
                .await?;
            let records = self.core.context.store.records("_gus/version/")?;
            let mut states = self.core.state.write().unwrap();
            for (_, record) in records {
                let v: Version = serde_json::from_value(record)?;
                let state = states.entry(v.key.clone()).or_default();
                if v.installed {
                    state.latest = state.latest.max(v.tag);
                    state
                        .views
                        .entry(v.tag)
                        .or_default()
                        .insert(self.core.context.node_id);
                    state.versions.insert(v.tag, v.clone());
                }
                if v.tag == v.id {
                    state.offers.insert(v.id, v);
                }
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
