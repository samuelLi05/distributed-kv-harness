//! Static RF=3 MW-ABD. No leader, clocks, configuration changes, or local-only reads.
//! Replica apply workers own compare/persist/ACK, including after requester cancellation.
use crate::{
    config::stable_hash,
    coordinator::{Coordinator, CoordinatorContext},
    store::{JournalStore, Record},
    types::{BoxFuture, ErrorKind, KvError, Operation, Outcome},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};

const BOOT: &str = "_abd/system/incarnation";
const STRIPES: usize = 32;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Tag {
    pub counter: u64,
    pub writer: u64,
    pub incarnation: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Register {
    pub tag: Tag,
    pub value: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Message {
    QueryTag { key: String },
    QueryValue { key: String },
    Store { key: String, state: Register },
}
impl Message {
    fn key(&self) -> &str {
        match self {
            Self::QueryTag { key } | Self::QueryValue { key } | Self::Store { key, .. } => key,
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Reply {
    Tag(Tag),
    Value(Register),
    Stored(Tag),
}

struct Apply {
    key: String,
    proposed: Register,
    reply: oneshot::Sender<Result<Tag, KvError>>,
    _activity: crate::activity::Activity,
}
struct Core {
    context: CoordinatorContext,
    apply: Vec<mpsc::Sender<Apply>>,
    incarnation: AtomicU64,
    counter: AtomicU64,
    capacity: Semaphore,
}
pub struct AbdCoordinator {
    core: Arc<Core>,
    fast_reads: bool,
}

impl AbdCoordinator {
    pub fn build(context: CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError> {
        if !matches!(context.config.coordinator.as_str(), "abd" | "abd_basic")
            || context.config.shards.iter().any(|r| r.len() != 3)
        {
            return Err(KvError::invalid(
                "ABD requires coordinator=abd or abd_basic and exactly three replicas per shard",
            ));
        }
        let fast_reads = context.config.coordinator == "abd";
        let mut apply = Vec::new();
        for _ in 0..STRIPES {
            let (tx, rx) = mpsc::channel(8);
            apply.push(tx);
            tokio::spawn(apply_worker(context.store.clone(), rx));
        }
        Ok(Arc::new(Self {
            core: Arc::new(Core {
                context,
                apply,
                incarnation: AtomicU64::new(0),
                counter: AtomicU64::new(0),
                capacity: Semaphore::new(256),
            }),
            fast_reads,
        }))
    }
}

fn data_key(key: &str) -> String {
    format!("_abd/data/{key}")
}
fn read(store: &JournalStore, key: &str) -> Result<Register, KvError> {
    store
        .get(&data_key(key))?
        .map_or(Ok(Register::default()), |v| {
            serde_json::from_value(v)
                .map_err(|e| KvError::storage(format!("invalid ABD record: {e}")))
        })
}
async fn apply_worker(store: Arc<JournalStore>, mut rx: mpsc::Receiver<Apply>) {
    while let Some(command) = rx.recv().await {
        let result = async {
            let current = read(&store, &command.key)?;
            if current.tag == command.proposed.tag && current.value != command.proposed.value {
                return Err(KvError::protocol("same tag has different values"));
            }
            if current.tag >= command.proposed.tag {
                return Ok(current.tag);
            }
            store
                .commit(vec![Record {
                    key: data_key(&command.key),
                    state: serde_json::to_value(&command.proposed)?,
                }])
                .await?;
            Ok(command.proposed.tag)
        }
        .await;
        let _ = command.reply.send(result);
    }
}

impl Core {
    async fn replica(&self, message: Message) -> Result<Reply, KvError> {
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
            return Err(KvError::protocol("ABD RPC sent to a nonowner"));
        }
        match message {
            Message::QueryTag { key } => Ok(Reply::Tag(read(&self.context.store, &key)?.tag)),
            Message::QueryValue { key } => Ok(Reply::Value(read(&self.context.store, &key)?)),
            Message::Store { key, state } => {
                if state.tag.counter == 0 {
                    return Err(KvError::protocol("zero tag cannot be stored"));
                }
                if let Some(value) = &state.value {
                    Operation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    }
                    .validate()?;
                }
                let stripe = stable_hash(key.as_bytes()) as usize % STRIPES;
                let (reply, done) = oneshot::channel();
                self.apply[stripe]
                    .send(Apply {
                        key,
                        proposed: state,
                        reply,
                        _activity: crate::activity::Activity::new(self.context.metrics.clone()),
                    })
                    .await
                    .map_err(|_| KvError::storage("apply worker stopped"))?;
                Ok(Reply::Stored(done.await.map_err(|_| {
                    KvError::storage("apply worker stopped before ACK")
                })??))
            }
        }
    }

    async fn quorum(self: &Arc<Self>, message: Message) -> Result<Vec<Reply>, KvError> {
        let mut calls = JoinSet::new();
        for &node in self.context.config.replicas(message.key()) {
            let core = self.clone();
            let message = message.clone();
            calls.spawn(async move {
                let result = if node == core.context.node_id {
                    core.replica(message.clone()).await
                } else {
                    let response = core
                        .context
                        .transport
                        .request(node, serde_json::to_value(&message)?)
                        .await?;
                    serde_json::from_value(response).map_err(KvError::from)
                }?;
                let valid = match (&message, &result) {
                    (Message::QueryTag { .. }, Reply::Tag(_))
                    | (Message::QueryValue { .. }, Reply::Value(_)) => true,
                    (Message::Store { state, .. }, Reply::Stored(tag)) => *tag >= state.tag,
                    _ => false,
                };
                if !valid {
                    return Err(KvError::protocol("invalid ABD reply"));
                }
                Ok::<_, KvError>(result)
            });
        }
        let mut replies = Vec::new();
        while let Some(result) = calls.join_next().await {
            if let Ok(Ok(reply)) = result {
                replies.push(reply);
            }
            if replies.len() == 2 {
                return Ok(replies);
            }
            if replies.len() + calls.len() < 2 {
                break;
            }
        }
        Err(KvError::unavailable(
            "ABD quorum unavailable; mutation may have taken effect",
        ))
    }

    async fn execute(
        self: &Arc<Self>,
        operation: Operation,
        fast: bool,
    ) -> Result<Outcome, KvError> {
        match operation {
            Operation::Get { key } => {
                let replies = self
                    .quorum(Message::QueryValue { key: key.clone() })
                    .await?;
                let states: Vec<_> = replies
                    .into_iter()
                    .filter_map(|r| {
                        if let Reply::Value(s) = r {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .collect();
                let selected = states.iter().max_by_key(|s| s.tag).unwrap().clone();
                if states
                    .iter()
                    .any(|s| s.tag == selected.tag && s.value != selected.value)
                {
                    return Err(KvError::protocol("conflicting values for one tag"));
                }
                if fast && states.iter().all(|s| s == &selected) {
                    self.context
                        .metrics
                        .abd_fast_reads
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.context
                        .metrics
                        .abd_read_repairs
                        .fetch_add(1, Ordering::Relaxed);
                    // The implicit initial state is already present on every replica.
                    if selected.tag.counter != 0 {
                        self.quorum(Message::Store {
                            key,
                            state: selected.clone(),
                        })
                        .await?;
                    } else {
                        self.quorum(Message::QueryTag { key }).await?;
                    }
                }
                Ok(selected.value.map_or(Outcome::Missing, Outcome::Value))
            }
            mutation => {
                let key = mutation.key().to_string();
                let replies = self.quorum(Message::QueryTag { key: key.clone() }).await?;
                let max = replies
                    .into_iter()
                    .filter_map(|r| {
                        if let Reply::Tag(t) = r {
                            Some(t.counter)
                        } else {
                            None
                        }
                    })
                    .max()
                    .unwrap();
                let previous = self
                    .counter
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        n.max(max).checked_add(1)
                    })
                    .map_err(|_| KvError::unavailable("logical timestamp exhausted"))?;
                let counter = previous.max(max) + 1;
                let incarnation = self.incarnation.load(Ordering::Acquire);
                if incarnation == 0 {
                    return Err(KvError::unavailable("ABD has not initialized"));
                }
                let (value, outcome) = match mutation {
                    Operation::Put { value, .. } => (Some(value), Outcome::Written),
                    Operation::Delete { .. } => (None, Outcome::Deleted),
                    _ => unreachable!(),
                };
                self.quorum(Message::Store {
                    key,
                    state: Register {
                        tag: Tag {
                            counter,
                            writer: self.context.node_id,
                            incarnation,
                        },
                        value,
                    },
                })
                .await?;
                Ok(outcome)
            }
        }
    }
}

impl Coordinator for AbdCoordinator {
    fn name(&self) -> &'static str {
        if self.fast_reads { "abd" } else { "abd_basic" }
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
                        .ok_or_else(|| KvError::storage("invalid boot incarnation"))
                })
                .transpose()?
                .unwrap_or(0);
            let next = old
                .checked_add(1)
                .ok_or_else(|| KvError::storage("boot incarnation exhausted"))?;
            self.core
                .context
                .store
                .commit(vec![Record {
                    key: BOOT.into(),
                    state: Value::from(next),
                }])
                .await?;
            self.core.incarnation.store(next, Ordering::Release);
            Ok(())
        })
    }
    fn execute(&self, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        Box::pin(async move {
            operation.validate()?;
            timeout(
                Duration::from_millis(self.core.context.config.rpc_timeout_ms),
                async {
                    let _permit = self
                        .core
                        .capacity
                        .acquire()
                        .await
                        .map_err(|_| KvError::unavailable("coordinator stopped"))?;
                    self.core.execute(operation, self.fast_reads).await
                },
            )
            .await
            .map_err(|_| {
                KvError::new(
                    ErrorKind::Timeout,
                    "ABD deadline; mutation outcome may be unknown",
                )
            })?
        })
    }
    fn on_peer(&self, _from: u64, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            Ok(serde_json::to_value(
                self.core.replica(serde_json::from_value(payload)?).await?,
            )?)
        })
    }
}
