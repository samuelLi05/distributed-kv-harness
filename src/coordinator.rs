use crate::{
    config::ClusterConfig,
    metrics::Metrics,
    store::{JournalStore, Record},
    transport::Transport,
    types::{BoxFuture, KvError, NodeId, Operation, Outcome},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, atomic::Ordering};

/// A protocol owns admission, read eligibility, completion and interpretation of durable state.
/// Implementations must validate peer payloads and tolerate dropped/cancelled client futures.
pub trait Coordinator: Send + Sync {
    fn name(&self) -> &'static str;
    fn execute(&self, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>>;
    fn on_peer(&self, from: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>>;
}

#[derive(Clone)]
pub struct CoordinatorContext {
    pub node_id: NodeId,
    pub config: Arc<ClusterConfig>,
    pub store: Arc<JournalStore>,
    pub transport: Arc<dyn Transport>,
    pub metrics: Arc<Metrics>,
}

pub type CoordinatorFactory = fn(CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError>;

/// Single-owner baseline: routing and durability work, replication and failover are NOT provided.
pub struct LocalCoordinator {
    context: CoordinatorContext,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalState {
    value: Option<Value>,
}

impl LocalCoordinator {
    pub fn build(context: CoordinatorContext) -> Result<Arc<dyn Coordinator>, KvError> {
        if context.config.coordinator != "local"
            || context.config.shards.iter().any(|owners| owners.len() != 1)
        {
            return Err(KvError::invalid(
                "local coordinator requires coordinator=local and exactly one owner per shard; it is unreplicated",
            ));
        }
        Ok(Arc::new(Self { context }))
    }

    async fn execute_owned(&self, operation: Operation) -> Result<Outcome, KvError> {
        operation.validate()?;
        if self.context.config.replicas(operation.key()) != [self.context.node_id] {
            return Err(KvError::protocol(
                "forwarded operation did not reach its configured owner",
            ));
        }
        match operation {
            Operation::Get { key } => match self.context.store.get(&key)? {
                Some(record) => {
                    let state: LocalState = serde_json::from_value(record)?;
                    Ok(state.value.map_or(Outcome::Missing, Outcome::Value))
                }
                None => Ok(Outcome::Missing),
            },
            Operation::Put { key, value } => {
                self.context
                    .store
                    .commit(vec![Record {
                        key,
                        state: serde_json::to_value(LocalState { value: Some(value) })?,
                    }])
                    .await?;
                Ok(Outcome::Written)
            }
            Operation::Delete { key } => {
                self.context
                    .store
                    .commit(vec![Record {
                        key,
                        state: serde_json::to_value(LocalState { value: None })?,
                    }])
                    .await?;
                Ok(Outcome::Deleted)
            }
        }
    }
}

impl Coordinator for LocalCoordinator {
    fn name(&self) -> &'static str {
        "local"
    }
    fn execute(&self, operation: Operation) -> BoxFuture<'_, Result<Outcome, KvError>> {
        Box::pin(async move {
            operation.validate()?;
            let owner = self.context.config.replicas(operation.key())[0];
            if owner == self.context.node_id {
                return self.execute_owned(operation).await;
            }
            self.context
                .metrics
                .forwarded
                .fetch_add(1, Ordering::Relaxed);
            let response = self
                .context
                .transport
                .request(owner, serde_json::to_value(operation)?)
                .await?;
            Ok(serde_json::from_value(response)?)
        })
    }
    fn on_peer(&self, _from: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            let operation: Operation = serde_json::from_value(payload)?;
            // Do not re-forward peer operations: an inconsistent route fails instead of looping.
            Ok(serde_json::to_value(self.execute_owned(operation).await?)?)
        })
    }
}
