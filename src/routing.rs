//! Cached owner forwarding. Probe retries are safe; forwarded mutations are never retried.
use crate::{coordinator::CoordinatorContext, types::KvError};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::atomic::Ordering, time::Duration};
use tokio::{sync::Mutex, task::JoinSet, time::timeout};
#[derive(Default)]
pub struct ReplicaRouter {
    routes: Mutex<HashMap<Vec<u64>, u64>>,
}
impl ReplicaRouter {
    pub async fn forward(
        &self,
        context: &CoordinatorContext,
        key: &str,
        payload: Value,
    ) -> Result<Value, KvError> {
        let owners = context.config.replicas(key);
        let cached = self.routes.lock().await.get(owners).copied();
        let target = if let Some(target) = cached {
            target
        } else {
            let mut calls = JoinSet::new();
            for &node in owners {
                let transport = context.transport.clone();
                let probe = json!({"kind":"probe","key":key});
                let deadline = Duration::from_millis(context.config.rpc_timeout_ms);
                calls.spawn(async move {
                    (
                        node,
                        timeout(deadline, transport.request(node, probe)).await,
                    )
                });
            }
            let mut selected = None;
            while let Some(result) = calls.join_next().await {
                if let Ok((node, Ok(Ok(reply)))) = result
                    && reply["kind"] == "pong"
                {
                    selected = Some(node);
                    break;
                }
            }
            let target = selected.ok_or_else(|| KvError::unavailable("no owner reachable"))?;
            self.routes.lock().await.insert(owners.to_vec(), target);
            target
        };
        context.metrics.forwarded.fetch_add(1, Ordering::Relaxed);
        let result = timeout(
            Duration::from_millis(context.config.rpc_timeout_ms),
            context.transport.request(target, payload),
        )
        .await
        .unwrap_or_else(|_| {
            Err(KvError::unavailable(
                "forward deadline; mutation outcome unknown",
            ))
        });
        if result.is_err() {
            self.routes.lock().await.remove(owners);
        }
        result
    }
}
