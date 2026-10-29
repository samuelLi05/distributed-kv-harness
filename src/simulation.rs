//! Explicit RPC-boundary scheduling, independent of the production TCP transport.
//! Save/replay actions against pending message IDs. Tokio and disk execution are not simulated.
use crate::{
    coordinator::Coordinator,
    transport::Transport,
    types::{BoxFuture, KvError, NodeId},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::oneshot;

type Reply = oneshot::Sender<Result<Value, KvError>>;
struct Pending {
    from: NodeId,
    to: NodeId,
    payload: Value,
    reply: Reply,
}
#[derive(Default)]
struct State {
    next: u64,
    peers: BTreeMap<NodeId, Weak<dyn Coordinator>>,
    pending: BTreeMap<u64, Pending>,
    trace: Vec<Step>,
}
#[derive(Default, Clone)]
pub struct Network(Arc<Mutex<State>>);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Step {
    pub id: u64,
    pub copies: usize,
    pub drop_reply: bool,
}

impl Network {
    pub fn register(&self, id: NodeId, coordinator: &Arc<dyn Coordinator>) {
        self.0
            .lock()
            .unwrap()
            .peers
            .insert(id, Arc::downgrade(coordinator));
    }
    pub fn transport(&self, from: NodeId) -> Arc<dyn Transport> {
        Arc::new(Endpoint {
            network: self.clone(),
            from,
        })
    }
    pub fn pending(&self) -> Vec<(u64, NodeId, NodeId, Value)> {
        self.0
            .lock()
            .unwrap()
            .pending
            .iter()
            .map(|(id, p)| (*id, p.from, p.to, p.payload.clone()))
            .collect()
    }
    pub fn trace(&self) -> Vec<Step> {
        self.0.lock().unwrap().trace.clone()
    }
    /// copies=0 loses the request; copies=2 duplicates it; arbitrary ID selection reorders it.
    pub async fn step(&self, step: Step) -> Result<(), KvError> {
        if step.copies > 2 {
            return Err(KvError::invalid("at most two deliveries per step"));
        }
        let (pending, peer) = {
            let mut state = self.0.lock().unwrap();
            let pending = state
                .pending
                .remove(&step.id)
                .ok_or_else(|| KvError::invalid("message not pending"))?;
            let peer = state.peers.get(&pending.to).and_then(Weak::upgrade);
            state.trace.push(step.clone());
            (pending, peer)
        };
        let mut result = Err(KvError::unavailable(
            "simulated request loss or unavailable peer",
        ));
        if let Some(peer) = peer {
            for _ in 0..step.copies {
                result = peer.on_peer(pending.from, pending.payload.clone()).await;
            }
        }
        if step.drop_reply {
            result = Err(KvError::unavailable(
                "simulated reply loss; mutation may have applied",
            ));
        }
        let _ = pending.reply.send(result);
        Ok(())
    }
}
struct Endpoint {
    network: Network,
    from: NodeId,
}
impl Transport for Endpoint {
    fn request(&self, to: NodeId, payload: Value) -> BoxFuture<'_, Result<Value, KvError>> {
        Box::pin(async move {
            let (reply, done) = oneshot::channel();
            {
                let mut state = self.network.0.lock().unwrap();
                if state.pending.len() >= 4096 {
                    return Err(KvError::unavailable("simulation queue full"));
                }
                let id = state.next;
                state.next = state
                    .next
                    .checked_add(1)
                    .ok_or_else(|| KvError::unavailable("simulation id exhausted"))?;
                state.pending.insert(
                    id,
                    Pending {
                        from: self.from,
                        to,
                        payload,
                        reply,
                    },
                );
            }
            done.await
                .map_err(|_| KvError::unavailable("simulated connection closed"))?
        })
    }
}
