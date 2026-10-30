use crate::types::{KvError, NodeId};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::SocketAddr, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub id: NodeId,
    pub http: SocketAddr,
    pub peer: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub coordinator: String,
    pub nodes: Vec<NodeConfig>,
    /// Explicit ordered replica sets. The local baseline requires singleton sets.
    pub shards: Vec<Vec<NodeId>>,
    #[serde(default = "default_timeout")]
    pub rpc_timeout_ms: u64,
    /// Benchmark-only symmetric per-RPC delay, applied concurrently at dispatch/reply.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub benchmark_peer_rtt_ms: u64,
}
fn is_zero(n: &u64) -> bool {
    *n == 0
}
fn default_timeout() -> u64 {
    2000
}

impl ClusterConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let config: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), KvError> {
        if self.cluster_id.is_empty()
            || self.coordinator.is_empty()
            || self.nodes.is_empty()
            || self.shards.is_empty()
            || self.rpc_timeout_ms == 0
            || self.benchmark_peer_rtt_ms > 10_000
        {
            return Err(KvError::invalid(
                "cluster, coordinator, nodes, shards and positive timeout are required",
            ));
        }
        let mut ids = HashSet::new();
        let mut addresses = HashSet::new();
        for node in &self.nodes {
            if !ids.insert(node.id) {
                return Err(KvError::invalid("duplicate node id"));
            }
            for address in [node.http, node.peer] {
                if address.port() == 0
                    || address.ip().is_unspecified()
                    || !addresses.insert(address)
                {
                    return Err(KvError::invalid(
                        "node addresses must be distinct, connectable and use nonzero ports",
                    ));
                }
            }
        }
        for replicas in &self.shards {
            let mut seen = HashSet::new();
            if replicas.is_empty()
                || replicas
                    .iter()
                    .any(|id| !ids.contains(id) || !seen.insert(id))
            {
                return Err(KvError::invalid(
                    "each shard needs distinct owners present in nodes",
                ));
            }
        }
        Ok(())
    }

    pub fn node(&self, id: NodeId) -> Result<&NodeConfig, KvError> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| KvError::invalid(format!("unknown node {id}")))
    }
    pub fn shard(&self, key: &str) -> usize {
        (stable_hash(key.as_bytes()) % self.shards.len() as u64) as usize
    }
    pub fn replicas(&self, key: &str) -> &[NodeId] {
        &self.shards[self.shard(key)]
    }
}

/// FNV-1a 64-bit with fixed constants; placement is independent of process hash seeds.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placement_hash_has_stable_vectors() {
        assert_eq!(stable_hash(b""), 0xcbf29ce484222325);
        assert_eq!(stable_hash(b"hello"), 0xa430d84680aabd0b);
    }
}
