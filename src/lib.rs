//! Infrastructure for durable coordination experiments: local baseline and static RF=3 MW-ABD.
pub mod abd;
pub mod activity;
pub mod config;
pub mod coordinator;
pub mod history;
pub mod http_client;
pub mod metrics;
pub mod node;
pub mod protocol;
pub mod resources;
pub mod simulation;
pub mod store;
pub mod transport;
pub mod types;

pub use abd::AbdCoordinator;
pub use config::{ClusterConfig, NodeConfig};
pub use coordinator::{Coordinator, CoordinatorContext, CoordinatorFactory, LocalCoordinator};
pub use node::RunningNode;
pub use types::{KvError, Operation, Outcome};
