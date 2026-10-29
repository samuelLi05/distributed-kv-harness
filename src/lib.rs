//! Infrastructure for coordination experiments. The shipped coordinator is NOT replicated.
pub mod config;
pub mod coordinator;
pub mod metrics;
pub mod node;
pub mod store;
pub mod transport;
pub mod types;

pub use config::{ClusterConfig, NodeConfig};
pub use coordinator::{Coordinator, CoordinatorContext, CoordinatorFactory, LocalCoordinator};
pub use node::RunningNode;
pub use types::{KvError, Operation, Outcome};
