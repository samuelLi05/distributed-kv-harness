use distributed_kv_harness::{ClusterConfig, RunningNode};
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut arguments = std::env::args().skip(1);
    let mut config = None;
    let mut node = None;
    let mut directory = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config = arguments.next().map(PathBuf::from),
            "--node" => {
                node = Some(
                    arguments
                        .next()
                        .ok_or("--node requires a value")?
                        .parse::<u64>()?,
                )
            }
            "--data-dir" => directory = arguments.next().map(PathBuf::from),
            "--protocols" => {
                println!(
                    "{}",
                    serde_json::to_string(distributed_kv_harness::protocol::PROTOCOLS)?
                );
                return Ok(());
            }
            "--help" | "-h" => {
                println!(
                    "distributed-kv-harness --config PATH --node ID --data-dir PATH\n\nCoordinators: local, abd_basic, abd, gus, hermes_static, hermes_alr_static. Use --protocols for contracts."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument {argument}; see --help").into()),
        }
    }
    let config = Arc::new(ClusterConfig::load(config.ok_or("--config is required")?)?);
    let node = RunningNode::start(
        config,
        node.ok_or("--node is required")?,
        directory.ok_or("--data-dir is required")?,
    )
    .await?;
    tracing::info!(http = %node.http_address, peer = %node.peer_address, coordinator = node.coordinator.name(), "node ready");
    tokio::signal::ctrl_c().await?;
    node.shutdown().await?;
    Ok(())
}
