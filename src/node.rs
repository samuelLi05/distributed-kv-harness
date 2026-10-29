use crate::{
    config::ClusterConfig,
    coordinator::{Coordinator, CoordinatorContext, CoordinatorFactory, LocalCoordinator},
    metrics::Metrics,
    store::JournalStore,
    transport::{FaultController, TcpTransport, serve_peers},
    types::{ErrorKind, KvError, MAX_VALUE_BYTES, NodeId, Operation, Outcome},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::Path as FilePath,
    sync::{Arc, atomic::Ordering},
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

#[derive(Clone)]
struct AppState {
    coordinator: Arc<dyn Coordinator>,
    metrics: Arc<Metrics>,
    config: Arc<ClusterConfig>,
    node_id: NodeId,
}

pub struct RunningNode {
    pub http_address: SocketAddr,
    pub peer_address: SocketAddr,
    pub coordinator: Arc<dyn Coordinator>,
    pub metrics: Arc<Metrics>,
    pub faults: Arc<FaultController>,
    pub store: Arc<JournalStore>,
    // Keep the connection pool alive independently of a coordinator's implementation.
    _transport: Arc<TcpTransport>,
    stop: watch::Sender<bool>,
    http: Option<JoinHandle<Result<(), std::io::Error>>>,
    peer: Option<JoinHandle<()>>,
}

impl RunningNode {
    pub async fn start(
        config: Arc<ClusterConfig>,
        node_id: NodeId,
        directory: impl AsRef<FilePath>,
    ) -> Result<Self, KvError> {
        let node = config.node(node_id)?;
        let http = TcpListener::bind(node.http).await?;
        let peer = TcpListener::bind(node.peer).await?;
        let factory: CoordinatorFactory = match config.coordinator.as_str() {
            "abd" | "abd_basic" => crate::abd::AbdCoordinator::build,
            _ => LocalCoordinator::build,
        };
        Self::with_listeners(config, node_id, directory, http, peer, factory).await
    }

    /// The extension seam for a new protocol. Tests can supply prebound ephemeral listeners.
    pub async fn with_listeners(
        config: Arc<ClusterConfig>,
        node_id: NodeId,
        directory: impl AsRef<FilePath>,
        http_listener: TcpListener,
        peer_listener: TcpListener,
        factory: CoordinatorFactory,
    ) -> Result<Self, KvError> {
        config.validate()?;
        let node = config.node(node_id)?;
        let http_address = http_listener.local_addr()?;
        let peer_address = peer_listener.local_addr()?;
        if http_address != node.http || peer_address != node.peer {
            return Err(KvError::invalid(
                "listeners do not match configured addresses",
            ));
        }
        let metrics = Arc::new(Metrics::default());
        let identity =
            json!({"format": "saronic-kv-harness-v1", "node_id": node_id, "cluster": &*config});
        let store = JournalStore::open(directory, identity, metrics.clone())?;
        let transport = TcpTransport::new(config.clone(), node_id, metrics.clone());
        let context = CoordinatorContext {
            node_id,
            config: config.clone(),
            store: store.clone(),
            transport: transport.clone(),
            metrics: metrics.clone(),
        };
        let coordinator = match factory(context) {
            Ok(coordinator) if coordinator.name() == config.coordinator => coordinator,
            Ok(_) => {
                store.close().await;
                return Err(KvError::invalid(
                    "coordinator factory name does not match configuration",
                ));
            }
            Err(error) => {
                store.close().await;
                return Err(error);
            }
        };
        if let Err(error) = coordinator.initialize().await {
            store.close().await;
            return Err(error);
        }
        let state = AppState {
            coordinator: coordinator.clone(),
            metrics: metrics.clone(),
            config: config.clone(),
            node_id,
        };
        let app = Router::new()
            .route("/{key}", get(read).put(write).delete(delete))
            .route("/__harness/status", get(status))
            .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES))
            .with_state(state);
        let (stop, mut stopping) = watch::channel(false);
        let peer = tokio::spawn(serve_peers(
            peer_listener,
            node_id,
            config,
            coordinator.clone(),
            metrics.clone(),
            stop.subscribe(),
        ));
        let http = tokio::spawn(async move {
            axum::serve(http_listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopping.wait_for(|value| *value).await;
                })
                .await
        });
        Ok(Self {
            http_address,
            peer_address,
            coordinator,
            metrics,
            faults: transport.faults.clone(),
            store,
            _transport: transport,
            stop,
            http: Some(http),
            peer: Some(peer),
        })
    }

    pub async fn shutdown(mut self) -> Result<(), KvError> {
        let _ = self.stop.send(true);
        if let Some(task) = self.http.take() {
            task.await
                .map_err(|e| KvError::unavailable(e.to_string()))??;
        }
        if let Some(task) = self.peer.take() {
            task.await
                .map_err(|e| KvError::unavailable(e.to_string()))?;
        }
        self.store.close().await;
        Ok(())
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = &self.http {
            task.abort();
        }
        if let Some(task) = &self.peer {
            task.abort();
        }
    }
}

async fn read(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    invoke(state, Operation::Get { key }).await
}

async fn write(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    match body {
        Ok(Json(value)) => invoke(state, Operation::Put { key, value }).await,
        Err(error) => {
            state.metrics.http_requests.fetch_add(1, Ordering::Relaxed);
            state.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
            (error.status(), Json(json!({"error": error.body_text()}))).into_response()
        }
    }
}

async fn delete(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    invoke(state, Operation::Delete { key }).await
}

async fn status(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "node_id": state.node_id,
        "coordinator": state.coordinator.name(),
        "guarantee": match state.coordinator.name() { "local" => "unreplicated_single_owner", "abd" | "abd_basic" => "static_rf3_linearizable_durable_quorum", _ => "coordinator_defined" },
        "shards": state.config.shards,
        "metrics": state.metrics.snapshot(),
        "process": crate::resources::usage(),
    }))
}

async fn invoke(state: AppState, operation: Operation) -> Response {
    state.metrics.http_requests.fetch_add(1, Ordering::Relaxed);
    let result = match operation.validate() {
        Ok(()) => state.coordinator.execute(operation.clone()).await,
        Err(error) => Err(error),
    };
    let response = match (operation, result) {
        (Operation::Get { .. }, Ok(Outcome::Value(value))) if value.is_object() => {
            Json(value).into_response()
        }
        (Operation::Get { .. }, Ok(Outcome::Missing)) => StatusCode::NOT_FOUND.into_response(),
        (Operation::Put { .. }, Ok(Outcome::Written)) => StatusCode::CREATED.into_response(),
        (Operation::Delete { .. }, Ok(Outcome::Deleted)) => StatusCode::NO_CONTENT.into_response(),
        (_, Err(error)) => error_response(error),
        _ => error_response(KvError::protocol(
            "coordinator returned an invalid operation outcome",
        )),
    };
    if response.status().is_client_error() || response.status().is_server_error() {
        state.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
    }
    response
}

fn error_response(error: KvError) -> Response {
    let status = match error.kind {
        ErrorKind::Invalid => StatusCode::BAD_REQUEST,
        ErrorKind::Unavailable | ErrorKind::Timeout | ErrorKind::Overloaded => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        ErrorKind::Storage => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::Protocol => StatusCode::BAD_GATEWAY,
    };
    (status, Json(error)).into_response()
}
