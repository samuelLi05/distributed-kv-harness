//! Static registry: protocol selection and declared contracts, separate from implementation state.
use crate::{
    coordinator::{CoordinatorFactory, LocalCoordinator},
    types::KvError,
};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Descriptor {
    pub name: &'static str,
    pub replicas: usize,
    pub guarantee: &'static str,
    pub majority_progress: bool,
    pub clock_independent_safety: bool,
    pub experimental: bool,
    pub ingress: &'static str,
}

pub const PROTOCOLS: &[Descriptor] = &[
    Descriptor {
        name: "local",
        replicas: 1,
        guarantee: "unreplicated_single_owner",
        majority_progress: false,
        clock_independent_safety: true,
        experimental: false,
        ingress: "owner_forwarding",
    },
    Descriptor {
        name: "abd_basic",
        replicas: 3,
        guarantee: "static_rf3_linearizable_durable_quorum",
        majority_progress: true,
        clock_independent_safety: true,
        experimental: false,
        ingress: "direct_quorum",
    },
    Descriptor {
        name: "abd",
        replicas: 3,
        guarantee: "static_rf3_linearizable_durable_quorum",
        majority_progress: true,
        clock_independent_safety: true,
        experimental: false,
        ingress: "direct_quorum",
    },
];
pub fn descriptor(name: &str) -> Result<&'static Descriptor, KvError> {
    PROTOCOLS
        .iter()
        .find(|d| d.name == name)
        .ok_or_else(|| KvError::invalid(format!("unknown coordinator {name}")))
}
pub fn factory(name: &str) -> Result<CoordinatorFactory, KvError> {
    match descriptor(name)?.name {
        "local" => Ok(LocalCoordinator::build),
        "abd" | "abd_basic" => Ok(crate::abd::AbdCoordinator::build),
        _ => Err(KvError::invalid("coordinator not built")),
    }
}
