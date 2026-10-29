use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, future::Future, pin::Pin};

pub type NodeId = u64;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Get { key: String },
    Put { key: String, value: Value },
    Delete { key: String },
}

impl Operation {
    pub fn key(&self) -> &str {
        match self {
            Self::Get { key } | Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }

    pub fn validate(&self) -> Result<(), KvError> {
        if self.key().is_empty() || self.key().len() > MAX_KEY_BYTES {
            return Err(KvError::invalid("key must contain 1..=1024 UTF-8 bytes"));
        }
        if let Self::Put { value, .. } = self {
            if !value.is_object() {
                return Err(KvError::invalid("PUT requires a JSON object"));
            }
            if serde_json::to_vec(value)?.len() > MAX_VALUE_BYTES {
                return Err(KvError::invalid("JSON object exceeds 1 MiB"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum Outcome {
    Value(Value),
    Missing,
    Written,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    Invalid,
    Unavailable,
    Timeout,
    Storage,
    Protocol,
    Overloaded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvError {
    pub kind: ErrorKind,
    pub message: String,
}

impl KvError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, message)
    }
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }
    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Storage, message)
    }
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, message)
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for KvError {}
impl From<std::io::Error> for KvError {
    fn from(e: std::io::Error) -> Self {
        Self::storage(e.to_string())
    }
}
impl From<serde_json::Error> for KvError {
    fn from(e: serde_json::Error) -> Self {
        Self::protocol(e.to_string())
    }
}
