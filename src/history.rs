//! Bounded exhaustive register-history checking. Unknown mutations may take effect or be omitted.
use crate::{Operation, Outcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

/// One recorder supplies comparable logical invocation/response times across concurrent clients.
pub struct Recorder {
    bound: usize,
    state: Mutex<(u64, Vec<Entry>)>,
}
impl Recorder {
    pub fn new(bound: usize) -> Self {
        Self {
            bound,
            state: Mutex::new((0, Vec::new())),
        }
    }
    pub fn begin(&self, operation: Operation) -> Result<usize, crate::KvError> {
        let mut state = self.state.lock().unwrap();
        if state.1.len() >= self.bound {
            return Err(crate::KvError::invalid("history recorder full"));
        }
        state.0 += 1;
        let start = state.0;
        let ticket = state.1.len();
        state.1.push(Entry {
            operation,
            start,
            end: None,
            outcome: None,
        });
        Ok(ticket)
    }
    /// Pass None for an uncertain result; its mutation remains possible in the checker.
    pub fn finish(&self, ticket: usize, outcome: Option<Outcome>) -> Result<(), crate::KvError> {
        let mut state = self.state.lock().unwrap();
        if ticket >= state.1.len() || state.1[ticket].end.is_some() {
            return Err(crate::KvError::invalid(
                "invalid or already completed history ticket",
            ));
        }
        state.0 += 1;
        let end = state.0;
        state.1[ticket].end = Some(end);
        state.1[ticket].outcome = outcome;
        Ok(())
    }
    pub fn snapshot(&self) -> Vec<Entry> {
        self.state.lock().unwrap().1.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub operation: Operation,
    pub start: u64,
    /// Actual response time, including errors. Unknown outcomes have no completion constraint.
    pub end: Option<u64>,
    pub outcome: Option<Outcome>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum Verdict {
    Valid,
    Invalid,
    Inconclusive,
}

/// Histories start with absent keys; use complete bounded experiments, not arbitrary trace windows.
pub fn check(history: &[Entry]) -> Verdict {
    let mut keys = BTreeMap::<&str, Vec<&Entry>>::new();
    for entry in history {
        if entry.operation.validate().is_err()
            || entry.end.is_some_and(|end| end <= entry.start)
            || (entry.outcome.is_some() && entry.end.is_none())
        {
            return Verdict::Invalid;
        }
        keys.entry(entry.operation.key()).or_default().push(entry);
    }
    for entries in keys.values() {
        if entries.len() > 24 {
            return Verdict::Inconclusive;
        }
        let required = entries.iter().enumerate().fold(0, |mask, (i, e)| {
            mask | if e.outcome.is_some() { 1 << i } else { 0 }
        });
        let mut seen = HashSet::new();
        match search(entries, required, 0, None, &mut seen) {
            Some(true) => (),
            Some(false) => return Verdict::Invalid,
            None => return Verdict::Inconclusive,
        }
    }
    Verdict::Valid
}

fn search(
    entries: &[&Entry],
    required: u32,
    done: u32,
    value: Option<Value>,
    seen: &mut HashSet<(u32, String)>,
) -> Option<bool> {
    if done & required == required {
        return Some(true);
    }
    if seen.len() >= 100_000 {
        return None;
    }
    if !seen.insert((done, serde_json::to_string(&value).unwrap())) {
        return Some(false);
    }
    for (i, entry) in entries.iter().enumerate() {
        if done & (1 << i) != 0 {
            continue;
        }
        // A completed operation preceding this invocation must have been linearized first.
        if entries.iter().enumerate().any(|(j, other)| {
            done & (1 << j) == 0
                && other.outcome.is_some()
                && other.end.is_some_and(|end| end <= entry.start)
        }) {
            continue;
        }
        let (next, expected) = match &entry.operation {
            Operation::Get { .. } => (
                value.clone(),
                value.clone().map_or(Outcome::Missing, Outcome::Value),
            ),
            Operation::Put { value, .. } => (Some(value.clone()), Outcome::Written),
            Operation::Delete { .. } => (None, Outcome::Deleted),
        };
        if entry
            .outcome
            .as_ref()
            .is_some_and(|actual| *actual != expected)
        {
            continue;
        }
        if search(entries, required, done | (1 << i), next, seen)? {
            return Some(true);
        }
    }
    Some(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detects_stale_reads_and_retains_unknown_writes() {
        let put = Entry {
            operation: Operation::Put {
                key: "x".into(),
                value: json!({"v":1}),
            },
            start: 1,
            end: Some(2),
            outcome: Some(Outcome::Written),
        };
        let get = Entry {
            operation: Operation::Get { key: "x".into() },
            start: 3,
            end: Some(4),
            outcome: Some(Outcome::Missing),
        };
        assert_eq!(check(&[put.clone(), get.clone()]), Verdict::Invalid);
        let mut unknown = put;
        unknown.outcome = None;
        assert_eq!(check(&[unknown.clone(), get.clone()]), Verdict::Valid);
        let mut observed = get.clone();
        observed.outcome = Some(Outcome::Value(json!({"v":1})));
        assert_eq!(check(&[unknown.clone(), observed.clone()]), Verdict::Valid);
        let later = Entry {
            start: 5,
            end: Some(6),
            ..get
        };
        assert_eq!(check(&[unknown, observed, later]), Verdict::Invalid);
    }

    #[test]
    fn concurrent_replacements_and_deletion_are_linearizable() {
        let entries = vec![
            Entry {
                operation: Operation::Put {
                    key: "x".into(),
                    value: json!({"v":1}),
                },
                start: 1,
                end: Some(5),
                outcome: Some(Outcome::Written),
            },
            Entry {
                operation: Operation::Delete { key: "x".into() },
                start: 2,
                end: Some(4),
                outcome: Some(Outcome::Deleted),
            },
            Entry {
                operation: Operation::Get { key: "x".into() },
                start: 6,
                end: Some(7),
                outcome: Some(Outcome::Missing),
            },
        ];
        assert_eq!(check(&entries), Verdict::Valid);
        assert_eq!(check(&vec![entries[0].clone(); 25]), Verdict::Inconclusive);
    }
}
