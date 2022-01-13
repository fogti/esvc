use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;

// NOTE: avoid changing these data types, it would influence the data format
// which means that hashes or some other relevant stuff would change.
// We don't want that. (especially this one)
#[derive(Clone, Debug, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct Event {
    pub name: u128,
    pub arg: Vec<u8>,
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventWithDeps {
    pub ev: Event,
    pub deps: BTreeSet<u128>,
}

/// used to resolve command ids to actions
pub trait Context {
    type State: Clone + PartialEq;
    type Error: std::error::Error;

    /// execute an event, recording its results
    fn execute(self, data: Self::State, ev: &Event) -> Result<Self::State, Self::Error>;
}

mod apply;
pub use apply::*;

mod hash;
pub use hash::*;
