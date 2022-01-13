use crate::Event;
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::{BTreeSet, HashMap};

/// state glue, defines all necessary interactions with state and runners,
/// which execute the events; not async because this is mostly CPU-bound
pub trait State: Clone + PartialEq {
    type Error: std::error::Error;

    /// execute an event, recording its results
    fn run(&mut self, ev: &Event) -> Result<(), Self::Error>;

    /// this check is used to decide if we need to put two events into a chain,
    /// or if we can put them into parallel chains
    fn are_evs_commutative(&self, ev1: &Event, ev2: &Event) -> Result<bool, Self::Error> {
        if ev1 == ev2 {
            return Ok(true);
        }

        // TODO: parallelism?

        let mut a = self.clone();
        a.run(ev1)?;
        a.run(ev2)?;

        let mut b = self.clone();
        b.run(ev2)?;
        b.run(ev1)?;

        Ok(a == b)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HiStateError<SE> {
    #[error("dependency not satisfied: {0:x}")]
    DependencyUnsatisfied(u128),

    #[error("re-run of event {0:x} forbidden")]
    RerunForbidden(u128),

    #[error(transparent)]
    Inner(#[from] SE),
}

/// high-level state, wrapping an implementor of `State`.
///
/// this e.g. keeps track of applied nodes and their relations and such.
#[derive(Clone, Debug, Default, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct HiState<S> {
    pub top: BTreeSet<u128>,
    pub inner: S,
}

impl<S: State> HiState<S> {
    pub fn run(
        &mut self,
        nid: u128,
        deps: &BTreeSet<u128>,
        ev: &Event,
    ) -> Result<(), HiStateError<S::Error>> {
        if self.top.contains(&nid) {
            // this only catches direct reruns
            return Err(HiStateError::RerunForbidden(nid));
        }
        if let Some(&x) = deps.difference(&self.top).next() {
            return Err(HiStateError::DependencyUnsatisfied(x));
        }

        self.inner.run(ev)?;
        Ok(())
    }
}

impl<S> HiState<S> {
    pub fn cleanup_top(&mut self, tags: &HashMap<u128, BTreeSet<u128>>) {
        for (k, v) in tags
            .iter()
            .filter(|(k, v)| !self.top.contains(k) && self.top.is_superset(v))
            .collect::<Vec<_>>()
        {
            let dif = self.top.difference(v);

            #[cfg(debug_assertions)]
            dif.clone().next().unwrap();

            self.top = dif.chain(core::iter::once(k)).copied().collect();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn histate_cleanup_direction() {
        use super::*;
        let mut tmp = HiState::<()>::default();

        tmp.top.insert(1);
        tmp.top.insert(2);
        tmp.top.insert(3);
        tmp.top.insert(4);

        let mut exp = HiState::<()>::default();

        exp.top.insert(1);
        exp.top.insert(3);
        exp.top.insert(5);

        let mut tags = HashMap::new();
        tags.insert(5, {
            let mut x = BTreeSet::new();
            x.insert(2);
            x.insert(4);
            x
        });

        tmp.cleanup_top(&tags);
        assert_eq!(tmp, exp);
    }
}
