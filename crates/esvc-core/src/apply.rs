use crate::Event;
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("dependency not satisfied: {0:x}")]
    DependencyUnsatisfied(u128),

    #[error("re-run of event {0:x} forbidden")]
    RerunForbidden(u128),
}

/// high-level state, wrapping an implementor of `State`.
///
/// this e.g. keeps track of applied nodes and their relations and such.
#[derive(Clone, Debug, Default, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApplyTracker {
    pub top: BTreeSet<u128>,
}

impl ApplyTracker {
    pub fn can_run(&self, nid: u128, deps: &BTreeSet<u128>) -> Result<(), ApplyError> {
        if self.top.contains(&nid) {
            // this only catches direct reruns
            return Err(ApplyError::RerunForbidden(nid));
        }
        if let Some(&x) = deps.difference(&self.top).next() {
            return Err(ApplyError::DependencyUnsatisfied(x));
        }
        Ok(())
    }

    pub fn register_as_ran(&mut self, nid: u128) {
        self.top.insert(nid);
    }

    pub fn apply_tags(&mut self, tags: &HashMap<u128, BTreeSet<u128>>) {
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
    fn applytracker_cleanup_direction() {
        use super::*;
        let mut tmp = ApplyTracker::default();

        tmp.top.insert(1);
        tmp.top.insert(2);
        tmp.top.insert(3);
        tmp.top.insert(4);

        let mut exp = ApplyTracker::default();

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

        tmp.apply_tags(&tags);
        assert_eq!(tmp, exp);
    }
}
