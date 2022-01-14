use crate::Hash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// NOTE: avoid changing these data types, it would influence the data format
// which means that hashes or some other relevant stuff would change.
// We don't want that. (especially this one)

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Event {
    pub cmd: u32,
    pub arg: Vec<u8>,
    pub deps: BTreeSet<Hash>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Graph {
    pub events: BTreeMap<Hash, Event>,

    /// cmds entries contain WASM bytecode
    pub cmds: Vec<Vec<u8>>,

    /// saved combined states
    pub nstates: BTreeMap<String, BTreeSet<Hash>>,
}

impl Graph {
    /// fold a state, expanding of compressing it along the dependencies.
    /// `st` entries should be initialized to `false` when creating a state from a `BTreeSet<Hash>`.
    pub fn fold_state(
        &self,
        mut st: BTreeMap<Hash, bool>,
        expand: bool,
    ) -> Option<BTreeMap<Hash, bool>> {
        loop {
            let orig_len = st.len();
            let mut errs = false;
            st.extend(
                st.clone()
                    .into_iter()
                    .flat_map(|(i, _)| match self.events.get(&i) {
                        Some(x) => Some(x.deps.iter().map(|&j| (j, true))),
                        None => {
                            errs = true;
                            None
                        }
                    })
                    .flatten(),
            );
            if errs {
                return None;
            }
            if orig_len != st.len() {
                break;
            }
        }
        if !expand {
            // keep only non-dependencies
            st.retain(|_, is_dep| !*is_dep);
        }
        Some(st)
    }

    /// get-or-insert event, check if it matches
    pub fn ensure_event(&mut self, ev: &Event) -> bool {
        let serval = bincode::serialize::<Event>(ev).unwrap();
        let h = crate::calculate_hash(&serval[..]);
        use std::collections::btree_map::Entry;
        match self.events.entry(h) {
            Entry::Occupied(o) => o.get() == ev,
            Entry::Vacant(v) => {
                v.insert(ev.clone());
                true
            }
        }
    }
}
