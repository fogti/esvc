use crate::Hash;
use anyhow::anyhow as anyhow_;
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IncludeSpec {
    IncludeAll,
    IncludeOnlyDeps,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
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
            if orig_len == st.len() {
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
    ///
    /// @returns (Some(@arg ev) if collision else None, Hash of @arg ev)
    pub fn ensure_event(&mut self, ev: Event) -> (Option<Event>, Hash) {
        let serval = bincode::serialize::<Event>(&ev).unwrap();
        let h = crate::calculate_hash(&serval[..]);
        use std::collections::btree_map::Entry;
        (
            match self.events.entry(h) {
                Entry::Occupied(o) if o.get() == &ev => None,
                Entry::Occupied(_) => Some(ev),
                Entry::Vacant(v) => {
                    v.insert(ev);
                    None
                }
            },
            h,
        )
    }

    /// utility function for debugging of incorrect evaluation orders
    pub fn debug_exec_order(
        &self,
        evids: BTreeMap<Hash, IncludeSpec>,
    ) -> anyhow::Result<Vec<Hash>> {
        let mut tt = BTreeSet::new();
        let mut ret = Vec::new();
        let mut deps = Vec::new();
        for (main_evid, incl) in evids {
            // heap of necessary dependencies
            deps.push(main_evid);

            while let Some(evid) = deps.pop() {
                if tt.contains(&evid) {
                    // nothing to do
                    continue;
                } else if evid == main_evid && !deps.is_empty() {
                    anyhow::bail!("dependency circuit @ {}", main_evid);
                }

                let evwd = self
                    .events
                    .get(&evid)
                    .ok_or_else(|| anyhow_!("unable to retrieve dependency {}", evid))?;
                let mut necessary_deps = evwd.deps.difference(&tt);
                if let Some(&x) = necessary_deps.next() {
                    deps.push(evid);
                    // TODO: check for dependency cycles
                    deps.push(x);
                    deps.extend(necessary_deps.copied());
                } else {
                    if evid == main_evid && incl != IncludeSpec::IncludeAll {
                        // we want to omit the final dep
                        deps.clear();
                        break;
                    }
                    // run the item, all dependencies are satisfied
                    ret.push(evid);
                    tt.insert(evid);
                }
            }
        }
        Ok(ret)
    }
}

pub fn print_deps<W, DI>(w: &mut W, pfx: &str, deps: DI) -> std::io::Result<()>
where
    DI: Iterator<Item = Hash>,
    W: std::io::Write,
{
    for i in deps {
        writeln!(w, "{}{}", pfx, i)?;
    }
    Ok(())
}
