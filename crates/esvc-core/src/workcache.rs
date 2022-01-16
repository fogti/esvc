use crate::{Event, Graph, GraphError, Hash, IncludeSpec};
use esvc_traits::Engine;
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

// NOTE: the elements of this *must* be public, because the user needs to be
// able to deconstruct it if they want to modify the engine
// (e.g. to register a new command at runtime)
pub struct WorkCache<'a, En: Engine> {
    pub engine: &'a En,
    pub sts: BTreeMap<BTreeSet<Hash>, <En as Engine>::Dat>,
}

impl<'a, En: Engine> core::clone::Clone for WorkCache<'a, En> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine,
            sts: self.sts.clone(),
        }
    }

    fn clone_from(&mut self, other: &Self) {
        self.engine = other.engine;
        self.sts.clone_from(&other.sts);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkCacheError<EE> {
    #[error("engine couldn't find command with ID {0}")]
    CommandNotFound(u32),

    #[error(transparent)]
    Graph(#[from] GraphError),

    #[error(transparent)]
    Engine(EE),
}

pub type RunResult<'a, En> =
    Result<(&'a <En as Engine>::Dat, BTreeSet<Hash>), WorkCacheError<<En as Engine>::Error>>;

impl<'a, En: Engine> WorkCache<'a, En> {
    pub fn new(engine: &'a En, init_data: En::Dat) -> Self {
        let mut sts = BTreeMap::new();
        sts.insert(BTreeSet::new(), init_data);
        Self { engine, sts }
    }

    /// this returns an error if `tt` is not present in `sts`.
    pub fn run_recursively(
        &mut self,
        graph: &Graph<En::Arg>,
        mut tt: BTreeSet<Hash>,
        main_evid: Hash,
        incl: IncludeSpec,
    ) -> RunResult<'_, En> {
        // heap of necessary dependencies
        let mut deps = vec![main_evid];

        let mut data: En::Dat = (*self.sts.get(&tt).ok_or(GraphError::DatasetNotFound)?).clone();

        while let Some(evid) = deps.pop() {
            if tt.contains(&evid) {
                // nothing to do
                continue;
            } else if evid == main_evid && !deps.is_empty() {
                return Err(GraphError::DependencyCircuit(main_evid).into());
            }

            let evwd = graph
                .events
                .get(&evid)
                .ok_or(GraphError::DependencyNotFound(evid))?;
            let mut necessary_deps = evwd.deps.difference(&tt);
            if let Some(&x) = necessary_deps.next() {
                deps.push(evid);
                // TODO: check for dependency cycles
                deps.push(x);
                deps.extend(necessary_deps.copied());
            } else {
                if evid == main_evid && incl != IncludeSpec::IncludeAll {
                    // we want to omit the final dep
                    break;
                }

                // run the item, all dependencies are satisfied
                use std::collections::btree_map::Entry;
                // TODO: check if `data...clone()` is a bottleneck.
                match self.sts.entry({
                    let mut tmp = tt.clone();
                    tmp.insert(evid);
                    tmp
                }) {
                    Entry::Occupied(o) => {
                        // reuse cached entry
                        data = o.get().clone();
                    }
                    Entry::Vacant(v) => {
                        // create cache entry
                        data = self
                            .engine
                            .run_event_bare(evwd.cmd, &evwd.arg, &data)
                            .map_err(WorkCacheError::Engine)?;
                        v.insert(data.clone());
                    }
                }
                tt.insert(evid);
            }
        }

        let res = self.sts.get(&tt).unwrap();
        Ok((res, tt))
    }

    pub fn run_foreach_recursively(
        &mut self,
        graph: &Graph<En::Arg>,
        evids: BTreeMap<Hash, IncludeSpec>,
    ) -> RunResult<'_, En> {
        let tt = evids
            .into_iter()
            .try_fold(BTreeSet::new(), |tt, (i, idspec)| {
                self.run_recursively(graph, tt, i, idspec)
                    .map(|(_, new_tt)| new_tt)
            })?;
        let res = self.sts.get(&tt).unwrap();
        Ok((res, tt))
    }

    /// NOTE: this ignores the contents of `ev.deps`
    pub fn shelve_event(
        &mut self,
        graph: &mut Graph<En::Arg>,
        mut seed_deps: BTreeSet<Hash>,
        ev: Event<En::Arg>,
    ) -> Result<Option<Hash>, WorkCacheError<En::Error>> {
        // check `ev` for independence
        #[derive(Clone, Copy, PartialEq)]
        enum DepSt {
            Use,
            Deny,
        }
        let mut cur_deps = BTreeMap::new();
        let engine = self.engine;

        while !seed_deps.is_empty() {
            let mut new_seed_deps = BTreeSet::new();
            // calculate cur state
            let (base_st, _) = self.run_foreach_recursively(
                graph,
                seed_deps
                    .iter()
                    .chain(
                        cur_deps
                            .iter()
                            .filter(|&(_, &s)| s == DepSt::Use)
                            .map(|(h, _)| h),
                    )
                    .filter(|i| cur_deps.get(i) != Some(&DepSt::Deny))
                    .map(|&i| (i, IncludeSpec::IncludeAll))
                    .collect(),
            )?;
            let cur_st = engine
                .run_event_bare(ev.cmd, &ev.arg, base_st)
                .map_err(WorkCacheError::Engine)?;
            if cur_deps.is_empty() && base_st == &cur_st {
                // this is a no-op event, we can't handle it anyways.
                return Ok(None);
            }

            for &conc_evid in &seed_deps {
                if cur_deps.contains_key(&conc_evid) {
                    continue;
                }
                // calculate base state = cur - conc
                let (base_st, _) = self.run_foreach_recursively(
                    graph,
                    seed_deps
                        .iter()
                        .chain(
                            cur_deps
                                .iter()
                                .filter(|&(_, s)| s == &DepSt::Use)
                                .map(|(h, _)| h),
                        )
                        .map(|&i| {
                            (
                                i,
                                if i == conc_evid {
                                    IncludeSpec::IncludeOnlyDeps
                                } else {
                                    IncludeSpec::IncludeAll
                                },
                            )
                        })
                        .collect(),
                )?;
                let conc_ev = graph.events.get(&conc_evid).unwrap();
                let is_indep = if &cur_st == base_st {
                    // this is a NOP, needs to be skipped to avoid invalid
                    // reorderings later
                    true
                } else if ev.cmd == conc_ev.cmd && ev.arg == conc_ev.arg {
                    // necessary for non-idempotent events (e.g. s/0/0000/g)
                    // base_st + conc = cur_st, so we detect if conc has an effect
                    // even if it was already applied (case above)
                    false
                } else {
                    engine
                        .run_event_bare(ev.cmd, &ev.arg, base_st)
                        .and_then(|next_st| {
                            self.engine
                                .run_event_bare(conc_ev.cmd, &conc_ev.arg, &next_st)
                        })
                        .map_err(WorkCacheError::Engine)?
                        == cur_st
                };
                if is_indep {
                    // independent -> move backward
                    new_seed_deps.extend(conc_ev.deps.iter().copied());
                } else {
                    // not independent -> move forward
                    // make sure that we don't overwrite `deny` entries
                    cur_deps.entry(conc_evid).or_insert(DepSt::Use);
                    cur_deps.extend(conc_ev.deps.iter().map(|&dep| (dep, DepSt::Deny)));
                }
            }
            seed_deps = new_seed_deps;
        }

        // mangle deps
        let ev = Event {
            cmd: ev.cmd,
            arg: ev.arg,
            deps: cur_deps
                .into_iter()
                .flat_map(|(dep, st)| if st == DepSt::Use { Some(dep) } else { None })
                .collect(),
        };

        // register event
        let (collinfo, evhash) = graph.ensure_event(ev);
        if let Some(ev) = collinfo {
            return Err(GraphError::HashCollision(evhash, format!("{:?}", ev)).into());
        }

        Ok(Some(evhash))
    }

    pub fn check_if_mergable(
        &mut self,
        graph: &Graph<En::Arg>,
        sts: BTreeSet<Hash>,
    ) -> Result<Option<Self>, WorkCacheError<En::Error>> {
        // we run this recursively (and non-parallel), which is a bit unfortunate,
        // but we get the benefit that we can share the cache...
        let bases = sts
            .iter()
            .map(|&h| {
                self.run_recursively(graph, BTreeSet::new(), h, IncludeSpec::IncludeAll)
                    .map(|r| (h, r.1))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        // calculate 2d matrix
        let ret = bases
            .iter()
            .enumerate()
            .flat_map(|(ni, (_, i))| {
                sts.iter()
                    .enumerate()
                    .filter(move |(nj, _)| ni != *nj)
                    .map(|(_, &j)| (i.clone(), j))
            })
            .collect::<Vec<_>>()
            .into_par_iter()
            // source: https://sts10.github.io/2019/06/06/is-all-equal-function.html
            .try_fold(|| (true, None), {
                |acc: (bool, Option<_>), (i, j)| {
                    if !acc.0 {
                        return Ok((false, None));
                    }
                    let mut this = self.clone();
                    this.run_recursively(graph, i, j, IncludeSpec::IncludeAll)?;
                    let elem = this.sts;
                    Ok(if acc.1.map(|prev| prev == elem).unwrap_or(true) {
                        (true, Some(elem))
                    } else {
                        (false, None)
                    })
                }
            })
            .collect::<Result<Vec<_>, WorkCacheError<_>>>()?
            .into_iter()
            .flat_map(|(uacc, x)| x.map(|y| (uacc, y)))
            .fold((true, None), {
                |acc, (uacc, elem)| {
                    let is_mrgb = uacc && acc.0 && acc.1.map(|prev| prev == elem).unwrap_or(true);
                    (is_mrgb, if is_mrgb { Some(elem) } else { None })
                }
            });
        Ok(ret.1.map(|sts| Self {
            engine: self.engine,
            sts,
        }))
    }
}
