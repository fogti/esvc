use crate::{Event, Graph, GraphError, Hash, IncludeSpec};
use core::fmt;
use esvc_traits::Engine;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "tracing")]
use tracing::{event, Level};

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

impl<En: Engine> fmt::Debug for WorkCache<'_, En> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkCache")
            .field("sts", &self.sts)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkCacheError<EE> {
    #[error("engine couldn't find command with ID {0}")]
    CommandNotFound(u32),

    #[error(transparent)]
    Graph(#[from] GraphError),

    #[error("event {0}: merge failed, new resulting hash was {1}")]
    HashChangeAtMerge(Hash, Hash),

    #[error("event {0} got turned into a no-op at merge")]
    NoopAtMerge(Hash),

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

    /// invariant: `deps` and `tt` are distinct
    fn run_deps(
        &mut self,
        graph: &Graph<En::Arg>,
        mut tt: BTreeSet<Hash>,
        deps: Vec<Hash>,
    ) -> RunResult<'_, En> {
        let mut data: En::Dat = (*self.sts.get(&tt).ok_or(GraphError::DatasetNotFound)?).clone();

        for &evid in &deps {
            let evwd = graph
                .events
                .get(&evid)
                .ok_or(GraphError::DependencyNotFound(evid))?;

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

        let res = self.sts.get(&tt).unwrap();
        Ok((res, tt))
    }

    pub fn run_foreach_recursively(
        &mut self,
        graph: &Graph<En::Arg>,
        evids: BTreeMap<Hash, IncludeSpec>,
    ) -> RunResult<'_, En> {
        let deps = graph.calculate_dependencies(Default::default(), evids)?;
        self.run_deps(graph, Default::default(), deps)
    }

    /// NOTE: this ignores the contents of `ev.deps`
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(seed_deps)))]
    pub fn shelve_event(
        &mut self,
        graph: &mut Graph<En::Arg>,
        mut seed_deps: BTreeSet<Hash>,
        mut ev: Event<En::Arg>,
    ) -> Result<Option<Hash>, WorkCacheError<En::Error>> {
        ev.deps.clear();
        // check `ev` for independence
        #[derive(Clone, Copy, PartialEq)]
        enum DepSt {
            Use,
            UseSoft,
            Deny,
        }
        let mut cur_deps = BTreeMap::new();
        let engine = self.engine;

        // calculate expected state
        let (base_st, _base_tt) = self.run_foreach_recursively(
            graph,
            seed_deps
                .iter()
                .map(|&i| (i, IncludeSpec::IncludeAll))
                .collect(),
        )?;
        let cur_st = engine
            .run_event_bare(ev.cmd, &ev.arg, base_st)
            .map_err(WorkCacheError::Engine)?;

        #[cfg(feature = "tracing")]
        event!(
            Level::TRACE,
            "from {:?} constructed expected state {:?} +cur> {:?}",
            _base_tt,
            base_st,
            cur_st
        );

        if cur_deps.is_empty() && base_st == &cur_st {
            // this is a no-op event, we can't handle it anyways.
            return Ok(None);
        }

        while !seed_deps.is_empty() {
            let mut new_seed_deps = BTreeSet::<Hash>::new();

            #[cfg(feature = "tracing")]
            let trc_span = tracing::span!(Level::DEBUG, "fe-seeds", ?seed_deps);

            #[cfg(feature = "tracing")]
            let _enter = trc_span.enter();

            seed_deps = seed_deps
                .into_iter()
                .filter(|conc_evid| !cur_deps.contains_key(conc_evid))
                .collect();

            // calculate cur state
            let (base_st, _base_tt) = self.run_foreach_recursively(
                graph,
                seed_deps
                    .iter()
                    .filter(|&i| cur_deps.get(i) != Some(&DepSt::Deny))
                    .chain(
                        cur_deps
                            .iter()
                            .filter(|&(_, &s)| s == DepSt::Use)
                            .map(|(h, _)| h),
                    )
                    .map(|&i| (i, IncludeSpec::IncludeAll))
                    .collect(),
            )?;
            let cur_st = engine
                .run_event_bare(ev.cmd, &ev.arg, base_st)
                .map_err(WorkCacheError::Engine)?;

            let mut extra_new_seed_deps = BTreeSet::new();

            #[cfg(feature = "tracing")]
            event!(
                Level::TRACE,
                "from {:?} constructed state {:?} +cur> {:?}",
                _base_tt,
                base_st,
                cur_st
            );

            if cur_deps.is_empty() && base_st == &cur_st {
                // this is a no-op event, we can't handle it anyways.
                return Ok(None);
            }

            let seed_deps2 = seed_deps
                .iter()
                .map(|&conc_evid| {
                    Ok((
                        conc_evid,
                        // calculate base state = cur - conc
                        self.run_foreach_recursively(
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
                        )?
                        .1,
                    ))
                })
                .filter(|maybe_stuff| {
                    match maybe_stuff {
                        Ok((conc_evid, tmptt)) => {
                            if tmptt.contains(conc_evid) {
                                // if some other dependency pulls in this,
                                // then skip it for now, as it will be added
                                // to the next seed if necessary
                                // TODO: add a unit test for this
                                #[cfg(feature = "tracing")]
                                event!(
                                    Level::TRACE,
                                    "{} is pulled in multiple times, skip",
                                    conc_evid
                                );
                                // to make sure that we don't accidentially hit the
                                // 'necessary dep got lost' if the dependee gets dropped.
                                extra_new_seed_deps.insert(*conc_evid);
                                false
                            } else {
                                true
                            }
                        }
                        Err(_) => true,
                    }
                })
                .collect::<Result<BTreeMap<_, _>, WorkCacheError<_>>>()?;

            for (conc_evid, tmptt) in seed_deps2 {
                let base_st = self.sts.get(&tmptt).unwrap();
                let conc_ev = graph.events.get(&conc_evid).unwrap();
                #[allow(clippy::if_same_then_else, clippy::let_and_return)]
                let is_indep = if &cur_st == base_st {
                    // this is a revert
                    #[cfg(feature = "tracing")]
                    event!(Level::TRACE, "{} is revert", conc_evid);
                    false
                } else if ev.cmd == conc_ev.cmd && ev.arg == conc_ev.arg {
                    // necessary for non-idempotent events (e.g. s/0/0000/g)
                    // base_st + conc = cur_st, so we detect if conc has an effect
                    // even if it was already applied (case above)
                    #[cfg(feature = "tracing")]
                    event!(Level::TRACE, "{} is non-idempotent", conc_evid);
                    false
                } else {
                    let evfirst = engine
                        .run_event_bare(ev.cmd, &ev.arg, base_st)
                        .map_err(WorkCacheError::Engine)?;
                    let evfirst_then = engine
                        .run_event_bare(conc_ev.cmd, &conc_ev.arg, &evfirst)
                        .map_err(WorkCacheError::Engine)?;
                    // we need to make sure that this event does not make merging
                    // later impossible because another event gets inapplicable.
                    let res = evfirst != evfirst_then && evfirst_then == cur_st;
                    #[cfg(feature = "tracing")]
                    if !res {
                        event!(
                            Level::TRACE,
                            "cur_st={:?} vs. evfirst={:?}",
                            cur_st,
                            evfirst
                        );
                    }
                    res
                };
                #[cfg(feature = "tracing")]
                event!(
                    Level::TRACE,
                    "{} is {}dependent",
                    conc_evid,
                    if is_indep { "in" } else { "" }
                );
                if is_indep {
                    // independent -> move backward
                    new_seed_deps.extend(conc_ev.deps.keys().copied());
                } else {
                    // not independent -> move forward
                    // make sure that we don't overwrite `deny` entries
                    cur_deps.entry(conc_evid).or_insert(DepSt::Use);
                    cur_deps.extend(
                        conc_ev
                            .deps
                            .iter()
                            .filter(|(_, &is_hard)| is_hard)
                            .map(|(&dep, _)| (dep, DepSt::Deny)),
                    );
                }
            }

            if extra_new_seed_deps != seed_deps {
                new_seed_deps.extend(extra_new_seed_deps);
            } else {
                #[cfg(feature = "tracing")]
                event!(
                    Level::TRACE,
                    ?extra_new_seed_deps,
                    "extra seed deps dropped to prevent infinite loop",
                );
            }

            // check if we haven't missed any essential dependency
            let (bare_st, bare_tt) = self.run_foreach_recursively(
                graph,
                new_seed_deps
                    .iter()
                    .filter(|&i| cur_deps.get(i) != Some(&DepSt::Deny))
                    .chain(
                        cur_deps
                            .iter()
                            .filter(|&(_, &s)| s == DepSt::Use)
                            .map(|(h, _)| h),
                    )
                    .map(|&i| (i, IncludeSpec::IncludeAll))
                    .collect(),
            )?;
            let mut tmp_st = engine
                .run_event_bare(ev.cmd, &ev.arg, bare_st)
                .map_err(WorkCacheError::Engine)?;
            seed_deps = seed_deps.difference(&bare_tt).copied().collect();
            for &conc_evid in &seed_deps {
                let conc_ev = graph.events.get(&conc_evid).unwrap();
                tmp_st = engine
                    .run_event_bare(conc_ev.cmd, &conc_ev.arg, &tmp_st)
                    .map_err(WorkCacheError::Engine)?;
            }
            if cur_st != tmp_st {
                // some necessary dependency got lost
                // to avoid any dependency on concrete hash value ordering or such here,
                // just simply add all current seed deps to the necessary set
                // we can avoid entry juggling here because all entries might end up as `deny`
                // should be already present in `bare_tt` and thus already filtered.
                #[cfg(feature = "tracing")]
                event!(
                    Level::TRACE,
                    ?bare_tt,
                    bare_st = ?(*self.sts.get(&bare_tt).unwrap()),
                    ?cur_st,
                    ?tmp_st,
                    ?seed_deps,
                    "some necessary dependency got lost, stopping here",
                );
                assert!(cur_deps
                    .iter()
                    .filter(|&(_, &s)| matches!(s, DepSt::Deny | DepSt::Use))
                    .all(|(h, _)| !seed_deps.contains(h)));
                cur_deps.extend(seed_deps.into_iter().map(|h| (h, DepSt::UseSoft)));
                break;
            } else {
                // reduction successful
                seed_deps = new_seed_deps;
            }
        }

        // mangle deps
        let ev = Event {
            cmd: ev.cmd,
            arg: ev.arg,
            deps: cur_deps
                .into_iter()
                .flat_map(|(dep, st)| match st {
                    DepSt::Use => Some((dep, true)),
                    DepSt::UseSoft => Some((dep, false)),
                    DepSt::Deny => None,
                })
                .collect(),
        };

        // register event
        let (collinfo, evhash) = graph.ensure_event(ev);
        if let Some(ev) = collinfo {
            return Err(GraphError::HashCollision(evhash, format!("{:?}", ev)).into());
        }

        Ok(Some(evhash))
    }

    pub fn try_merge(
        &mut self,
        graph: &mut Graph<En::Arg>,
        sts: BTreeSet<Hash>,
    ) -> Result<(), WorkCacheError<En::Error>>
    where
        En::Arg: Clone,
    {
        // TODO: make this more effective

        let full_seed_deps: BTreeSet<_> = graph
            .calculate_dependencies(
                Default::default(),
                sts.iter()
                    .map(|&h| (h, IncludeSpec::IncludeOnlyDeps))
                    .collect(),
            )?
            .into_iter()
            .collect();

        let mut seed_deps: BTreeSet<_> = graph
            .fold_state(full_seed_deps.iter().map(|&h| (h, false)).collect(), false)?
            .into_iter()
            .map(|(h, _)| h)
            .collect();

        #[cfg(feature = "tracing")]
        event!(Level::TRACE, ?full_seed_deps, ?seed_deps, "merge seeds");

        for i in sts {
            if full_seed_deps.contains(&i) {
                continue;
            }
            let ev = graph.events[&i].clone();
            if let Some(ih) = self.shelve_event(graph, seed_deps.clone(), ev)? {
                if ih != i {
                    let ev = graph.events[&i].clone();
                    let nev = graph.events[&ih].clone();
                    if nev
                        .deps
                        .iter()
                        .filter(|(_, is_hard)| **is_hard)
                        .collect::<Vec<_>>()
                        != ev
                            .deps
                            .iter()
                            .filter(|(_, is_hard)| **is_hard)
                            .collect::<Vec<_>>()
                    {
                        // carry on, only soft deps changed.
                    } else {
                        return Err(WorkCacheError::HashChangeAtMerge(i, ih));
                    }
                }
                seed_deps.insert(i);
            } else {
                return Err(WorkCacheError::NoopAtMerge(i));
            }
        }
        Ok(())
    }
}

// this is somewhat equivalent to the fuzzer code,
// and is used to test known edge cases
#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone, Debug, PartialEq, serde::Serialize)]
    struct SearEvent<'a>(&'a str, &'a str);

    impl<'a> From<SearEvent<'a>> for Event<SearEvent<'a>> {
        fn from(ev: SearEvent<'a>) -> Self {
            Event {
                cmd: 0,
                arg: ev,
                deps: Default::default(),
            }
        }
    }

    struct SearEngine;

    impl Engine for SearEngine {
        type Error = ();
        type Arg = SearEvent<'static>;
        type Dat = String;

        fn run_event_bare(&self, cmd: u32, arg: &SearEvent, dat: &String) -> Result<String, ()> {
            assert_eq!(cmd, 0);
            Ok(dat.replace(&arg.0, &arg.1))
        }
    }

    fn assert_no_reorder_inner(start: &str, sears: Vec<SearEvent<'static>>) {
        let expected = sears
            .iter()
            .fold(start.to_string(), |acc, item| acc.replace(&item.0, &item.1));
        let e = SearEngine;
        let mut g = Graph::default();
        let mut w = WorkCache::new(&e, start.to_string());
        let mut xs = BTreeSet::new();
        for i in sears {
            if let Some(h) = w
                .shelve_event(&mut g, xs.clone(), i.into())
                .expect("unable to shelve event")
            {
                xs.insert(h);
            }
        }

        let minx: BTreeSet<_> = g
            .fold_state(xs.iter().map(|&y| (y, false)).collect(), false)
            .unwrap()
            .into_iter()
            .map(|x| x.0)
            .collect();

        let evs: BTreeMap<_, _> = minx
            .iter()
            .map(|&i| (i, crate::IncludeSpec::IncludeAll))
            .collect();

        let (got, tt) = w.run_foreach_recursively(&g, evs.clone()).unwrap();
        assert_eq!(xs, tt);
        assert_eq!(*got, expected);
    }

    fn optional_tracing(f: impl FnOnce()) {
        #[cfg(feature = "tracing")]
        tracing::subscriber::with_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(std::io::stderr)
                .finish(),
            f,
        );
        #[cfg(not(feature = "tracing"))]
        f();
    }

    fn assert_no_reorder(start: &str, sears: Vec<SearEvent<'static>>) {
        optional_tracing(|| assert_no_reorder_inner(start, sears))
    }

    #[test]
    fn equal_but_non_idempotent() {
        assert_no_reorder(
            "x",
            vec![
                SearEvent("x", "xx"),
                SearEvent("x", "xx"),
                SearEvent("x", "y"),
            ],
        );
    }

    #[test]
    fn indirect_dep() {
        assert_no_reorder(
            "Hi, what's up??",
            vec![
                SearEvent("Hi", "Hello UwU"),
                SearEvent("UwU", "World"),
                SearEvent("what", "wow"),
                SearEvent("s up", "sup"),
                SearEvent("??", "!"),
                SearEvent("sup!", "soap?"),
                SearEvent("p", "np"),
            ],
        );
    }

    #[test]
    fn revert_then() {
        assert_no_reorder(
            "a",
            vec![
                SearEvent("a", "xaa"),
                SearEvent("xa", ""),
                SearEvent("a", "bbbbb"),
            ],
        );
    }

    #[test]
    fn diverg_mult_steps() {
        assert_no_reorder(
            "XXXXX",
            vec![
                SearEvent("X", "XXXX"),
                SearEvent("X", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
                SearEvent("XXXXXXXXXX", "XXXXXXXXXXXXXX"),
            ],
        );
    }

    #[test]
    fn diverg_mult_steps2() {
        // 920 vs 1288
        assert_no_reorder(
            // 5
            "\0\0\0\0\0",
            vec![
                SearEvent(
                    //  1 ->  4
                    "\0",
                    "\0\0\0\0",
                ),
                SearEvent(
                    //  1 -> 46
                    "\0",
                    "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
                ),
                // if the last event is reordered to the start,
                // then it fails to apply
                SearEvent(
                    // 10 -> 14
                    "\0\0\0\0\0\0\0\0\0\0",
                    "\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
                ),
            ],
        );
    }

    fn assert_simple_merge(
        start: &str,
        dest: &str,
        common_sears: Vec<SearEvent<'static>>,
        tomerge_sears: Vec<SearEvent<'static>>,
    ) {
        optional_tracing(|| {
            let e = SearEngine;
            let mut g = Graph::default();
            let mut w = WorkCache::new(&e, start.to_string());
            let mut xs = BTreeSet::new();
            for i in common_sears {
                let x = w
                    .shelve_event(&mut g, xs.clone(), i.into())
                    .unwrap()
                    .unwrap();
                xs.insert(x);
            }
            let oldxs = xs.clone();
            for i in tomerge_sears {
                let x = w
                    .shelve_event(&mut g, oldxs.clone(), i.into())
                    .unwrap()
                    .unwrap();
                xs.insert(x);
            }
            let _ = oldxs;

            if let Err(e) = w.try_merge(&mut g, xs.clone()) {
                #[cfg(feature = "tracing")]
                event!(Level::TRACE, ?w, ?g, "state after try_merge",);
                panic!("merge failed: {:?}", e);
            }

            assert_eq!(
                w.run_foreach_recursively(
                    &g,
                    xs.into_iter()
                        .map(|h| (h, IncludeSpec::IncludeAll))
                        .collect()
                )
                .expect("unable to compute final result")
                .0,
                dest
            );
        });
    }

    #[test]
    fn basic_merge() {
        assert_simple_merge(
            "A|B|C",
            "E|D|F",
            vec![SearEvent("B", "D")],
            vec![SearEvent("A|D", "E|D"), SearEvent("D|C", "D|F")],
        );
    }

    #[test]
    fn merge2() {
        assert_simple_merge(
            "XXXX",
            r#"fn main() {
    println!("Hewwo UwU!");
    println!("Hello World!");
}"#,
            vec![SearEvent(
                "XXXX",
                r#"fn main() {
    println!("Hewwo!");
    println!("Hello Wrold!");
}"#,
            )],
            vec![SearEvent("o!", "o UwU!"), SearEvent("Wrold", "World")],
        );
    }

    #[test]
    fn merge_after_clear() {
        optional_tracing(|| {
            let e = SearEngine;
            let mut g = Graph::default();
            let mut w = WorkCache::new(&e, "X".to_string());
            let mut xs = BTreeSet::new();
            let mut xsv = Vec::new();
            for i in [SearEvent("X", "XXX"), SearEvent("X", "")] {
                let x = w
                    .shelve_event(&mut g, xs.clone(), i.into())
                    .unwrap()
                    .unwrap();
                xs.insert(x);
                xsv.push(x);
            }

            if let Err(e) = w.try_merge(&mut g, xs.clone()) {
                #[cfg(feature = "tracing")]
                event!(Level::TRACE, ?w, ?g, "state after try_merge",);
                panic!("merge failed: {:?}", e);
            }

            assert_eq!(
                w.run_foreach_recursively(
                    &g,
                    xs.into_iter()
                        .map(|h| (h, IncludeSpec::IncludeAll))
                        .collect()
                )
                .expect("unable to compute final result")
                .0,
                ""
            );
        });
    }

    #[test]
    fn merge_after_clear2() {
        optional_tracing(|| {
            let e = SearEngine;
            let mut g = Graph::default();
            let mut w = WorkCache::new(&e, "\0".to_string());
            let mut xs = BTreeSet::new();
            let mut xsv = Vec::new();
            for i in [
                SearEvent("\0", "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
                SearEvent("\0", ""),
            ] {
                let x = w
                    .shelve_event(&mut g, xs.clone(), i.into())
                    .unwrap()
                    .unwrap();
                xs.insert(x);
                xsv.push(x);
            }

            #[cfg(feature = "tracing")]
            event!(Level::TRACE, ?w, ?g, "checkpoint before merge");
            if let Err(e) = w.try_merge(&mut g, xs.clone()) {
                #[cfg(feature = "tracing")]
                event!(Level::TRACE, ?w, ?g, "state after try_merge");
                panic!("merge failed: {:?}", e);
            }

            assert_eq!(
                w.run_foreach_recursively(
                    &g,
                    xs.into_iter()
                        .map(|h| (h, IncludeSpec::IncludeAll))
                        .collect()
                )
                .expect("unable to compute final result")
                .0,
                ""
            );
        });
    }
}
