use crate::{Event, Graph, Hash, IncludeSpec};
use anyhow::{anyhow as anyhow_, Context};
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

pub trait EngineError: Sized + Sync + Send + Into<anyhow::Error> {}
impl<T: Sync + Send + Into<anyhow::Error>> EngineError for T {}

pub trait Engine: Sync {
    type Command;
    type Error: EngineError;

    /// execute an event of a given data `dat`, ignoring dependencies.
    /// returns `Err` if execution failed, and everything already lookup'ed
    fn run_event_bare(
        &self,
        cmd: &Self::Command,
        arg: &[u8],
        dat: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;

    /// lookup a command in the internal index
    fn resolve_cmd(&self, cmd: u32) -> Option<&Self::Command>;
}

#[derive(Clone)]
pub struct WasmEngine {
    wte: wasmtime::Engine,
    cmds: Vec<wasmtime::Module>,
}

impl Engine for WasmEngine {
    type Command = wasmtime::Module;
    type Error = anyhow::Error;

    fn run_event_bare(
        &self,
        cmd: &wasmtime::Module,
        arg: &[u8],
        dat: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let datlen: i32 = dat
            .len()
            .try_into()
            .map_err(|_| anyhow_!("argument buffer overflow dat.len={}", dat.len()))?;
        let evarglen: i32 = arg
            .len()
            .try_into()
            .map_err(|_| anyhow_!("argument buffer overflow ev.arg.len={}", arg.len()))?;

        // WASM stuff

        let mut store = wasmtime::Store::new(&self.wte, ());
        let instance = wasmtime::Instance::new(&mut store, cmd, &[])?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow_!("unable to get export `memory`"))?;

        let retptr = instance
            .get_typed_func::<i32, i32, _>(&mut store, "__wbindgen_add_to_stack_pointer")?
            .call(&mut store, -16)?;
        let malloc = instance.get_typed_func::<i32, i32, _>(&mut store, "__wbindgen_malloc")?;
        //let free = instance.get_typed_func::<(i32, i32), (), _>(&mut store, "__wbindgen_free")?;

        // transform :: retptr:i32 -> evargptr:i32 -> evarglen:i32 -> datptr:i32 -> datlen:i32 -> ()
        let transform =
            instance.get_typed_func::<(i32, i32, i32, i32, i32), (), _>(&mut store, "transform")?;

        let evargptr = malloc.call(&mut store, evarglen)?;
        memory.write(&mut store, evargptr.try_into()?, arg)?;

        let datptr = malloc.call(&mut store, datlen)?;
        memory.write(&mut store, datptr.try_into()?, dat)?;

        // the main transform call
        let () = transform.call(&mut store, (retptr, evargptr, evarglen, datptr, datlen))?;

        // retrieve results
        let ret = {
            // *retptr :: (retptr2:i32, retlen2:i32)
            let mut retbuf = [0u8; 8];
            memory.read(&mut store, retptr.try_into()?, &mut retbuf)?;
            let (retp0, retp1) = retbuf.split_at(4);
            let retptr2: usize =
                i32::from_le_bytes(<[u8; 4]>::try_from(retp0).unwrap()).try_into()?;
            let retlen2: usize =
                i32::from_le_bytes(<[u8; 4]>::try_from(retp1).unwrap()).try_into()?;
            memory
                .data(&mut store)
                .get(retptr2..retptr2 + retlen2)
                .with_context(|| "return value length out of bounds".to_string())?
                .to_vec()
        };

        Ok(ret)
    }

    fn resolve_cmd(&self, cmd: u32) -> Option<&wasmtime::Module> {
        let cmd: usize = cmd.try_into().ok()?;
        self.cmds.get(cmd)
    }
}

/// execute an event of a given data `dat`, ignoring dependencies.
/// returns `Err` if execution failed, and everything already lookup'ed

impl WasmEngine {
    pub fn new() -> anyhow::Result<Self> {
        let wtc = wasmtime::Config::default();
        Ok(Self {
            wte: wasmtime::Engine::new(&wtc)?,
            cmds: Vec::new(),
        })
    }

    pub fn add_commands<II, Iter, Item>(&mut self, wasms: II) -> anyhow::Result<(u32, usize)>
    where
        II: IntoIterator<IntoIter = Iter>,
        Iter: Iterator<Item = Item> + Send,
        Item: AsRef<[u8]> + Send,
    {
        let orig_id = self.cmds.len();
        let id: u32 = orig_id.try_into()?;
        self.cmds.extend(
            wasms
                .into_iter()
                .par_bridge()
                .map(|cmd| wasmtime::Module::new(&self.wte, cmd))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok((id, self.cmds.len() - orig_id))
    }
}

#[derive(Clone, Default)]
pub struct WorkCache(pub BTreeMap<BTreeSet<Hash>, Vec<u8>>);

impl WorkCache {
    pub fn new(init_data: Vec<u8>) -> Self {
        let mut sts = BTreeMap::new();
        sts.insert(BTreeSet::new(), init_data);
        Self(sts)
    }

    /// this returns an error if `tt` is not present in `sts`.
    pub fn run_recursively<C, E: EngineError>(
        &mut self,
        graph: &Graph,
        engine: &dyn Engine<Command = C, Error = E>,
        mut tt: BTreeSet<Hash>,
        main_evid: Hash,
        incl: IncludeSpec,
    ) -> anyhow::Result<(&[u8], BTreeSet<Hash>)> {
        // heap of necessary dependencies
        let mut deps = vec![main_evid];

        let mut data = self
            .0
            .get(&tt)
            .with_context(|| anyhow_!("unable to find initial dataset"))?
            .clone();

        while let Some(evid) = deps.pop() {
            if tt.contains(&evid) {
                // nothing to do
                continue;
            } else if evid == main_evid && !deps.is_empty() {
                anyhow::bail!("dependency circuit @ {}", main_evid);
            }

            let evwd = graph
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
                    break;
                }

                // run the item, all dependencies are satisfied
                use std::collections::btree_map::Entry;
                // TODO: check if `data...clone()` is a bottleneck.
                match self.0.entry({
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
                        let cmd = engine.resolve_cmd(evwd.cmd).ok_or_else(|| {
                            anyhow_!("unable to lookup event command for {}", evid)
                        })?;
                        data = engine
                            .run_event_bare(cmd, &evwd.arg[..], &data[..])
                            .map_err(|e| e.into())?;
                        v.insert(data.clone());
                    }
                }
                tt.insert(evid);
            }
        }

        let res = self.0.get(&tt).unwrap();
        Ok((res, tt))
    }

    pub fn run_foreach_recursively<C, E: EngineError>(
        &mut self,
        graph: &Graph,
        engine: &dyn Engine<Command = C, Error = E>,
        evids: BTreeMap<Hash, IncludeSpec>,
    ) -> anyhow::Result<(&[u8], BTreeSet<Hash>)> {
        let tt = evids
            .into_iter()
            .try_fold(BTreeSet::new(), |tt, (i, idspec)| {
                self.run_recursively(graph, engine, tt, i, idspec)
                    .map(|(_, new_tt)| new_tt)
            })?;
        let res = self.0.get(&tt).unwrap();
        Ok((res, tt))
    }

    /// NOTE: this ignores the contents of `ev.deps`
    pub fn shelve_event<C, E: EngineError>(
        &mut self,
        graph: &mut Graph,
        engine: &dyn Engine<Command = C, Error = E>,
        mut seed_deps: BTreeSet<Hash>,
        ev: Event,
    ) -> anyhow::Result<Option<Hash>> {
        let cur_cmd = engine
            .resolve_cmd(ev.cmd)
            .ok_or_else(|| anyhow_!("unable to lookup event command for {:?}", ev))?;

        // check `ev` for independence
        #[derive(Clone, Copy, PartialEq)]
        enum DepSt {
            Use,
            Deny,
        }
        let mut cur_deps = BTreeMap::new();

        while !seed_deps.is_empty() {
            let mut new_seed_deps = BTreeSet::new();
            // calculate cur state
            let (base_st, _) = self.run_foreach_recursively(
                graph,
                engine,
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
                .run_event_bare(cur_cmd, &ev.arg[..], base_st)
                .map_err(|e| e.into())?;
            if cur_deps.is_empty() && base_st == &cur_st[..] {
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
                    engine,
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
                let conc_cmd = engine
                    .resolve_cmd(conc_ev.cmd)
                    .ok_or_else(|| anyhow_!("unable to lookup event command for {}", conc_evid))?;
                let is_indep = if cur_st == base_st {
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
                        .run_event_bare(cur_cmd, &ev.arg[..], base_st)
                        .and_then(|next_st| {
                            engine.run_event_bare(conc_cmd, &conc_ev.arg[..], &next_st[..])
                        })
                        .map_err(|e| e.into())?
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
            anyhow::bail!(
                "hash collision @ {} detected while trying to insert {:?}",
                evhash,
                ev
            );
        }

        Ok(Some(evhash))
    }

    pub fn check_if_mergable<C, E: EngineError>(
        &mut self,
        graph: &Graph,
        engine: &dyn Engine<Command = C, Error = E>,
        sts: BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Self>> {
        // we run this recursively (and non-parallel), which is a bit unfortunate,
        // but we get the benefit that we can share the cache...
        let bases = sts
            .iter()
            .map(|&h| {
                self.run_recursively(graph, engine, BTreeSet::new(), h, IncludeSpec::IncludeAll)
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
                    this.run_recursively(graph, engine, i, j, IncludeSpec::IncludeAll)?;
                    let elem = this.0;
                    Ok(if acc.1.map(|prev| prev == elem).unwrap_or(true) {
                        (true, Some(elem))
                    } else {
                        (false, None)
                    })
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|(uacc, x)| x.map(|y| (uacc, y)))
            .fold((true, None), {
                |acc, (uacc, elem)| {
                    let is_mrgb = uacc && acc.0 && acc.1.map(|prev| prev == elem).unwrap_or(true);
                    (is_mrgb, if is_mrgb { Some(elem) } else { None })
                }
            });
        Ok(ret.1.map(Self))
    }
}
