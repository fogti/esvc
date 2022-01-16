use crate::Hash;
use anyhow::{anyhow as anyhow_, Context};
use rayon::prelude::*;
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
}

#[derive(Clone)]
pub struct Engine {
    wte: wasmtime::Engine,
    g: Graph,
    cmds: Vec<wasmtime::Module>,
}

/// execute an event of a given data `dat`, ignoring dependencies.
/// returns `Err` if execution failed, and everything already lookup'ed
fn run_event_bare(
    wte: &wasmtime::Engine,
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

    let mut store = wasmtime::Store::new(wte, ());
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
        let retptr2: usize = i32::from_le_bytes(<[u8; 4]>::try_from(retp0).unwrap()).try_into()?;
        let retlen2: usize = i32::from_le_bytes(<[u8; 4]>::try_from(retp1).unwrap()).try_into()?;
        memory
            .data(&mut store)
            .get(retptr2..retptr2 + retlen2)
            .with_context(|| "return value length out of bounds".to_string())?
            .to_vec()
    };

    Ok(ret)
}

impl Engine {
    pub fn new() -> anyhow::Result<Self> {
        let wtc = wasmtime::Config::default();
        Ok(Self {
            wte: wasmtime::Engine::new(&wtc)?,
            g: Default::default(),
            cmds: Vec::new(),
        })
    }

    #[inline]
    pub fn graph(&self) -> &Graph {
        &self.g
    }

    pub fn with_graph(&self, g: Graph) -> anyhow::Result<Self> {
        let cmds = g
            .cmds
            .par_iter()
            .map(|cmd| wasmtime::Module::new(&self.wte, cmd))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            wte: self.wte.clone(),
            g,
            cmds,
        })
    }

    pub fn add_command(&mut self, wasm: Vec<u8>) -> anyhow::Result<u32> {
        assert_eq!(self.g.cmds.len(), self.cmds.len());
        let id: u32 = self.g.cmds.len().try_into()?;
        let comp = wasmtime::Module::new(&self.wte, &wasm[..])?;
        self.g.cmds.push(wasm);
        self.cmds.push(comp);
        Ok(id)
    }

    fn get_cmd_module(&self, cmd: u32) -> Option<&wasmtime::Module> {
        let cmd: usize = cmd.try_into().ok()?;
        self.cmds.get(cmd)
    }

    /// execute an event of a given data `dat`, ignoring all dependencies of it.
    /// returns `Err` if execution failed, and `Ok(None)` if lookup of the event failed
    pub fn run_event_igndeps(&self, dat: &[u8], evid: &Hash) -> anyhow::Result<Option<Vec<u8>>> {
        let ev = match self.g.events.get(evid) {
            Some(x) => x,
            None => return Ok(None),
        };
        let cmd = match self.get_cmd_module(ev.cmd) {
            Some(x) => x,
            None => return Ok(None),
        };
        run_event_bare(&self.wte, cmd, &ev.arg[..], dat).map(Some)
    }
}

#[derive(Clone, Default)]
pub struct WorkCache(pub BTreeMap<BTreeSet<Hash>, Vec<u8>>);

pub fn print_deps<W: std::io::Write>(
    w: &mut W,
    pfx: &str,
    deps: &BTreeSet<Hash>,
) -> std::io::Result<()> {
    for i in deps {
        writeln!(w, "{}{}", pfx, i)?;
    }
    Ok(())
}

impl WorkCache {
    pub fn new(init_data: Vec<u8>) -> Self {
        let mut sts = BTreeMap::new();
        sts.insert(BTreeSet::new(), init_data);
        Self(sts)
    }

    /// this returns an error if `tt` is not present in `sts`.
    pub fn run_recursively(
        &mut self,
        parent: &Engine,
        mut tt: BTreeSet<Hash>,
        main_evid: Hash,
        include_top: bool,
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

            let evwd = parent
                .g
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
                if evid == main_evid && !include_top {
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
                        let cmd = parent.get_cmd_module(evwd.cmd).ok_or_else(|| {
                            anyhow_!("unable to lookup event command for {}", evid)
                        })?;
                        data = run_event_bare(&parent.wte, cmd, &evwd.arg[..], &data[..])?;
                        v.insert(data.clone());
                    }
                }
                tt.insert(evid);
            }
        }

        let res = self.0.get(&tt).unwrap();
        Ok((res, tt))
    }

    /// NOTE: this ignores the contents of `evs.[].deps`
    pub fn shelve_events(
        &mut self,
        parent: &mut Engine,
        seed_deps: BTreeSet<Hash>,
        evs: Vec<Event>,
    ) -> anyhow::Result<Vec<Hash>> {
        if !self.0.contains_key(&Default::default()) {
            anyhow::bail!("unable to find initial dataset");
        }
        let mut ret = Vec::new();
        let mut next_deps = seed_deps;

        for ev in evs {
            let cur_cmd = parent
                .get_cmd_module(ev.cmd)
                .ok_or_else(|| anyhow_!("unable to lookup event command for {:?}", ev))?
                .clone();

            // check `ev` for independence
            #[derive(PartialEq)]
            enum DepSt {
                Use,
                Deny,
            }
            let mut cur_deps = BTreeMap::new();
            let mut my_next_deps = next_deps.clone();

            while !my_next_deps.is_empty() {
                for conc_evid in core::mem::take(&mut my_next_deps) {
                    if cur_deps.get(&conc_evid) == Some(&DepSt::Deny) {
                        continue;
                    }
                    // calculate base state of conc excluding conc event itself
                    let base_deps = self
                        .run_recursively(parent, BTreeSet::new(), conc_evid, false)?
                        .1;
                    let base_st = self.0.get(&base_deps).unwrap();
                    let conc_ev = parent.g.events.get(&conc_evid).unwrap();
                    let conc_cmd = parent.get_cmd_module(ev.cmd).ok_or_else(|| {
                        anyhow_!("unable to lookup event command for {}", conc_evid)
                    })?;
                    let wte = &parent.wte;
                    let (a, b) = rayon::join(
                        || {
                            run_event_bare(wte, conc_cmd, &conc_ev.arg[..], base_st).and_then(
                                |next_st| run_event_bare(wte, &cur_cmd, &ev.arg[..], &next_st[..]),
                            )
                        },
                        || {
                            run_event_bare(wte, &cur_cmd, &ev.arg[..], base_st).and_then(
                                |next_st| {
                                    run_event_bare(wte, conc_cmd, &conc_ev.arg[..], &next_st[..])
                                },
                            )
                        },
                    );
                    let (a, b) = (a?, b?);
                    if a == b {
                        // independent -> move backward
                        my_next_deps.extend(conc_ev.deps.iter().copied());
                    } else {
                        // not independent -> move forward
                        cur_deps.extend(conc_ev.deps.iter().map(|&dep| (dep, DepSt::Deny)));
                        cur_deps.insert(conc_evid, DepSt::Use);
                    }
                }
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

            // replace the dependencies of this event with this event itself
            // (move forward)
            next_deps.retain(|i| !ev.deps.contains(i));

            // register event
            let (collinfo, evhash) = parent.g.ensure_event(ev);
            if let Some(ev) = collinfo {
                anyhow::bail!(
                    "hash collision @ {} detected while trying to insert {:?}",
                    evhash,
                    ev
                );
            }

            next_deps.insert(evhash);
            ret.push(evhash);
        }

        Ok(ret)
    }

    pub fn check_if_mergable(
        &mut self,
        parent: &Engine,
        sts: BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Self>> {
        // we run this recursively, which is a bit unfortunate,
        // but we get the benefit that we can share the cache...
        let bases = sts
            .iter()
            .map(|&h| {
                self.run_recursively(parent, BTreeSet::new(), h, true)
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
                    this.run_recursively(parent, i, j, true)?;
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
