use crate::utils::*;
use esvc_core::{Context as EsvcCtx, Event as CEvent};
use pyo3::{
    class::gc,
    create_exception,
    exceptions::PyException,
    prelude::{pyclass, pymethods, pymodule, pyproto, PyModule, PyResult, Python},
    types::{PyBytes, PyList},
    Py, PyAny, PyErr,
};
use std::collections::{BTreeMap, BTreeSet};

create_exception!(esvc_indra, EsvcError, PyException);
create_exception!(esvc_indra, DatabaseError, EsvcError);
create_exception!(esvc_indra, ApplyError, EsvcError);

fn db_err(x: indradb::Error) -> PyErr {
    DatabaseError::new_err(x.to_string())
}
fn apply_err(x: esvc_core::ApplyError) -> PyErr {
    ApplyError::new_err(x.to_string())
}

#[derive(Clone, Copy)]
struct Context<'p>(Python<'p>, &'p PyList);

impl<'p> EsvcCtx for Context<'p> {
    type State = &'p PyAny;
    type Error = PyErr;

    fn execute(self, data: &'p PyAny, ev: &CEvent) -> PyResult<&'p PyAny> {
        let Context(py, cmdreg) = self;
        let cmd = cmdreg.get_item(ev.name.try_into()?)?;
        cmd.call1((data, PyBytes::new(py, &ev.arg[..])))
    }
}

#[pyclass]
#[derive(Clone)]
struct ApplyTracker(esvc_core::ApplyTracker);

#[pymethods]
impl ApplyTracker {
    #[new]
    fn new() -> Self {
        Self(Default::default())
    }
}

#[pyclass]
#[derive(Clone)]
struct Event(CEvent);

#[pymethods]
impl Event {
    #[new]
    fn new(name: u128, arg: &PyBytes) -> Self {
        Self(CEvent {
            name,
            arg: arg.as_bytes().to_vec(),
        })
    }
}

#[pyclass(gc)]
#[derive(Clone)]
struct EsvcIndra {
    // uses Arc internally
    idb: indradb::MemoryDatastore,

    #[pyo3(get, set)]
    cmdreg: Py<PyList>,
}

#[pyproto]
impl gc::PyGCProtocol<'p> for EsvcIndra {
    fn __traverse__(&'p self, visit: gc::PyVisit<'_>) -> Result<(), gc::PyTraverseError> {
        visit.call(&self.cmdreg)?;
        Ok(())
    }

    fn __clear__(&mut self) {
        Python::with_gil(|py| {
            self.cmdreg = PyList::empty(py).into();
        })
    }
}

impl EsvcIndra {
    fn run_recursively_intern<'p>(
        &'p self,
        py: Python<'p>,
        cache_st: &mut BTreeMap<BTreeSet<u128>, &'p PyAny>,
        cache_dp: &mut BTreeMap<u128, BTreeSet<u128>>,
        data: &mut &'p PyAny,
        trackertop: &mut BTreeSet<u128>,
        main_id: u128,
        include_top: bool,
    ) -> PyResult<()> {
        // recursively apply all needed dependencies.
        let ctx = Context(py, self.cmdreg.as_ref(py));

        // heap of necessary dependencies
        let mut deps = vec![main_id];

        let can_write_cache_dp = trackertop.is_empty();
        if let Some(x) = cache_dp.get(&main_id) {
            deps.extend(x.iter().copied());
        }

        while let Some(id) = deps.pop() {
            // equivalent logic as `ApplyTracker::can_run`, but more effective
            if trackertop.contains(&id) {
                // nothing to do
                continue;
            } else if id == main_id {
                if !deps.is_empty() {
                    return Err(EsvcError::new_err(format!(
                        "dependency circuit @ {}",
                        id_to_base32(main_id)
                    )));
                }
                if !include_top {
                    // we want to omit the final dep
                    break;
                }
            }

            let evwd = get_event(&self.idb, id).map_err(db_err)?;
            let mut necessary_deps = evwd.deps.difference(trackertop);

            if let Some(&x) = necessary_deps.next() {
                deps.push(id);
                // TODO: maybe check for possible circles, resulting in a forever loop?
                deps.push(x);
                deps.extend(necessary_deps.copied());
            } else {
                // run the item, all dependencies are satisfied
                use std::collections::btree_map::Entry;
                trackertop.insert(id);
                match cache_st.entry(trackertop.clone()) {
                    Entry::Occupied(o) => {
                        // reuse cached entry
                        *data = *o.get();
                    }
                    Entry::Vacant(v) => {
                        trackertop.remove(&id);
                        *data = ctx.execute(*data, &evwd.ev)?;
                        // create cache entry
                        v.insert(*data);
                        trackertop.insert(id);
                    }
                }
            }
        }

        if can_write_cache_dp && !cache_dp.contains_key(&main_id) {
            cache_dp.insert(main_id, {
                let mut x = trackertop.clone();
                x.remove(&main_id);
                x
            });
        }
        Ok(())
    }
}

#[pymethods]
impl EsvcIndra {
    #[new]
    fn new(persistence_path: String, cmdreg: Py<PyList>) -> PyResult<Self> {
        Ok(Self {
            idb: {
                use indradb::MemoryDatastore as Mds;
                if persistence_path.is_empty() {
                    Mds::default()
                } else if std::path::Path::new(&*persistence_path).exists() {
                    Mds::read(&*persistence_path).map_err(|e| EsvcError::new_err(e.to_string()))?
                } else {
                    Mds::create(&*persistence_path)
                        .map_err(|e| EsvcError::new_err(e.to_string()))?
                }
            },
            cmdreg,
        })
    }

    fn sync(&self) -> PyResult<()> {
        use indradb::Datastore;
        self.idb.sync().map_err(db_err)
    }

    fn reg_event(&self, name: u128, arg: &PyBytes, deps: Vec<u128>) -> PyResult<u128> {
        let deps: BTreeSet<u128> = deps.into_iter().collect();

        ensure_node(
            &self.idb,
            &esvc_core::EventWithDeps {
                ev: CEvent {
                    name,
                    arg: arg.as_bytes().to_vec(),
                },
                deps,
            },
        )
        .map_err(db_err)
    }

    // horribly inefficient, but dunno how to fix it.
    fn shelve_events<'p>(
        &'p self,
        py: Python<'p>,
        init_data: &'p PyAny,
        init_deps: Vec<u128>,
        evs: Vec<Event>,
    ) -> PyResult<&'p PyList> {
        use std::mem::drop;

        let ctx = Context(py, self.cmdreg.as_ref(py));
        let mut cache_st: BTreeMap<BTreeSet<u128>, &'p PyAny> = Default::default();
        let mut cache_dp: BTreeMap<u128, BTreeSet<u128>> = Default::default();
        let mut next_deps: BTreeSet<_> = init_deps.into_iter().collect();
        let ret = PyList::empty(py);

        for ev in evs {
            // apply it
            let newst = ctx.execute(init_data, &ev.0)?;
            // skip all noop events
            if init_data == newst {
                ret.append(py.None())?;
                continue;
            }

            // check `ev` for independence
            let mut use_deps = BTreeSet::new();
            let mut deny_deps = BTreeSet::new();
            let mut my_next_deps = next_deps.clone();
            while !my_next_deps.is_empty() {
                for conc_evid in std::mem::take(&mut my_next_deps) {
                    if deny_deps.contains(&conc_evid) {
                        continue;
                    }
                    let mut a_st = init_data;
                    self.run_recursively_intern(
                        py,
                        &mut cache_st,
                        &mut cache_dp,
                        &mut a_st,
                        &mut BTreeSet::new(),
                        conc_evid,
                        true,
                    )?;
                    let a = ctx.execute(a_st, &ev.0)?;
                    let conc_evwd = get_event(&self.idb, conc_evid).map_err(db_err)?;
                    let b = ctx.execute(newst, &conc_evwd.ev)?;
                    if a == b {
                        // independent -> move backward
                        my_next_deps.extend(conc_evwd.deps);
                    } else {
                        // not independent -> move forward
                        deny_deps.extend(conc_evwd.deps);
                        use_deps.insert(conc_evid);
                    }
                }
            }
            use_deps.retain(|i| !deny_deps.contains(i));
            drop(deny_deps);

            // register event, mangle deps
            let evwd = esvc_core::EventWithDeps {
                ev: ev.0,
                deps: use_deps,
            };
            let evid = ensure_node(&self.idb, &evwd).map_err(db_err)?;

            // replace the dependecies of this event with this event itself
            next_deps.retain(|i| !evwd.deps.contains(i));
            next_deps.insert(evid);
            ret.append(evid)?;
        }
        Ok(ret)
    }

    fn run_events<'p>(
        &'p self,
        py: Python<'p>,
        ids: Vec<u128>,
        mut data: &'p PyAny,
        tracker: Option<Py<ApplyTracker>>,
    ) -> PyResult<&'p PyAny> {
        let ctx = Context(py, self.cmdreg.as_ref(py));

        if let Some(tracker) = tracker {
            let mut tracker = tracker.borrow_mut(py);
            for id in ids {
                let evwd = get_event(&self.idb, id).map_err(db_err)?;
                tracker.0.can_run(id, &evwd.deps).map_err(apply_err)?;
                data = ctx.execute(data, &evwd.ev)?;
                tracker.0.register_as_ran(id);
            }
            // TODO: handle tags
        } else {
            for id in ids {
                let evwd = get_event(&self.idb, id).map_err(db_err)?;
                data = ctx.execute(data, &evwd.ev)?;
            }
        }
        Ok(data)
    }
}

#[pymodule]
pub fn esvc_indra(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<ApplyTracker>()?;
    m.add_class::<EsvcIndra>()?;
    m.add_class::<Event>()?;
    m.add_function(pyo3::wrap_pyfunction!(id_to_base32, m)?)?;
    m.add("EsvcError", py.get_type::<EsvcError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("ApplyError", py.get_type::<ApplyError>())?;
    Ok(())
}
