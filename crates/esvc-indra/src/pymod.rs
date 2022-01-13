use esvc_core::{Context as EsvcCtx, Event as CEvent};
use pyo3::{
    class::gc,
    create_exception,
    exceptions::PyException,
    prelude::{pyclass, pymethods, pymodule, pyproto, PyModule, PyResult, Python},
    types::{PyBytes, PyInt, PyList, PyString},
    Py, PyAny, PyErr,
};
use std::collections::BTreeSet;

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

        crate::utils::ensure_node(
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

    fn run_events<'p>(
        &'p self,
        py: Python<'p>,
        ids: Vec<u128>,
        mut data: &'p PyAny,
        tracker: Option<Py<ApplyTracker>>,
    ) -> PyResult<&'p PyAny> {
        let ctx = Context(py, self.cmdreg.as_ref(py));

        if let Some(mut tracker) = tracker {
            let mut tracker = tracker.borrow_mut(py);
            for id in ids {
                let evwd = crate::utils::get_event(&self.idb, id).map_err(db_err)?;
                tracker.0.can_run(id, &evwd.deps).map_err(apply_err)?;
                data = ctx.execute(data, &evwd.ev)?;
                tracker.0.register_as_ran(id);
            }
            // TODO: handle tags
        } else {
            for id in ids {
                let evwd = crate::utils::get_event(&self.idb, id).map_err(db_err)?;
                data = ctx.execute(data, &evwd.ev)?;
            }
        }
        Ok(data)
    }
}

#[pymodule]
pub fn esvc_indra(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    use crate::utils::*;
    m.add_class::<ApplyTracker>()?;
    m.add_class::<EsvcIndra>()?;
    m.add_function(pyo3::wrap_pyfunction!(id_to_base32, m)?);
    m.add("EsvcError", py.get_type::<EsvcError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("ApplyError", py.get_type::<ApplyError>())?;
    Ok(())
}
