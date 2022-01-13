use esvc_core::{state::State as EsvcStateTr, Event as CEvent};
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
create_exception!(esvc_indra, StateError, EsvcError);

// uses Arc internally
type InnerShared = indradb::MemoryDatastore;

#[pyclass(gc)]
#[derive(Clone, PartialEq)]
struct State {
    cmdreg: Py<PyList>,

    #[pyo3(get)]
    data: Py<PyAny>,
}

#[pyproto]
impl gc::PyGCProtocol<'p> for State {
    fn __traverse__(&'p self, visit: gc::PyVisit<'_>) -> Result<(), gc::PyTraverseError> {
        visit.call(&self.cmdreg)?;
        visit.call(&self.data)?;
        Ok(())
    }

    fn __clear__(&mut self) {
        Python::with_gil(|py| {
            self.cmdreg = PyList::empty(py).into();
            self.data = py.None();
        })
    }
}

impl EsvcStateTr for State {
    type Error = PyErr;

    fn run(&mut self, ev: &CEvent) -> PyResult<()> {
        Python::with_gil(|py| {
            let cmd = self.cmdreg.as_ref(py).get_item(ev.name.try_into()?)?;
            self.data = cmd
                .call1((self.data.clone(), PyBytes::new(py, &ev.arg[..])))?
                .into();
            Ok(())
        })
    }
}

#[pyclass(gc)]
struct HiState {
    parent: InnerShared,
    inner: esvc_core::state::HiState<State>,
}

#[pyproto]
impl gc::PyGCProtocol<'p> for HiState {
    fn __traverse__(&'p self, visit: gc::PyVisit<'_>) -> Result<(), gc::PyTraverseError> {
        self.inner.inner.__traverse__(visit)?;
        Ok(())
    }

    fn __clear__(&mut self) {
        self.inner.inner.__clear__();
    }
}

#[pymethods]
impl HiState {
    fn run(&mut self, ev: u128) -> PyResult<()> {
        let evwd = crate::utils::get_event(&self.parent, ev)
            .map_err(|e| DatabaseError::new_err(format!("{:?}", e)))?;
        self.inner
            .run(ev, &evwd.deps, &evwd.ev)
            .map_err(|e| match e {
                esvc_core::state::HiStateError::Inner(i) => i,
                _ => StateError::new_err(format!("{:?}", e)),
            })?;
        // TODO: call `cleanup_top`
        Ok(())
    }

    #[getter]
    fn data<'p>(&'p self, py: Python<'p>) -> &'p PyAny {
        self.inner.inner.data.as_ref(py)
    }
}

#[pyclass(gc)]
struct EsvcIndra {
    inner: InnerShared,
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
            inner: {
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

    fn mkstate(&self, data: Py<PyAny>) -> HiState {
        HiState {
            parent: self.inner.clone(),
            inner: esvc_core::state::HiState {
                top: Default::default(),
                inner: State {
                    cmdreg: self.cmdreg.clone(),
                    data,
                },
            },
        }
    }

    fn event(&self, name: u128, arg: &PyBytes, deps: Vec<u128>) -> PyResult<u128> {
        let deps: BTreeSet<u128> = deps.into_iter().collect();

        crate::utils::ensure_node(
            &self.inner,
            &esvc_core::EventWithDeps {
                ev: CEvent {
                    name,
                    arg: arg.as_bytes().to_vec(),
                },
                deps,
            },
        )
        .map_err(|e| DatabaseError::new_err(format!("{:?}", e)))
    }

    fn sync(&self) -> PyResult<()> {
        use indradb::Datastore;
        self.inner
            .sync()
            .map_err(|e| DatabaseError::new_err(format!("{:?}", e)))
    }
}

#[pymodule]
pub fn esvc_indra(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<State>()?;
    m.add_class::<HiState>()?;
    m.add_class::<EsvcIndra>()?;
    m.add("EsvcError", py.get_type::<EsvcError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("StateError", py.get_type::<StateError>())?;
    Ok(())
}
