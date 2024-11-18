//! Rust wrapper for the `iisa.network` Python module.

use pyo3::{
    exceptions::PyTypeError,
    intern,
    sync::GILOnceCell,
    types::{PyAny, PyAnyMethods, PyList, PyListMethods, PyType},
    Bound, FromPyObject, Py, PyResult, Python,
};

use super::{geoip::PyGeoipResolver, import_iisa_module};
use crate::api::Indexer;

/// Import the `iisa.network.NetworkProvider` class.
fn import_network_provider_class(py: Python) -> PyResult<&Bound<PyType>> {
    static NETWORK_PROVIDER_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
    NETWORK_PROVIDER_CLASS
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let type_object = import_iisa_module(py)?
                .getattr("NetworkProvider")?
                .downcast_into()?;
            Ok(type_object.unbind())
        })
        .map(|ty| ty.bind(py))
}

/// Import the `iisa.network.Indexer` class.
fn import_indexer_class(py: Python) -> PyResult<&Bound<PyType>> {
    static INDEXER_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
    INDEXER_CLASS
        .get_or_try_init(py, || {
            let type_object = py
                .import("iisa.network")?
                .getattr("Indexer")?
                .downcast_into()?;
            Ok(type_object.unbind())
        })
        .map(|ty| ty.bind(py))
}

/// Convert a network snapshot indexers iterator to a Python list of `iisa.network.Indexer`
/// instances.
fn network_snapshot_to_python_list<'py, 'snapshot>(
    py: Python<'py>,
    snapshot: impl IntoIterator<Item = &'snapshot Indexer>,
) -> PyResult<Bound<'py, PyList>> {
    let indexer_class = import_indexer_class(py)?;

    // Create a list of `iisa.network.Indexer` instances from the latest network snapshot
    let indexers = PyList::empty(py);
    for indexer in snapshot {
        // Use LowerHex to format the ID as a hexadecimal string
        // See: https://docs.rs/thegraph-core/0.6.0/thegraph_core/struct.IndexerId.html#formatting
        let indexer_id = format!("{:#x}", indexer.id);
        let indexer_url = indexer.url.to_string();

        let indexer = indexer_class.call1((indexer_id, indexer_url))?;
        indexers.append(indexer)?;
    }

    Ok(indexers)
}

/// Create a new `iisa.network.NetworkProvider` instance.
fn new_network_provider<'py>(
    py: Python<'py>,
    geoip_resolver: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let network_provider_class = import_network_provider_class(py)?;
    network_provider_class.call1((geoip_resolver,))
}

/// Python network provider.
///
/// This struct is used to interact with the Python network provider and update the
/// network snapshot in the Python network provider.
#[derive(Clone)]
pub struct PyNetworkProvider<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyNetworkProvider<'py> {
    /// Create a new `PyNetworkProvider` instance.
    pub fn new(py: Python<'py>, geoip_resolver: PyGeoipResolver<'py>) -> PyResult<Self> {
        let inner = new_network_provider(py, geoip_resolver.into_any())?;
        Ok(Self { inner })
    }

    /// Cast to `Bound<'py, PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Set the network snapshot in the network provider
    pub fn set_snapshot<'snapshot>(
        &self,
        py: Python<'py>,
        indexers: impl IntoIterator<Item = &'snapshot Indexer>,
    ) -> PyResult<()> {
        let indexers = network_snapshot_to_python_list(py, indexers)?;
        self.inner
            .call_method1(intern!(py, "set_snapshot"), (indexers,))?;
        Ok(())
    }

    /// Get indexers from the network provider.
    #[cfg(test)]
    pub fn indexers(&self) -> PyResult<Bound<'py, PyAny>> {
        self.inner.call_method0("indexers")
    }
}

impl<'py> FromPyObject<'py> for PyNetworkProvider<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let class = import_network_provider_class(ob.py())?;
        if !ob.is_exact_instance(class) {
            return Err(PyTypeError::new_err("Invalid instance type"));
        }

        Ok(Self { inner: ob.clone() })
    }
}

impl std::fmt::Debug for PyNetworkProvider<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::{
        types::{PyAnyMethods, PyDict},
        PyResult, Python,
    };

    use super::{new_network_provider, PyGeoipResolver, PyNetworkProvider};

    #[test]
    fn extract_from_any() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let geoip_resolver_any = PyGeoipResolver::new(py, "test_auth")
                .expect("Failed to create GeoipResolver instance")
                .into_any();

            let network_provider_any = new_network_provider(py, geoip_resolver_any)
                .expect("Failed to create NetworkProvider instance");

            //* When
            let result: PyResult<PyNetworkProvider> = network_provider_any.extract();

            //* Then
            assert!(result.is_ok());
        });
    }

    #[test]
    fn extract_fails_from_invalid_instance() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let invalid_instance = PyDict::new(py);

            //* When
            let result: PyResult<PyNetworkProvider> = invalid_instance.extract();

            //* Then
            assert!(result.is_err());

            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "TypeError: Invalid instance type");
        });
    }
}
