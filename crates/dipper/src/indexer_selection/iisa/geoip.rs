//! Rust wrapper for the `iisa.geoip` Python module.

use pyo3::{
    exceptions::PyTypeError,
    sync::GILOnceCell,
    types::{PyAnyMethods, PyType},
    Bound, FromPyObject, Py, PyAny, PyResult, Python,
};

use super::import_iisa_module;

/// Import the `iisa.geoip.GeoipResolver` class.
fn import_geoip_resolver_class(py: Python) -> PyResult<&Bound<PyType>> {
    static GEOIP_RESOLVER_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
    GEOIP_RESOLVER_CLASS
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let class = import_iisa_module(py)?
                .getattr("GeoipResolver")?
                .downcast_into()?;
            Ok(class.unbind())
        })
        .map(|ty| ty.bind(py))
}

/// Create a new `iisa.geoip.GeoipResolver` instance.
fn new_geoip_resolver(py: Python) -> PyResult<Bound<PyAny>> {
    let class = import_geoip_resolver_class(py)?;
    class.call0()
}

/// Python GeoIP resolver provider wrapper.
#[derive(Debug, Clone)]
pub struct PyGeoipResolver<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyGeoipResolver<'py> {
    /// Create a new `PyGeoipResolver` instance.
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        let inner = new_geoip_resolver(py)?;
        Ok(Self { inner })
    }

    /// Cast to `Bound<'py, PyAny>`.
    pub fn as_any(&self) -> &Bound<'py, PyAny> {
        &self.inner
    }

    /// Cast to `Bound<'py, PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Resolve the given IP address to a country code.
    #[cfg(test)]
    pub fn resolve_url_host_info(&self, url: &str) -> PyResult<Bound<'py, PyAny>> {
        self.inner.call_method1("resolve_url_host_info", (url,))
    }
}

impl<'py> FromPyObject<'py> for PyGeoipResolver<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let class = import_geoip_resolver_class(ob.py())?;
        if !ob.is_exact_instance(class) {
            return Err(PyTypeError::new_err("Invalid instance type"));
        }

        Ok(Self { inner: ob.clone() })
    }
}

#[cfg(test)]
mod tests {
    use pyo3::{prelude::*, types::PyDict};

    use super::{new_geoip_resolver, PyGeoipResolver};

    #[test]
    fn extract_from_any() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let geoip_resolver_any =
                new_geoip_resolver(py).expect("Failed to create GeoipResolver instance");

            //* When
            let result: PyResult<PyGeoipResolver> = geoip_resolver_any.extract();

            //* Then
            assert!(result.is_ok());
        });
    }

    #[test]
    fn extract_fails_from_invalid_instance() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let invalid_instance = PyDict::new_bound(py);

            //* When
            let result: PyResult<PyGeoipResolver> = invalid_instance.extract();

            //* Then
            assert!(result.is_err());

            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "TypeError: Invalid instance type");
        });
    }
}
