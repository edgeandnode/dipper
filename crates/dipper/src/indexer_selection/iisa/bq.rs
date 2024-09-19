//! Rust wrapper for the `iisa.bq` Python module.

#[cfg(test)]
use pyo3::types::PyDate;
use pyo3::{
    exceptions::PyTypeError,
    sync::GILOnceCell,
    types::{PyAnyMethods, PyType},
    Bound, FromPyObject, Py, PyAny, PyResult, Python,
};

use super::import_iisa_module;

/// Import the `iisa.bq.BigQueryProvider` class.
fn import_bigquery_provider_class(py: Python) -> PyResult<&Bound<PyType>> {
    static BIGQUERY_PROVIDER_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
    BIGQUERY_PROVIDER_CLASS
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let class = import_iisa_module(py)?
                .getattr("BigQueryProvider")?
                .downcast_into()?;
            Ok(class.unbind())
        })
        .map(|ty| ty.bind(py))
}

/// Create a new `iisa.bq.BigQueryProvider` instance.
fn new_bigquery_provider<'py>(
    py: Python<'py>,
    project: &str,
    location: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let bigquery_provider_class = import_bigquery_provider_class(py)?;
    bigquery_provider_class.call1((project, location))
}

/// Python BigQuery provider wrapper.
#[derive(Clone)]
pub struct PyBigQueryProvider<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyBigQueryProvider<'py> {
    /// Create a new `PyBigQueryProvider` instance.
    pub fn new(py: Python<'py>, project: &str, location: &str) -> PyResult<Self> {
        let inner = new_bigquery_provider(py, project, location)?;
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

    /// Fetch initial query results
    #[cfg(test)]
    pub fn fetch_initial_query_results(
        &self,
        start_date: Bound<'py, PyDate>,
        num_days: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.inner
            .call_method1("fetch_initial_query_results", (start_date, num_days))
    }
}

impl<'py> FromPyObject<'py> for PyBigQueryProvider<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let class = import_bigquery_provider_class(ob.py())?;
        if !ob.is_exact_instance(class) {
            return Err(PyTypeError::new_err("Invalid instance type"));
        }

        Ok(Self { inner: ob.clone() })
    }
}

impl std::fmt::Debug for PyBigQueryProvider<'_> {
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

    use super::{new_bigquery_provider, PyBigQueryProvider};

    #[test]
    fn extract_from_any() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let geoip_resolver_any = new_bigquery_provider(py, "project", "us-west3")
                .expect("Failed to create BigQueryProvider instance");

            //* When
            let result: PyResult<PyBigQueryProvider> = geoip_resolver_any.extract();

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
            let result: PyResult<PyBigQueryProvider> = invalid_instance.extract();

            //* Then
            assert!(result.is_err());

            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "TypeError: Invalid instance type");
        });
    }
}
