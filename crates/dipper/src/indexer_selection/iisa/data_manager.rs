//! Rust wrapper for the `iisa.iisa.DataManager` Python class.

use pyo3::{
    exceptions::PyTypeError,
    sync::GILOnceCell,
    types::{PyAny, PyAnyMethods, PyType},
    Bound, FromPyObject, Py, PyResult, Python,
};

use super::{import_iisa_module, PyBigQueryProvider, PyNetworkProvider};

/// Import the `iisa.data_manager.DataManager` class.
fn import_data_manager_class(py: Python) -> PyResult<&Bound<PyType>> {
    static DATA_MANAGER_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
    DATA_MANAGER_CLASS
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let type_object = import_iisa_module(py)?
                .getattr("DataManager")?
                .downcast_into()?;
            Ok(type_object.unbind())
        })
        .map(|ty| ty.bind(py))
}

/// Create a new `iisa.iisa.DataManager` instance.
fn new_data_manager<'py>(
    py: Python<'py>,
    bigquery_provider: Bound<'py, PyAny>,
    network_provider: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let data_manager_class = import_data_manager_class(py)?;
    data_manager_class.call1((bigquery_provider, network_provider))
}

/// Python data manager.
#[derive(Clone)]
pub struct PyDataManager<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyDataManager<'py> {
    /// Create a new `iisa.iisa.DataManager` instance.
    pub fn new(
        py: Python<'py>,
        bigquery_provider: PyBigQueryProvider<'py>,
        network_provider: PyNetworkProvider<'py>,
    ) -> PyResult<Self> {
        let inner = new_data_manager(
            py,
            bigquery_provider.into_any(),
            network_provider.into_any(),
        )?;
        Ok(Self { inner })
    }

    /// Cast to `Bound<'py PyAny>`.
    pub fn as_any(&self) -> &Bound<'py, PyAny> {
        &self.inner
    }

    /// Cast to `Bound<'py PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Fetch the latest data from BigQuery and update the data and indexer rankings information.
    pub fn fetch_data_and_update(&self) -> PyResult<()> {
        let _ = self.inner.call_method0("fetch_data_and_update")?;
        Ok(())
    }

    /// Return the cached BigQuery data.
    pub fn get_data(&self) -> PyResult<PyRequestHistoryDataFrame<'py>> {
        let dataframe = self.inner.call_method0("get_data")?;
        Ok(PyRequestHistoryDataFrame { inner: dataframe })
    }

    /// Return the indexer rankings from the latency linear regression.
    pub fn get_latency_linear_regression_indexer_rankings(
        &self,
    ) -> PyResult<PyIndexerRankingsDataFrame<'py>> {
        let dataframe = self
            .inner
            .call_method0("get_latency_linear_regression_indexer_rankings")?;
        Ok(PyIndexerRankingsDataFrame { inner: dataframe })
    }

    /// Return the results df from the latency linear regression.
    pub fn get_latency_linear_regression_results(
        &self,
    ) -> PyResult<PyRegressionResultsDataFrame<'py>> {
        let dataframe = self
            .inner
            .call_method0("get_latency_linear_regression_results")?;
        Ok(PyRegressionResultsDataFrame { inner: dataframe })
    }
}

impl<'py> FromPyObject<'py> for PyDataManager<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let class = import_data_manager_class(ob.py())?;
        if !ob.is_exact_instance(class) {
            return Err(PyTypeError::new_err("Invalid instance type"));
        }

        Ok(Self { inner: ob.clone() })
    }
}

impl std::fmt::Debug for PyDataManager<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

/// Python requests history data frame.
///
/// This is a wrapper around a `iisa.iisa.RequestsHistoryDataFrame` instance.
#[derive(Clone)]
pub struct PyRequestHistoryDataFrame<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyRequestHistoryDataFrame<'py> {
    /// Cast to `Bound<'py PyAny>`.
    pub fn as_any(&self) -> &Bound<'py, PyAny> {
        &self.inner
    }

    /// Cast to `Bound<'py PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Whether the data frame is empty.
    pub fn is_empty(&self) -> PyResult<bool> {
        self.inner.getattr("empty")?.extract()
    }

    /// Get the data frame shape.
    pub fn shape(&self) -> PyResult<(usize, usize)> {
        self.inner.getattr("shape")?.extract()
    }
}

impl std::fmt::Debug for PyRequestHistoryDataFrame<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

/// Python indexer rankings data frame.
///
/// This is a wrapper around a `iisa.iisa.IndexerRankingsDataFrame` instance.
#[derive(Clone)]
pub struct PyIndexerRankingsDataFrame<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyIndexerRankingsDataFrame<'py> {
    /// Cast to `Bound<'py PyAny>`.
    pub fn as_any(&self) -> &Bound<'py, PyAny> {
        &self.inner
    }

    /// Cast to `Bound<'py PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Whether the data frame is empty.
    pub fn is_empty(&self) -> PyResult<bool> {
        self.inner.getattr("empty")?.extract()
    }

    /// Get the data frame shape.
    pub fn shape(&self) -> PyResult<(usize, usize)> {
        self.inner.getattr("shape")?.extract()
    }
}

impl std::fmt::Debug for PyIndexerRankingsDataFrame<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

/// Python linear regression results data frame.
///
/// This is a wrapper around a `iisa.iisa.LinearRegressionResultsDataFrame` instance.
#[derive(Clone)]
pub struct PyRegressionResultsDataFrame<'py> {
    inner: Bound<'py, PyAny>,
}

impl<'py> PyRegressionResultsDataFrame<'py> {
    /// Cast to `Bound<'py PyAny>`.
    pub fn as_any(&self) -> &Bound<'py, PyAny> {
        &self.inner
    }

    /// Cast to `Bound<'py PyAny>`, transferring ownership.
    pub fn into_any(self) -> Bound<'py, PyAny> {
        self.inner
    }

    /// Whether the data frame is empty.
    pub fn is_empty(&self) -> PyResult<bool> {
        self.inner.getattr("empty")?.extract()
    }

    /// Get the data frame shape.
    pub fn shape(&self) -> PyResult<(usize, usize)> {
        self.inner.getattr("shape")?.extract()
    }
}

impl std::fmt::Debug for PyRegressionResultsDataFrame<'_> {
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

    use super::{new_data_manager, PyBigQueryProvider, PyDataManager, PyNetworkProvider};
    use crate::indexer_selection::iisa::PyGeoipResolver;

    #[test]
    fn extract_from_any() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            //* Given
            let geoip_resolver =
                PyGeoipResolver::new(py).expect("Failed to create GeoipResolver instance");
            let network_provider_any = PyNetworkProvider::new(py, geoip_resolver)
                .expect("Failed to create NetworkProvider instance")
                .into_any();
            let bigquery_provider_any = PyBigQueryProvider::new(py, "project", "us")
                .expect("Failed to create BigQueryProvider instance")
                .into_any();

            let data_manager_any =
                new_data_manager(py, bigquery_provider_any, network_provider_any)
                    .expect("Failed to create DataManager instance");

            //* When
            let result: PyResult<PyDataManager> = data_manager_any.extract();

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
            let result: PyResult<PyDataManager> = invalid_instance.extract();

            //* Then
            assert!(result.is_err());

            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "TypeError: Invalid instance type");
        });
    }
}
