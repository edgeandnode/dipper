use pyo3::{
    Bound, Py, PyAny, PyResult, Python, exceptions::PyTypeError, sync::GILOnceCell,
    types::PyAnyMethods,
};
use thegraph_core::IndexerId;

use super::import_iisa_module;

/// Import the `iisa.select_one` function.
fn import_select_one_function(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static SELECT_ONE_FUNCTION: GILOnceCell<Py<PyAny>> = GILOnceCell::new();
    SELECT_ONE_FUNCTION
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let function = import_iisa_module(py)?
                .getattr("select_one")?
                .downcast_into()?;
            Ok(function.unbind())
        })
        .map(|function| function.bind(py))
}

/// Import the select_many function.
fn import_select_many_function(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static SELECT_MANY_FUNCTION: GILOnceCell<Py<PyAny>> = GILOnceCell::new();
    SELECT_MANY_FUNCTION
        .get_or_try_init(py, || {
            // Import from root module to avoid cyclic import issues
            let function = import_iisa_module(py)?
                .getattr("select_many")?
                .downcast_into()?;
            Ok(function.unbind())
        })
        .map(|function| function.bind(py))
}

/// Select one indexer from the given list of candidates.
pub fn select_one<'a>(
    py: Python,
    candidates: impl Iterator<Item = &'a IndexerId>,
) -> PyResult<Option<IndexerId>> {
    let select_one_pyfn = import_select_one_function(py)?;

    let candidates = candidates
        .map(|id| format!("{:#x}", id))
        .collect::<Vec<_>>();

    let result: Option<String> = select_one_pyfn.call1((candidates,))?.extract()?;

    if let Some(id) = result {
        Ok(Some(id.parse().map_err(|err| {
            PyTypeError::new_err(format!("Failed to parse indexer ID from '{id}': {err:#}"))
        })?))
    } else {
        Ok(None)
    }
}

/// Selects the best `num_candidates` indexers from the given list of candidates.
pub fn select_many<'a>(
    py: Python,
    candidates: impl Iterator<Item = &'a IndexerId>,
    num_candidates: usize,
) -> PyResult<Vec<IndexerId>> {
    let select_many_pyfn = import_select_many_function(py)?;

    let candidates = candidates
        .map(|id| format!("{:#x}", id))
        .collect::<Vec<_>>();

    let result: Vec<String> = select_many_pyfn
        .call1((candidates, num_candidates))?
        .extract()?;

    let mut selected: Vec<IndexerId> = Vec::with_capacity(num_candidates);
    for id in result {
        selected.push(id.parse().map_err(|err| {
            PyTypeError::new_err(format!("Failed to parse indexer ID from '{id}': {err:#}"))
        })?);
    }
    Ok(selected)
}
