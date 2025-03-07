//! Rust wrapper for the `iisa` Python module.

mod select;

use pyo3::{Bound, Py, PyResult, Python, sync::GILOnceCell, types::PyModule};
pub use select::{select_many, select_one};

/// Import the `iisa` python module.
///
/// Internally caches the module object to avoid repeated imports.
fn import_iisa_module(py: Python) -> PyResult<&Bound<PyModule>> {
    static IISA_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();
    IISA_MODULE
        .get_or_try_init(py, || py.import("iisa").map(Bound::unbind))
        .map(|module| module.bind(py))
}

#[cfg(test)]
mod tests {
    mod common;
    mod it_iisa_select;
}
