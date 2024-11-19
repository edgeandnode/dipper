//! Rust wrapper for the `iisa` Python module.

mod bq;
mod data_manager;
mod geoip;
mod network;

pub use bq::PyBigQueryProvider;
pub use data_manager::PyDataManager;
pub use geoip::PyGeoipResolver;
pub use network::PyNetworkProvider;
use pyo3::{sync::GILOnceCell, types::PyModule, Bound, Py, PyResult, Python};

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
    mod it_iisa_bq;

    // TODO: Fix test dependency with network module
    // mod it_iisa_data_manager;
    mod it_iisa_geoip;
    mod it_iisa_network;
}
