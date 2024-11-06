use pyo3::{prelude::*, types::PyDict};

use super::common;
use crate::py::iisa::PyGeoipResolver;

#[test]
fn resolve_url_host_info() {
    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_geoip::resolve_url_host_info");
    common::init_test_tracing();

    Python::with_gil(|py| {
        //* Given
        let url_str = "https://one.one.one.one";

        let geoip_resolver =
            PyGeoipResolver::new(py).expect("Failed to create PyGeoipResolver instance");

        //* When
        let result = geoip_resolver
            .resolve_url_host_info(url_str)
            .expect("Failed to resolve URL host info")
            .downcast_into::<PyDict>()
            .expect("Failed to downcast into PyDict");

        //* Then
        let ip_addr = result
            .get_item("ip_addr")
            .unwrap()
            .unwrap()
            .extract::<Option<String>>()
            .unwrap();
        let org = result
            .get_item("org")
            .unwrap()
            .unwrap()
            .extract::<Option<String>>()
            .unwrap();
        let country = result
            .get_item("latitude")
            .unwrap()
            .unwrap()
            .extract::<Option<f64>>()
            .unwrap();
        let latitude = result
            .get_item("latitude")
            .unwrap()
            .unwrap()
            .extract::<Option<f64>>()
            .unwrap();
        let longitude = result
            .get_item("longitude")
            .unwrap()
            .unwrap()
            .extract::<Option<f64>>()
            .unwrap();

        assert!(ip_addr.is_some());
        assert!(org.is_some());
        assert!(country.is_some());
        assert!(latitude.is_some());
        assert!(longitude.is_some());
    });
}
