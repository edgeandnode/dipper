use std::path::PathBuf;

use pyo3::{
    ffi::c_str,
    prelude::{PyAnyMethods, PyListMethods},
    types::{IntoPyDict, PyList},
    Bound, Python,
};
use tracing_log::LogTracer;
use tracing_subscriber::{fmt::TestWriter, EnvFilter};

use crate::py::logging;

/// Get project root path.
fn project_root_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Get the path to the python sources directory for testin
fn assets_path() -> PathBuf {
    let mut path = project_root_path();
    path.push("dipper-iisa-python/src");
    path
}

/// Add the assets directory to the Python path.
pub fn add_assets_dir_to_sys_path() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let sys_path = py
            .import("sys")
            .expect("Failed to import sys")
            .getattr("path")
            .expect("Failed to get sys.path");
        let sys_path: Bound<PyList> = sys_path
            .extract()
            .expect("Failed to convert sys.path to PyList");

        // Add the assets directory to the Python path, if it is not already there
        let assets_path = assets_path();
        let assets_path_str = assets_path.to_string_lossy();
        let is_in_sys_path = sys_path
            .iter()
            .any(|path| path.extract::<String>().unwrap() == assets_path_str);
        if !is_in_sys_path {
            sys_path
                .insert(0, assets_path)
                .expect("Failed to insert iisa module into sys.path");
        }
        tracing::debug!("sys.path: {:?}", sys_path);
    });
}

/// Test method to initialize the tests tracing subscriber.
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

/// Initialize the Python `logging` module to redirect logs to the test writer.
pub fn init_python_logging(target: &str) {
    // Register the `RustHostHandler` logger in the Python logging module
    logging::register().expect("Failed to register host logger");

    // Initialize the Python logging module's root logger
    Python::with_gil(|py| {
        py.run(
            c_str!(indoc::indoc! {r#"
                import logging

                # Set initial logging config
                logging.basicConfig(level=logging.DEBUG)
                logging.captureWarnings(True)

                # Remove all existing handlers. Add a new HostLogHandler instance
                # to the root logger
                root_logger = logging.getLogger()

                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)

                root_logger.addHandler(logging.HostLogHandler(target))
                "#}),
            None,
            Some(&[("target", target)].into_py_dict(py).unwrap()),
        )
        .expect("Failed to initialize Python logging module");
    });

    // Configure the global `log` logger to redirect all logs to `tracing` logger
    let _ = LogTracer::init_with_filter(log::LevelFilter::Trace);
}

/// Get the `ipinfo.io` API key from the environment.
pub fn ipinfo_io_auth() -> String {
    std::env::var("IT_TEST_IPINFO_IO_AUTH").expect("Missing IT_TEST_IPINFO_IO_AUTH env var")
}
