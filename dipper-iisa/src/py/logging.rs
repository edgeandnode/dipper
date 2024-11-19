use log::{logger, Level, MetadataBuilder, Record};
use pyo3::{
    ffi::c_str,
    intern, pyfunction,
    sync::GILOnceCell,
    types::{PyAny, PyAnyMethods, PyListMethods, PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, Py, PyResult, Python,
};

/// Import the Python `logging` module.
fn import_logging(py: Python) -> PyResult<&Bound<PyModule>> {
    static MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();
    MODULE
        .get_or_try_init(py, || py.import("logging").map(Bound::unbind))
        .map(|module| module.bind(py))
}

/// Convenience function to register the rust logger with the Python logging instance.
pub fn register() -> PyResult<()> {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        // Extend the `logging` module to interact with log
        setup_logging(py)
    })
}

/// Consume a Python `logging.LogRecord` and emit a Rust `log` instead.
#[pyfunction]
fn host_log(target: &str, record: Bound<'_, PyAny>) -> PyResult<()> {
    let py = record.py();

    let level = record.getattr(intern!(py, "levelno"))?;
    let pathname = record.getattr(intern!(py, "pathname"))?.to_string();
    let lineno = record.getattr(intern!(py, "lineno"))?.extract()?;

    let full_target = {
        let logger_name = record.getattr(intern!(py, "name"))?.to_string();
        if !logger_name.trim().is_empty() && logger_name != "root" {
            // Libraries (ex: tracing_subscriber::filter::Directive) expect rust-style targets like foo::bar,
            // and may not deal well with "." as a module separator:
            let logger_name = logger_name.replace('.', "::");
            Some(format!("{target}::{logger_name}"))
        } else {
            None
        }
    };

    let message = record
        .getattr(intern!(py, "getMessage"))?
        .call0()?
        .to_string();

    let mut metadata_builder = MetadataBuilder::new();
    metadata_builder.target(full_target.as_deref().unwrap_or(target));

    if level.ge(40u8)? {
        metadata_builder.level(Level::Error)
    } else if level.ge(30u8)? {
        metadata_builder.level(Level::Warn)
    } else if level.ge(20u8)? {
        metadata_builder.level(Level::Info)
    } else if level.ge(10u8)? {
        metadata_builder.level(Level::Debug)
    } else {
        metadata_builder.level(Level::Trace)
    };

    logger().log(
        &Record::builder()
            .metadata(metadata_builder.build())
            .args(format_args!("{}", &message))
            .line(Some(lineno))
            .file(Some(&pathname))
            .module_path(Some(&pathname))
            .build(),
    );

    Ok(())
}

/// Registers the `_host_log` function in rust as the event handler for Python's logging logger
/// This function needs to be called from within a _pyo3_ context as early as possible to ensure
/// logging messages arrive to the rust consumer.
///
/// If the `_host_log` function is already registered, this function does nothing.
fn setup_logging(py: Python) -> PyResult<()> {
    let logging = import_logging(py)?;

    if !logging.hasattr("_host_log")? {
        // Register the `_host_log` function in the Python `logging` module
        logging.setattr("_host_log", wrap_pyfunction!(host_log, logging)?)?;
    }

    if !logging.hasattr("HostLogHandler")? {
        tracing::trace!("Registering HostLogHandler in Python logging module");

        // Define the `HostLogHandler` class in the Python `logging` module
        py.run(
            c_str!(indoc::indoc! {r#"
            class HostLogHandler(Handler):
                def __init__(self, target, level=NOTSET):
                    super().__init__(level)
                    self._target = target

                def emit(self, record):
                    _host_log(self._target, record)
            "#,
            }),
            Some(&logging.dict()),
            None,
        )?;

        // Add the `HostLogHandler` class name to the module's `__all__` list
        let all = logging.index()?;
        all.append("HostLogHandler")?;
    }

    Ok(())
}
