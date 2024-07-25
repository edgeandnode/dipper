use std::{fs, path::PathBuf};

use pyo3::{prelude::*, types::IntoPyDict};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexerSelectionError {
    #[error("missing indexer selection module")]
    MissingIndexerSelectionModule,
    #[error("python error: {0}")]
    PythonError(#[from] PyErr),
}

pub struct IndexerSelectionConfig<'a> {
    classname: &'a str,
    class_path: PathBuf,
}

impl<'a> IndexerSelectionConfig<'a> {
    pub fn from_path(classname: &'a str, class_path: PathBuf) -> Self {
        IndexerSelectionConfig {
            classname,
            class_path,
        }
    }
}

pub struct IndexerSelector<'a> {
    py_instance: PyObject,
    config: &'a IndexerSelectionConfig<'a>,
}

// TODO: use a real address type
pub type Address = String;

pub struct Selection {
    pub indexers: Vec<Address>,
}

impl<'a> IndexerSelector<'a> {
    pub fn new(
        config: &'a IndexerSelectionConfig,
        subgraph_info: &[(&str, &str)], // FIXME: use a real type
    ) -> Result<Self, IndexerSelectionError> {
        Python::with_gil(|py| {
            let path = config.class_path.as_path().display().to_string();
            let module_name = config
                .class_path
                .file_stem()
                .ok_or(IndexerSelectionError::MissingIndexerSelectionModule)?
                .to_str()
                .ok_or(IndexerSelectionError::MissingIndexerSelectionModule)?;

            let code = fs::read_to_string(&config.class_path)
                .map_err(|_| IndexerSelectionError::MissingIndexerSelectionModule)?;

            let module = PyModule::from_code_bound(py, &code, &path, module_name)?;

            let indexer_selector_class = module.getattr(config.classname)?;
            let subgraph_info = subgraph_info.into_py_dict_bound(py);

            // Instantiate the IndexerSelector class
            let py_instance = indexer_selector_class.call1((subgraph_info,))?.into();
            Ok(IndexerSelector {
                py_instance,
                config,
            })
        })
    }

    pub fn select_indexers(&self) -> Result<Selection, IndexerSelectionError> {
        Python::with_gil(|py| {
            let indexers = self
                .py_instance
                .getattr(py, "select_indexers")?
                .call0(py)?
                .extract::<Vec<Address>>(py)?;

            Ok(Selection { indexers })
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_indexer_selector() {
        let temp_dir = tempdir().unwrap();
        let temp_dir_path = temp_dir.path();

        let indexer_selection_module = temp_dir_path.join("indexer_selection.py");
        let indexer_selection_module_content = r#"
class IndexerSelector:
    def __init__(self, subgraph_info):
        self.subgraph_info = subgraph_info

    def select_indexers(self):
        return ["indexer1", "indexer2"]
"#;
        fs::write(&indexer_selection_module, indexer_selection_module_content).unwrap();

        let config = IndexerSelectionConfig::from_path("IndexerSelector", indexer_selection_module);

        let indexer_selector =
            IndexerSelector::new(&config, &[("one", "two"), ("two", "three")]).unwrap();
        let selection = indexer_selector.select_indexers().unwrap();
        assert_eq!(selection.indexers, vec!["indexer1", "indexer2"]);
    }
}
