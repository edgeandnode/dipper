use pyo3::{prelude::*, types::PyDate};

use super::common;
use crate::indexer_selection::iisa::PyBigQueryProvider;

#[test]
#[ignore = "requires Google BigQuery credentials"]
fn fetch_initial_query_results() {
    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_bq::fetch_initial_query_results");
    common::init_test_tracing();

    Python::with_gil(|py| {
        //* Given
        let start_date =
            PyDate::new_bound(py, 2024, 9, 1).expect("Failed to create a new PyDate instance");
        let num_days = 10;

        let bigquery_provider = PyBigQueryProvider::new(py, "graph-mainnet", "US")
            .expect("Failed to create a new PyBigQueryProvider instance");

        //* When
        let result = bigquery_provider
            .fetch_initial_query_results(start_date, num_days)
            .expect("Failed to fetch initial query results");

        //* Then
        // Assert that the initial query results dataframe has at least 1 row
        let (rows, _) = result
            .getattr("shape")
            .expect("Failed to get shape")
            .extract::<(usize, usize)>()
            .expect("Failed to extract shape");
        assert!(rows >= 1);
    });
}
