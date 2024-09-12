use pyo3::prelude::*;
use thegraph_core::indexer_id;

use super::common;
use crate::{
    indexer_selection::iisa::{PyGeoipResolver, PyNetworkProvider},
    network::Indexer,
};

#[test]
fn set_snapshot() {
    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_network::set_snapshot");
    common::init_test_tracing();

    Python::with_gil(|py| {
        //* Given
        let indexer1 = Indexer {
            id: indexer_id!("a0803Bab25068a0FdA2f39C52ef7caed4574CC2F"),
            url: "https://one.one.one.one".parse().expect("invalid url"), // 1.0.0.1 or 1.1.1.1
            staked_tokens: Default::default(),
            indexings: Default::default(),
        };
        let indexer2 = Indexer {
            id: indexer_id!("229eB37fC17FF7387D5c51Ddb973D53c13Abfc12"),
            url: "https://dns.google".parse().expect("invalid url"), // 8.8.8.8 or 8.8.4.4
            staked_tokens: Default::default(),
            indexings: Default::default(),
        };

        // Instantiate the NetworkProvider class
        let network_provider = {
            let geoip_resolver = PyGeoipResolver::new(py).expect("instantiate geoip resolver");
            PyNetworkProvider::new(py, geoip_resolver).expect("convert network provider")
        };

        //* When
        // Set the network snapshot
        network_provider
            .set_snapshot(py, [&indexer1, &indexer2])
            .expect("set network provider snapshot");

        // Get the indexers from the network provider
        let result = network_provider.indexers().expect("get indexers");

        //* Then
        // Assert the indexers dataframe is not empty
        let is_empty = result
            .getattr("empty")
            .expect("get empty")
            .extract::<bool>()
            .expect("extract empty");
        assert!(!is_empty);

        // Assert the indexers dataframe has 2 rows
        let (rows, _) = result
            .getattr("shape")
            .expect("get shape")
            .extract::<(usize, usize)>()
            .expect("extract shape");
        assert_eq!(rows, 2);
    });
}
