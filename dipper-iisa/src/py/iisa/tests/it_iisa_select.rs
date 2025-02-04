use fake::{Fake, Faker};
use pyo3::prelude::*;
use thegraph_core::IndexerId;

use super::common;
use crate::py::iisa;

#[test]
fn select_one_from_list() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_one_from_list");

    Python::with_gil(|py| {
        //* Given
        let indexers: Vec<IndexerId> = vec![
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
        ];

        //* When
        let result = iisa::select_one(py, indexers.iter()).expect("function call");

        //* Then
        assert!(result.is_some());

        let selected_indexer = result.unwrap();
        assert!(indexers.contains(&selected_indexer));
    });
}

#[test]
fn select_one_from_empty_list() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_one_from_empty_list");

    Python::with_gil(|py| {
        //* Given
        let indexers: Vec<IndexerId> = vec![];

        //* When
        let result = iisa::select_one(py, indexers.iter()).expect("function call");

        //* Then
        assert!(result.is_none());
    });
}

#[test]
fn select_many_from_list() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_many_from_list");

    Python::with_gil(|py| {
        //* Given
        let num_candidates = 2;

        let indexers: Vec<IndexerId> = vec![
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
        ];

        //* When
        let result = iisa::select_many(py, indexers.iter(), num_candidates).expect("function call");

        //* Then
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|id| indexers.contains(id)));
    });
}

#[test]
fn select_many_from_list_with_less_candidates() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_many_from_list_with_less_candidates");

    Python::with_gil(|py| {
        //* Given
        let num_candidates = 5;

        let indexers: Vec<IndexerId> = vec![
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
        ];

        //* When
        let result = iisa::select_many(py, indexers.iter(), num_candidates).expect("function call");

        //* Then
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|id| indexers.contains(id)));
    });
}

#[test]
fn select_many_from_empty_list() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_many_from_empty_list");

    Python::with_gil(|py| {
        //* Given
        let num_candidates = 2;

        let indexers: Vec<IndexerId> = vec![];

        //* When
        let result = iisa::select_many(py, indexers.iter(), num_candidates).expect("function call");

        //* Then
        assert!(result.is_empty());
    });
}

#[test]
fn select_zero_from_list() {
    common::init_test_tracing();

    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa_select::select_zero_from_list");

    Python::with_gil(|py| {
        //* Given
        let num_candidates = 0;

        let indexers: Vec<IndexerId> = vec![
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
            Faker.fake::<IndexerId>(),
        ];

        //* When
        let result = iisa::select_many(py, indexers.iter(), num_candidates).expect("function call");

        //* Then
        assert!(result.is_empty());
    });
}
