from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from iisa.iisa_functions import (
    _CalculateDistancesInputDataFrame,
    _CalculateDistancesInputSchema,
    _CalculateDistancesMixinSchema,
    _FilterSuccessfulQueriesInputDataFrame,
    _FilterSuccessfulQueriesInputSchema,
    _FilterSuccessfulQueriesMixinSchema,
    _MergeInIndexersInfoInputDataFrame,
    _MergeInIndexersInfoInputSchema,
    _MergeInIndexersInfoMixinSchema,
    _MergeInQueryGeolocationInputDataFrame,
    _MergeInQueryGeolocationInputSchema,
    _MergeInQueryGeolocationMixinSchema,
    adjust_rows,
    calculate_distances,
    filter_columns,
    iterative_filter,
    strategic_sample,
    hash_sampled_queries,
    perform_latency_linear_regression,
    preprocess_data_for_latency_linear_regression,
    create_latency_linear_regression_pipeline,
    analyze_latency_linear_regression_results,
    calculate_robust_normalized_coefficients_latency_linear_regression,
    calculate_indexer_success_rate,
    calculate_indexer_uptime,
    calculate_indexer_stake_to_fees,
    aggregate_indexer_info,
    merge_and_prepare_dataframes,
    normalize_metrics,
    normalize_generic,
    normalize_uptime_and_success_rate,
    normalize_indexing_agreement_acceptance_latency,
    calculate_weighted_score,
    filter_successful_queries,
    merge_in_indexers_info,
    merge_in_query_geolocation_info,
)
from iisa.network import IndexersDataFrame, IndexersSchema
from iisa.typing import empty_dataframe


class TestAdjustRows:
    """
    Tests for the adjust_rows function.

    This class tests various scenarios for adjusting the number of rows
    in a DataFrame to approximate a target total number of rows.
    """

    def test_adjust_rows_normal_case(self):
        # Setup sample data
        sample_data = pd.DataFrame(
            {
                "deployment_hash": ["hash1", "hash2", "hash3", "hash1"],
                "indexer": ["index1", "index2", "index3", "indexer4"],
                "num_rows": [50, 10000, 600, 50],
            }
        )

        # Test if adjustments approximate the target within the specified tolerance.
        target_rows = 600
        adjust_rows(sample_data, target_rows)
        adjusted_sum = sample_data["num_rows_restricted"].sum()
        assert target_rows * 0.99 <= adjusted_sum <= target_rows * 1.01

    def test_adjust_rows_empty_dataframe(self):
        # Setup an empty DataFrame
        df = pd.DataFrame({"deployment_hash": [], "indexer": [], "num_rows": []})

        # Test handling of empty data
        target_rows = 100
        adjust_rows(df, target_rows)
        assert df.empty

    def test_adjust_rows_zero_target(self):
        # Setup sample data
        sample_data = pd.DataFrame(
            {
                "deployment_hash": ["hash1", "hash2", "hash3", "hash1"],
                "indexer": ["index1", "index2", "index3", "indexer4"],
                "num_rows": [50, 10000, 600, 50],
            }
        )

        # Test response when the target number of rows is zero
        target_rows = 0
        adjust_rows(sample_data, target_rows)
        assert sample_data["num_rows_restricted"].sum() == 0

    def test_adjust_rows_negative_case(self):
        # Setup sample data with uniform distribution
        df = pd.DataFrame(
            {
                "deployment_hash": ["hash1", "hash1", "hash1", "hash1"],
                "indexer": ["index1", "index1", "index1", "index1"],
                "num_rows": [100, 100, 100, 100],
            }
        )

        # Test handling of negative target rows
        target_rows = -300
        with pytest.raises(
            ValueError, match="Target rows must be a non-negative integer"
        ):
            adjust_rows(df, target_rows)


class TestFilterStatus:
    def test_filter_200_ok_status(self):
        ## Given
        data = _FilterSuccessfulQueriesInputDataFrame(
            {
                "status": [
                    "200 OK",
                    "404 Not Found",
                    "200 OK",
                    "500 Internal Server Error",
                    "200 OK",
                ],
                "data": ["A", "B", "C", "D", "E"],
            }
        )

        ## When
        result = filter_successful_queries(data)

        ## Then
        # Assert the result complies with the output schema
        _FilterSuccessfulQueriesMixinSchema.validate(result)

        # Assert the result content
        assert len(result) == 3
        assert list(result["data"]) == ["A", "C", "E"]
        assert result["status"].eq("200 OK").all()

    def test_filter_status_empty_df(self):
        ## Given
        data = empty_dataframe(_FilterSuccessfulQueriesInputSchema)

        ## When
        result = filter_successful_queries(data)

        ## Then
        # Assert the result complies with the output schema
        _FilterSuccessfulQueriesMixinSchema.validate(result)

        # Assert the result is empty
        assert result.empty

    def test_filter_status_with_nan_values(self):
        ## Given
        data = _FilterSuccessfulQueriesInputDataFrame(
            {
                "status": ["200 OK", pd.NA, "200 OK", None],
                "data": ["A", "B", "C", "D"],
            }
        )

        ## When
        result = filter_successful_queries(data)

        ## Then
        # Assert the result complies with the output schema
        _FilterSuccessfulQueriesMixinSchema.validate(result)

        # Assert the result content
        assert len(result) == 2
        assert list(result["data"]) == ["A", "C"]
        assert result["status"].eq("200 OK").all()


class TestMergeInIndexersDataFrame:
    @pytest.fixture
    def combined_query_pandas(self):
        return _MergeInIndexersInfoInputDataFrame(
            {
                "indexer": [
                    "0x123fffffffffffffffffffffffffffffffffffff",
                    "0x456fffffffffffffffffffffffffffffffffffff",
                    "0x789fffffffffffffffffffffffffffffffffffff",
                ],
                "url": [
                    "https://example.com",
                    "https://test.com",
                    "https://another.com",
                ],
            }
        )

    @pytest.fixture
    def indexers(self):
        return IndexersDataFrame(
            {
                "indexer": [
                    "0x123fffffffffffffffffffffffffffffffffffff",
                    "0x456fffffffffffffffffffffffffffffffffffff",
                ],
                "url": ["https://example.com", "https://test.com"],
                "indexer_network": ["arbitrum", "arbitrum"],
                "ip_addr": ["1.1.2.2", "3.3.4.4"],
                "org": ["Org1", "Org2"],
                "country": ["US", "CN"],
                "latitude": [1.0, 2.0],
                "longitude": [1.0, 2.0],
            }
        )

    def test_merge_in_indexers_info(self, combined_query_pandas, indexers):
        ## When
        result = merge_in_indexers_info(combined_query_pandas, indexers)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInIndexersInfoInputSchema.validate(result)
        _MergeInIndexersInfoMixinSchema.validate(result)

        # Assert the result content
        expected = pd.DataFrame(
            {
                "indexer": [
                    "0x123fffffffffffffffffffffffffffffffffffff",
                    "0x456fffffffffffffffffffffffffffffffffffff",
                    "0x789fffffffffffffffffffffffffffffffffffff",
                ],
                "url": [
                    "https://example.com",
                    "https://test.com",
                    "https://another.com",
                ],
                "indexer_network": ["arbitrum", "arbitrum", None],
                "ip_addr": ["1.1.2.2", "3.3.4.4", None],
                "org": ["Org1", "Org2", None],
                "dst_country": ["US", "CN", None],
                "dst_lat": [1.0, 2.0, None],
                "dst_lon": [1.0, 2.0, None],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_merge_into_empty_combined_queries_results(self, indexers):
        ## Given
        data = empty_dataframe(_MergeInIndexersInfoInputSchema)

        ## When
        result = merge_in_indexers_info(data, indexers)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInIndexersInfoInputSchema.validate(result)
        _MergeInIndexersInfoMixinSchema.validate(result)

        # Assert that the result is an empty dataframe
        assert result.empty

    def test_merge_in_empty_indexers_info(self, combined_query_pandas):
        ## Given
        indexers_df = empty_dataframe(IndexersSchema)

        ## When
        result = merge_in_indexers_info(combined_query_pandas, indexers_df)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInIndexersInfoInputSchema.validate(result)
        _MergeInIndexersInfoMixinSchema.validate(result)

        # Assert the result is non-empty and the new columns are filled with NaN values
        assert not result.empty

        assert result["indexer_network"].isna().all()
        assert result["ip_addr"].isna().all()
        assert result["org"].isna().all()
        assert result["dst_country"].isna().all()
        assert result["dst_lat"].isna().all()
        assert result["dst_lon"].isna().all()


class TestMergeInQueryGeolocationInfo:
    def test_merge_in_iata_info(self):
        ## Given
        data = _MergeInQueryGeolocationInputDataFrame(
            {
                "query_id": [
                    "1111111111111111-AMS",
                    "2222222222222222-CDG",
                    "3333333333333333-LHR",
                ],
            }
        )

        ## When
        result = merge_in_query_geolocation_info(data)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInQueryGeolocationInputSchema.validate(result)
        _MergeInQueryGeolocationMixinSchema.validate(result)

        # Assert the result content
        expected = pd.DataFrame(
            {
                "query_id": [
                    "1111111111111111-AMS",
                    "2222222222222222-CDG",
                    "3333333333333333-LHR",
                ],
                "IATA_code": ["AMS", "CDG", "LHR"],
                "src_country": ["NL", "FR", "GB"],
                "src_lat": [52.3086, 49.0128, 51.4706],
                "src_lon": [4.7639, 2.5500, -0.46194],
            }
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_merge_with_unknown_iata_code(self):
        ## Given
        data = _MergeInQueryGeolocationInputDataFrame(
            {
                "query_id": [
                    "1111111111111111-AMS",
                    "2222222222222222-CDG",
                    "3333333333333333-LHR",
                    "0000000000000000-XXX",
                ],
            }
        )

        ## When
        result = merge_in_query_geolocation_info(data)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInQueryGeolocationInputSchema.validate(result)
        _MergeInQueryGeolocationMixinSchema.validate(result)

        # Assert the result content
        assert result.loc[3, "IATA_code"] == "XXX"
        assert pd.isna(result.loc[3, "src_country"])
        assert pd.isna(result.loc[3, "src_lat"])
        assert pd.isna(result.loc[3, "src_lon"])

    def test_merge_with_empty_dataframe(self):
        ## Given
        data = empty_dataframe(_MergeInQueryGeolocationInputSchema)

        ## When
        result = merge_in_query_geolocation_info(data)

        ## Then
        # Assert the result complies with the input and output schemas
        _MergeInQueryGeolocationInputSchema.validate(result)
        _MergeInQueryGeolocationMixinSchema.validate(result)

        # Assert result is as expected.
        assert result.empty


class TestCalculateDistances:
    @pytest.fixture
    def sample_df(self):
        return _CalculateDistancesInputDataFrame(
            {
                "src_lon": [-74.4444, -118.8888, -0.3333],
                "src_lat": [40.5555, 34.9999, 51.4444],
                "dst_lon": [-87.6666, -122.1111, 2.5555],
                "dst_lat": [41.7777, 37.2222, 48.6666],
            }
        )

    def test_calculate_distance(self):
        ## Given
        data = _CalculateDistancesInputDataFrame(
            {
                "src_lon": [0.0],
                "src_lat": [0.0],
                "dst_lon": [30.0],
                "dst_lat": [0.0],
            }
        )

        ## When
        result = calculate_distances(data)

        ## Then
        # Assert the result complies with the output schema
        _CalculateDistancesMixinSchema.validate(result)

        # Assert the result content
        expected_distance = 2000  # 2072.7 miles, approximate distance for 30 degrees of longitude at the equator
        assert result["distance_miles"].iloc[0] == expected_distance

    def test_calculate_distances_multiple(self, sample_df):
        ## When
        result = calculate_distances(sample_df)

        ## Then
        # Assert the result complies with the output schema
        _CalculateDistancesMixinSchema.validate(result)

        # Assert the result content
        assert len(sample_df) == len(result)
        assert result["distance_miles"].notna().all()

    def test_calculate_same_location(self):
        ## Given
        data = _CalculateDistancesInputDataFrame(
            {
                "src_lon": [10.0, 20.0],
                "src_lat": [10.0, 20.0],
                "dst_lon": [10.0, 20.0],
                "dst_lat": [10.0, 20.0],
            }
        )

        ## When
        result = calculate_distances(data)

        ## Then
        # Assert the result complies with the output schema
        _CalculateDistancesMixinSchema.validate(result)

        # Assert all distances are zero
        assert result["distance_miles"].eq(0.0).all()

    def test_calculate_distances_empty_df(self):
        ## Given
        data = empty_dataframe(_CalculateDistancesInputSchema)

        ## When
        result = calculate_distances(data)

        ## Then
        # Assert the result complies with the output schema
        _CalculateDistancesMixinSchema.validate(result)

        # Assert the result is an empty DataFrame
        assert result.empty

    def test_calculate_distances_nan_values(self):
        ## Given
        data = _CalculateDistancesInputDataFrame(
            {
                "src_lat": [40.99, None, 51.20, 37.25],
                "src_lon": [-74.00, -118.00, None, -122.00],
                "dst_lat": [40.99, 37.25, 48.10, None],
                "dst_lon": [-84.00, None, 2.20, 2.20],
            }
        )

        ## When
        result = calculate_distances(data)

        ## Then
        # Assert the result complies with the output schema
        _CalculateDistancesMixinSchema.validate(result)

        # Assert the result's first row has a non-zero distance
        assert result["distance_miles"].iloc[0] >= 0

        # Assert the rest of the distances are NaN
        assert pd.isna(result["distance_miles"].iloc[1])
        assert pd.isna(result["distance_miles"].iloc[2])
        assert pd.isna(result["distance_miles"].iloc[3])


class TestFilterColumns:
    def test_filter_columns_basic(self):
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": ["x", "y", "z"], "C": [True, False, True]}
        )

        # Test basic functionality
        result = filter_columns(df, ["A", "C"])
        assert list(result.columns) == ["A", "C"]
        assert len(result) == 3

        # Test with all columns
        assert filter_columns(df, df.columns).equals(df)

        # Test with empty column list
        assert len(filter_columns(df, []).columns) == 0

        # Test with non-existent column
        with pytest.raises(KeyError):
            filter_columns(df, ["A", "D"])


class TestIterativeFilter:
    @pytest.fixture
    def sample_df(self):
        data = {
            "deployment_hash": ["A"] * 12 + ["B"] * 8 + ["C"] * 8 + ["D"] * 4,
            "indexer": (["X", "Y", "Z"] * 10 + ["X", "Y"])[:32],
            "query_id": list(range(1, 33)),
        }
        return pd.DataFrame(data)

    def test_iterative_filter_base_case(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=2,
            min_deployments_per_indexer=2,
            min_queries_per_indexer=2,
            min_queries_per_deployment=2,
        )
        assert len(result) == 32
        assert result["deployment_hash"].value_counts().to_dict() == {
            "A": 12,
            "B": 8,
            "C": 8,
            "D": 4,
        }
        assert result["indexer"].value_counts().to_dict() == {"X": 11, "Y": 11, "Z": 10}
        assert len(result["query_id"].unique()) == 32

    def test_iterative_filter_no_change(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=0,
            min_deployments_per_indexer=0,
            min_queries_per_indexer=0,
            min_queries_per_deployment=0,
        )
        pd.testing.assert_frame_equal(result, sample_df)

    def test_iterative_filter_empty_result(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=100,
            min_deployments_per_indexer=100,
            min_queries_per_indexer=100,
            min_queries_per_deployment=100,
        )
        assert len(result) == 0

    def test_iterative_filter_indexers_per_deployment_only(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=3,
            min_deployments_per_indexer=0,
            min_queries_per_indexer=0,
            min_queries_per_deployment=0,
        )
        assert len(result) == 32
        assert set(result["deployment_hash"]) == {"A", "B", "C", "D"}

    def test_iterative_filter_deployments_per_indexer_only(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=0,
            min_deployments_per_indexer=4,
            min_queries_per_indexer=0,
            min_queries_per_deployment=0,
        )
        assert len(result) == 32
        assert set(result["indexer"]) == {"X", "Y", "Z"}

    def test_iterative_filter_queries_per_indexer_only(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=0,
            min_deployments_per_indexer=0,
            min_queries_per_indexer=11,
            min_queries_per_deployment=0,
        )
        assert len(result) == 22
        assert set(result["indexer"]) == {"X", "Y"}

    def test_iterative_filter_queries_per_deployment_only(self, sample_df):
        result = iterative_filter(
            sample_df,
            min_deployment_indexers=0,
            min_deployments_per_indexer=0,
            min_queries_per_indexer=0,
            min_queries_per_deployment=10,
        )
        assert len(result) == 12
        assert set(result["deployment_hash"]) == {"A"}

    def test_iterative_filter_empty_dataframe(self):
        df = pd.DataFrame(columns=["deployment_hash", "indexer", "query_id"])
        result = iterative_filter(
            df,
            min_deployment_indexers=0,
            min_deployments_per_indexer=0,
            min_queries_per_indexer=0,
            min_queries_per_deployment=0,
        )
        assert len(result) == 0
        assert list(result.columns) == ["deployment_hash", "indexer", "query_id"]


class TestStrategicSample:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "deployment_hash": ["A", "A", "A", "B", "B", "C"] * 10000,
                "indexer": ["X", "Y", "Z", "X", "Y", "X"] * 10000,
                "query_id": range(60000),
            }
        )

    def test_strategic_sample_basic(self, sample_df):
        # Compute the result
        result_df, integer_root = strategic_sample(
            sample_df, target_rows_per_subgraph=30
        )

        # Sample the sampled_query_id's
        sampled = result_df[result_df["sampled_query_id"].notna()]

        # Check the length of the output df has not changed
        assert len(result_df) == len(sample_df)

        # Check the number of not none rows in the output df as as expected
        assert result_df["sampled_query_id"].notna().sum() == 90

        # Verify the integer root is the expected integer
        assert isinstance(integer_root, int)
        assert integer_root == int(np.sqrt(result_df["sampled_query_id"].notna().sum()))

        # Calculate the number of unique indexers per deployment_hash
        indexers_per_subgraph = sampled.groupby("deployment_hash")["indexer"].nunique()

        # Verify there is at least 1 indexer per subgraph
        assert indexers_per_subgraph.min() > 0

        # For this case verify the spread of the number of indexers serving sugraphs is exactly 2.
        assert (indexers_per_subgraph.max() - indexers_per_subgraph.min()) == 2

    def test_strategic_sample_empty_df(self):
        empty_df = pd.DataFrame(columns=["deployment_hash", "indexer", "query_id"])
        result_df, integer_root = strategic_sample(
            empty_df, target_rows_per_subgraph=10
        )

        assert result_df.empty
        assert "sampled_query_id" in result_df.columns
        assert integer_root == 0

    def test_strategic_sample_target_rows_per_subgraph_greater_than_df(self, sample_df):
        # Compute the result
        result_df, integer_root = strategic_sample(
            sample_df, target_rows_per_subgraph=10_000_000_000_000
        )

        # Check the length of the output df has not changed
        assert len(result_df) == len(sample_df)

        # Check that each query ID has been sampled exactly once. (since target_rows_per_subgraph > len(sample_df))
        assert (
            result_df["sampled_query_id"].notna().sum()
            == sample_df["query_id"].nunique()
        )


class TestHashSampledQueries:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "sampled_query_id": [1, 2, 3, None, 5, 6, np.nan, 8, 9, 10] * 1_000,
                "other_column": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
                * 1_000,
            }
        )

    def test_hash_sampled_queries_basic(self, sample_df):
        integer_root = 33
        result = hash_sampled_queries(sample_df, integer_root)

        # Check that the new column is added
        assert "sampled_query_id_hashed_mod_integer_root" in result.columns

        # Check that only non-null sampled_query_id rows are hashed
        assert result["sampled_query_id_hashed_mod_integer_root"].notna().sum() == 8000
        assert result["sampled_query_id_hashed_mod_integer_root"].isna().sum() == 2000

        # Check that all hashed values are within the expected range
        assert all(
            0 <= x < integer_root
            for x in result["sampled_query_id_hashed_mod_integer_root"].dropna()
        )

    def test_hash_sampled_queries_empty_df(self):
        empty_df = pd.DataFrame(columns=["sampled_query_id"])
        result = hash_sampled_queries(empty_df, 5)
        assert "sampled_query_id_hashed_mod_integer_root" in result.columns
        assert result.empty

    def test_hash_sampled_queries_all_null(self):
        df = pd.DataFrame({"sampled_query_id": [None, None, None]})
        result = hash_sampled_queries(df, 5)
        assert "sampled_query_id_hashed_mod_integer_root" in result.columns
        assert result["sampled_query_id_hashed_mod_integer_root"].isna().all()

    def test_hash_sampled_queries_consistency(self, sample_df):
        integer_root = 7
        result1 = hash_sampled_queries(sample_df, integer_root)
        result2 = hash_sampled_queries(sample_df, integer_root)
        pd.testing.assert_frame_equal(result1, result2)

    def test_hash_sampled_queries_different_integer_roots(self, sample_df):
        result1 = hash_sampled_queries(sample_df.copy(), 5)
        result2 = hash_sampled_queries(sample_df.copy(), 10)

        assert not result1["sampled_query_id_hashed_mod_integer_root"].equals(
            result2["sampled_query_id_hashed_mod_integer_root"]
        )

    def test_hash_sampled_queries_original_df_unchanged(self, sample_df):
        original_df = sample_df.copy()
        _ = hash_sampled_queries(sample_df, 5)
        pd.testing.assert_frame_equal(original_df, sample_df)

    def test_hash_sampled_queries_large_integer_root(self):
        df = pd.DataFrame({"sampled_query_id": range(1000)})
        large_integer_root = 10_000_000_000
        result = hash_sampled_queries(df, large_integer_root)
        assert all(
            0 <= x < large_integer_root
            for x in result["sampled_query_id_hashed_mod_integer_root"]
        )


class TestPerformLinearRegression:
    """
    This integration test tests the perform_latency_linear_regression function and its dependencies:
    preprocess_data_for_latency_linear_regression, perform_latency_linear_regression, analyze_latency_linear_regression_results and
    calculate_robust_normalized_coefficients_latency_linear_regression.
    """

    @pytest.fixture
    def sample_df(self):
        # DataFrame with random data for testing
        np.random.seed(42)
        return pd.DataFrame(
            {
                "sampled_query_id": range(10_000),
                "indexer": np.random.choice(
                    ["0xABC", "0xXYZ", "0x123", "0x789"], 10_000
                ),
                "indexer_network": np.random.choice(
                    ["net1", "net2", "net3", "net4"], 10_000
                ),
                "deployment_hash": np.random.choice(
                    ["deployment_1", "deployment_2", "deployment_3", "deployment_4"],
                    10_000,
                ),
                "response_time_ms": np.random.randint(10, 20_000, 10_000),
                "fee": np.random.uniform(0.000001, 0.01, 10_000),
                "distance_miles": np.random.uniform(0, 1_000, 10_000),
                "score": np.random.uniform(0, 1, 10_000),
            }
        )

    def test_hash_sampled_queries_with_linear_regression(self, sample_df):
        # Apply hash_sampled_queries
        integer_root = 100
        hashed_df = hash_sampled_queries(sample_df, integer_root)

        # Check that the new column is added
        assert (
            "sampled_query_id_hashed_mod_integer_root" in hashed_df.columns
        ), "sampled_query_id_hashed_mod_integer_root not in hash_df.columns"

        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = [
            "indexer",
            "deployment_hash",
            "indexer_network",
            "sampled_query_id_hashed_mod_integer_root",
        ]
        numeric = ["distance_miles", "fee"]

        # Compute result
        (
            result_df,
            latency_linear_regression_indexer_rankings,
            latency_linear_regression_results_df,
        ) = perform_latency_linear_regression(
            hashed_df, predictor, categorical, numeric
        )

        # Check that the result_df contains the original columns
        assert all(
            col in result_df.columns for col in hashed_df.columns
        ), "result_df doesn't contain original columns"

        # Check that latency_linear_regression_indexer_rankings contains expected columns
        expected_columns = [
            "indexer",
            "Latency Coefficient",
            "Standard Error",
            "p-value",
            "Latency Coefficient + Error Confidence Interval",
            "Robust Normalized Latency Coefficient + Error Confidence Interval",
        ]
        assert all(
            col in latency_linear_regression_indexer_rankings.columns
            for col in expected_columns
        ), "latency_linear_regression_results_df doesn't contain expected columns"

        # Check that only indexer values are present in the indexer column
        assert all(
            latency_linear_regression_indexer_rankings["indexer"].isin(
                ["0xABC", "0xXYZ", "0x123", "0x789"]
            )
        ), "indexer values, not present in the indexer column"

        # Check to ensure regression results are reasonable
        assert (
            latency_linear_regression_indexer_rankings["Latency Coefficient"]
            .notna()
            .all()
        )
        assert latency_linear_regression_indexer_rankings["p-value"].between(0, 1).all()

        # Check that the hashed column affects the regression by using a different mod hash integer root
        hashed_df_different_root = hash_sampled_queries(sample_df, integer_root + 1)
        (
            _,
            latency_linear_regression_indexer_rankings_different_root,
            latency_linear_regression_results_df_different_root,
        ) = perform_latency_linear_regression(
            hashed_df_different_root, predictor, categorical, numeric
        )

        assert not latency_linear_regression_indexer_rankings[
            "Latency Coefficient"
        ].equals(
            latency_linear_regression_indexer_rankings_different_root[
                "Latency Coefficient"
            ]
        )

    def test_preprocess_data_for_latency_linear_regression(self, sample_df):
        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Perform preprocessing
        X, y, preprocessor = preprocess_data_for_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )

        # Assert the correct types and structures of the preprocessed data
        assert isinstance(X, pd.DataFrame)
        assert isinstance(y, pd.DataFrame)
        assert isinstance(preprocessor, ColumnTransformer)
        assert list(y.columns) == predictor
        assert set(X.columns) == set(categorical + numeric)

    def test_perform_latency_linear_regression(self, sample_df):
        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Preprocess data and perform regression
        X, y, preprocessor = preprocess_data_for_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )
        pipeline, y_pred = create_latency_linear_regression_pipeline(X, y, preprocessor)

        # Check the types and lengths of the regression outputs
        assert isinstance(pipeline, Pipeline)
        assert isinstance(y_pred, np.ndarray)
        assert len(y_pred) == len(y)

    def test_analyze_latency_linear_regression_results(self, sample_df):
        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Perform regression and analyze results
        X, y, preprocessor = preprocess_data_for_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )
        pipeline, y_pred = create_latency_linear_regression_pipeline(X, y, preprocessor)
        results_df = analyze_latency_linear_regression_results(pipeline, X, y, y_pred)

        # Check the structure and content of the results DataFrame
        assert isinstance(results_df, pd.DataFrame)
        assert set(results_df.columns) == {
            "Variable",
            "Latency Coefficient",
            "Standard Error",
            "p-value",
        }
        assert len(results_df) > 0

    def test_calculate_robust_normalized_coefficients_latency_linear_regression(
        self, sample_df
    ):
        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Perform regression, analyze results, and calculate normalized coefficients
        X, y, preprocessor = preprocess_data_for_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )
        pipeline, y_pred = create_latency_linear_regression_pipeline(X, y, preprocessor)
        results_df = analyze_latency_linear_regression_results(pipeline, X, y, y_pred)
        indexer_rankings = (
            calculate_robust_normalized_coefficients_latency_linear_regression(
                results_df
            )
        )

        # Check the structure and content of the indexer rankings DataFrame
        assert isinstance(indexer_rankings, pd.DataFrame)
        assert set(indexer_rankings.columns) == {
            "indexer",
            "Latency Coefficient",
            "Standard Error",
            "p-value",
            "Latency Coefficient + Error Confidence Interval",
            "Robust Normalized Latency Coefficient + Error Confidence Interval",
        }
        assert len(indexer_rankings) > 0

    def test_perform_latency_linear_regression_with_empty_df(self):
        # Create an empty DataFrame
        empty_df = pd.DataFrame(
            columns=[
                "indexer",
                "deployment_hash",
                "indexer_network",
                "response_time_ms",
                "fee",
                "distance_miles",
                "score",
            ]
        )

        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Check if the function raises an appropriate exception for empty DataFrame
        with pytest.raises(ValueError):
            perform_latency_linear_regression(empty_df, predictor, categorical, numeric)

    def test_perform_latency_linear_regression_with_missing_columns(self, sample_df):
        # Create a DataFrame with missing columns
        df_missing_columns = sample_df.drop(columns=["indexer", "fee"])

        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Check if the function raises an appropriate exception for missing columns
        with pytest.raises(KeyError):
            perform_latency_linear_regression(
                df_missing_columns, predictor, categorical, numeric
            )

    def test_perform_latency_linear_regression_deterministic_verification(
        self, sample_df
    ):
        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Perform linear regression twice and compare results
        (
            result_df1,
            latency_linear_regression_indexer_rankings1,
            latency_linear_regression_results_df1,
        ) = perform_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )
        (
            result_df2,
            latency_linear_regression_indexer_rankings2,
            latency_linear_regression_results_df2,
        ) = perform_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )

        # Check if the results are consistent across multiple runs
        pd.testing.assert_frame_equal(result_df1, result_df2)

        # Check if the results are consistent across multiple runs
        pd.testing.assert_frame_equal(
            latency_linear_regression_indexer_rankings1,
            latency_linear_regression_indexer_rankings2,
        )

        # Check if the results are consistent across multiple runs
        pd.testing.assert_frame_equal(
            latency_linear_regression_results_df1, latency_linear_regression_results_df2
        )

    def test_perform_latency_linear_regression_original_df_unchanged(self, sample_df):
        # Create a copy of the original DataFrame
        original_df = sample_df.copy()

        # Setup linear regression variables
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network"]
        numeric = ["distance_miles", "fee"]

        # Perform linear regression
        (_, _, _) = perform_latency_linear_regression(
            sample_df, predictor, categorical, numeric
        )

        # Check the original DataFrame is unchanged
        pd.testing.assert_frame_equal(original_df, sample_df)


class TestCalculateIndexerSuccessRate:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "indexer": [
                    "0xABC",
                    "0xXYZ",
                    "0x123",
                    "0xABC",
                    "0xXYZ",
                    "0x123",
                    "0xABC",
                    "0xXYZ",
                    "0x123",
                ],
                "status": [
                    "200 OK",
                    "404 Not Found",
                    "Unavailable(MissingBlock)",
                    "500 Internal Server Error",
                    "200 OK",
                    "200 OK",
                    "Unavailable(MissingBlock)",
                    "200 OK",
                    "403 Forbidden",
                ],
            }
        )

    def test_calculate_indexer_success_rate_basic(self, sample_df):
        result = calculate_indexer_success_rate(sample_df)

        # Check the structure of the result
        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {"indexer", "average_status"}

        # Check the content of the result
        expected_result = pd.DataFrame(
            {
                "indexer": ["0x123", "0xABC", "0xXYZ"],
                "average_status": [2 / 3, 2 / 3, 2 / 3],
            }
        )
        pd.testing.assert_frame_equal(result, expected_result, check_exact=False)

    def test_calculate_indexer_success_rate_all_fail(self):
        df = pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123"],
                "status": [
                    "404 Not Found",
                    "500 Internal Server Error",
                    "403 Forbidden",
                ],
            }
        )
        result = calculate_indexer_success_rate(df)
        assert all(result["average_status"] == 0.0)

    def test_calculate_indexer_success_rate_empty_df(self):
        df = pd.DataFrame(columns=["indexer", "status"])
        result = calculate_indexer_success_rate(df)
        assert result.empty

    def test_calculate_indexer_success_rate_case_sensitivity(self):
        df = pd.DataFrame(
            {
                "indexer": ["0xABC", "0xABC", "0xABC"],
                "status": ["200 OK", "200 ok", "200 Ok"],
            }
        )
        result = calculate_indexer_success_rate(df)
        assert result.loc[0, "average_status"] == 1 / 3

    def test_calculate_indexer_success_rate_sorting(self):
        df = pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123", "0x789"],
                "status": [
                    "200 OK",
                    "200 OK",
                    "404 Not Found",
                    "Unavailable(MissingBlock)",
                ],
            }
        )
        result = calculate_indexer_success_rate(df)
        assert list(result["indexer"]) == ["0x123", "0x789", "0xABC", "0xXYZ"]

    def test_calculate_indexer_success_rate_large_dataset(self):
        np.random.seed(42)
        large_df = pd.DataFrame(
            {
                "indexer": np.random.choice(
                    ["0xABC", "0xXYZ", "0x123", "0x789", "0x456"], 100_000
                ),
                "status": np.random.choice(
                    [
                        "200 OK",
                        "Unavailable(MissingBlock)",
                        "404 Not Found",
                        "500 Internal Server Error",
                    ],
                    100_000,
                ),
            }
        )
        result = calculate_indexer_success_rate(large_df)
        assert len(result) == 5
        assert all(0 <= rate <= 1 for rate in result["average_status"])

    def test_calculate_indexer_success_rate_original_df_unchanged(self, sample_df):
        original_df = sample_df.copy()
        _ = calculate_indexer_success_rate(sample_df)
        pd.testing.assert_frame_equal(original_df, sample_df)


class TestCalculateIndexerUptime:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "indexer": ["A", "A", "A", "A", "B", "B", "C"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 5),
                    datetime(2024, 1, 1, 12, 7),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 3),
                    datetime(2024, 1, 1, 12, 0),
                ],
                "status": [
                    "200 OK",
                    "200 OK",
                    "Error",
                    "200 OK",
                    "200 OK",
                    "Unavailable(MissingBlock)",
                    "200 OK",
                ],
            }
        )

    def test_calculate_indexer_uptime_base_case(self, sample_df):
        # Test the basic functionality of the function
        result = calculate_indexer_uptime(sample_df)

        # Check if the result has the expected columns
        expected_columns = [
            "indexer",
            "observed_duration_restricted",
            "uptime_duration_restricted",
            "observed_duration_full",
            "uptime_duration_full",
            "% up_y",
            "% up_x",
        ]
        result_columns = set(result.columns)
        expected_columns_set = set(expected_columns)

        missing_columns_not_in_result = expected_columns_set - result_columns
        unexpected_columns_in_result = result_columns - expected_columns_set

        assert not missing_columns_not_in_result and not unexpected_columns_in_result

        # Check if all indexers are present in the result
        assert set(result["indexer"]) == set(sample_df["indexer"])

        # Check that all percentages are either between 0 and 100, or nan's (where there was only 1 query)
        assert all(
            (0 <= percent <= 100) or np.isnan(percent) for percent in result["% up_x"]
        )
        assert all(
            (0 <= percent <= 100) or np.isnan(percent) for percent in result["% up_y"]
        )

    def test_calculate_indexer_uptime_all_up(self):
        # Test with all indexers being up
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                ],
                "status": [
                    "200 OK",
                    "Unavailable(MissingBlock)",
                    "200 OK",
                    "Unavailable(MissingBlock)",
                ],
            }
        )
        result = calculate_indexer_uptime(df)

        # Confirm all percentages are 100%
        assert all(result["% up_x"] == 100)
        assert all(result["% up_y"] == 100)

    def test_calculate_indexer_uptime_all_down(self):
        # Test with all indexers being down
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 2),
                ],
                "status": ["Error", "Bad", "Bad 504", "Error"],
            }
        )
        result = calculate_indexer_uptime(df)

        # Confirm all percentages are 0%
        assert all(result["% up_x"] == 0)
        assert all(result["% up_y"] == 0)

    def test_calculate_indexer_uptime_threshold(self):
        # Test the effect of the threshold parameter
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "A"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 5),
                    datetime(2024, 1, 1, 12, 10),
                ],
                "status": ["200 OK", "200 OK", "200 OK"],
            }
        )

        # Test with default threshold (120 seconds)
        result_default = calculate_indexer_uptime(df)

        # Test with a lower threshold (60 seconds)
        result_low_threshold = calculate_indexer_uptime(df, threshold_seconds=60)

        # The restricted uptime should be lower with the lower threshold
        assert (
            result_low_threshold["uptime_duration_restricted"].iloc[0]
            == result_default["uptime_duration_restricted"].iloc[0] / 2
        )

    def test_calculate_indexer_uptime_empty_df(self):
        # Test with an empty DataFrame
        df = pd.DataFrame(columns=["indexer", "timestamp", "status"])
        result = calculate_indexer_uptime(df)

        # The result should be an empty DataFrame with the expected columns
        assert result.empty
        expected_columns = [
            "indexer",
            "observed_duration_restricted",
            "uptime_duration_restricted",
            "% up_x",
            "observed_duration_full",
            "uptime_duration_full",
            "% up_y",
        ]
        assert all(col in result.columns for col in expected_columns)

    def test_calculate_indexer_uptime_single_entry_for_indexers(self):
        # Test with a DataFrame containing only one entry
        df = pd.DataFrame(
            {
                "indexer": ["A", "B"],
                "timestamp": [
                    datetime(2023, 1, 1, 12, 0),
                    datetime(2023, 1, 1, 12, 0),
                ],
                "status": ["200 OK", "BAD"],
            }
        )
        result = calculate_indexer_uptime(df)

        # Check if the result contains two rows
        assert len(result) == 2

        # All uptime's should be nan's
        assert np.isnan(result["% up_x"].iloc[0])
        assert np.isnan(result["% up_y"].iloc[0])
        assert np.isnan(result["% up_x"].iloc[1])
        assert np.isnan(result["% up_y"].iloc[1])

    def test_calculate_indexer_uptime_sorting(self):
        # Test if the result is sorted by '% up' in descending order
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "C", "C"],
                "timestamp": [datetime(2024, 1, 1, 12, i) for i in range(6)],
                "status": ["200 OK", "Error", "200 OK", "200 OK", "200 OK", "200 OK"],
            }
        )
        result = calculate_indexer_uptime(df)

        # Check if the '% up' column is sorted in descending order
        assert list(result["% up_x"]) == sorted(result["% up_x"], reverse=True)

    def test_calculate_indexer_uptime_rounding(self):
        # Test if the percentages are rounded to 3 decimal places
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "A"],
                "timestamp": [
                    datetime(2024, 1, 1, 12, 0),
                    datetime(2024, 1, 1, 12, 1),
                    datetime(2024, 1, 1, 12, 3),
                ],
                "status": ["200 OK", "Error", "200 OK"],
            }
        )
        result = calculate_indexer_uptime(df)

        # Check if the percentages are rounded to 3 decimal places
        assert all(round(percent, 3) == percent for percent in result["% up_x"])
        assert all(round(percent, 3) == percent for percent in result["% up_y"])


class TestCalculateStakeToFees:
    @pytest.fixture
    def sample_stake_query_pandas(self):
        return pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "stake_to_fees": [1.0, 2.0, 3.0, 4.0, 5.0],
                "other_column": [10, 20, 30, 40, 50],
            }
        )

    def test_calculate_indexer_stake_to_fees_base_case(self, sample_stake_query_pandas):
        # Calculate result
        result = calculate_indexer_stake_to_fees(sample_stake_query_pandas)

        # Check the result has the correct columns
        assert set(result.columns) == {
            "indexer",
            "stake_to_fees",
            "stake_to_fees_iqr_deviation",
        }

        # Check that 'stake_to_fees' column is unchanged
        pd.testing.assert_series_equal(
            result["stake_to_fees"], sample_stake_query_pandas["stake_to_fees"]
        )

        # Check that 'stake_to_fees_iqr_deviation' is calculated correctly
        median = 3.0
        q1 = 2.0
        q3 = 4.0
        iqr = q3 - q1
        expected_deviations = (
            sample_stake_query_pandas["stake_to_fees"] - median
        ) / iqr
        pd.testing.assert_series_equal(
            result["stake_to_fees_iqr_deviation"],
            expected_deviations,
            check_names=False,
        )

    def test_calculate_indexer_stake_to_fees_empty_df(self):
        # Create empty df
        empty_df = pd.DataFrame(columns=["indexer", "stake_to_fees"])

        # Calculate result
        result = calculate_indexer_stake_to_fees(empty_df)

        assert result.empty
        assert set(result.columns) == {
            "indexer",
            "stake_to_fees",
            "stake_to_fees_iqr_deviation",
        }

    def test_calculate_indexer_stake_to_fees_single_row(self):
        single_row_df = pd.DataFrame({"indexer": ["A"], "stake_to_fees": [1.0]})
        result = calculate_indexer_stake_to_fees(single_row_df)

        # Result should be nan because IQR in this case is 0 and /0 is nan.
        assert len(result) == 1
        assert pd.isna(result["stake_to_fees_iqr_deviation"].iloc[0])

    def test_calculate_indexer_stake_to_fees_with_nan_values(self):
        df_with_nan = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "stake_to_fees": [1.0, np.nan, 3.0, np.nan, 5.0],
            }
        )
        result = calculate_indexer_stake_to_fees(df_with_nan)

        # Check that NaN values are handled correctly
        assert result["stake_to_fees_iqr_deviation"].isna().sum() == 2

    def test_calculate_indexer_stake_to_fees_constant_values(self):
        constant_df = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D", "E"],
                "stake_to_fees": [3.0, 3.0, 3.0, 3.0, 3.0],
            }
        )
        result = calculate_indexer_stake_to_fees(constant_df)

        # All deviations should be NaN when all values are the same (IQR = 0)
        assert result["stake_to_fees_iqr_deviation"].isna().all()

    def test_calculate_indexer_stake_to_fees_extreme_values(self):
        extreme_df = pd.DataFrame(
            {"indexer": ["A", "B", "C"], "stake_to_fees": [1e9, 1e-9, 1e18]}
        )
        result = calculate_indexer_stake_to_fees(extreme_df)

        # Check that the function doesn't crash with extreme values
        assert len(result) == 3
        assert not result["stake_to_fees_iqr_deviation"].isna().any()

    def test_calculate_indexer_stake_to_fees_preserves_input(
        self, sample_stake_query_pandas
    ):
        original = sample_stake_query_pandas.copy()
        calculate_indexer_stake_to_fees(sample_stake_query_pandas)

        # Check that the input DataFrame is unchanged
        pd.testing.assert_frame_equal(sample_stake_query_pandas, original)


class TestAggregateIndexerInfo:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "C", "C", "C"],
                "org": ["X", "X", "Y", "Z", "W", "W", "W"],
                "destination_loc": [
                    "10.1,22",
                    "13.123445,25.123445",
                    "35,44",
                    "31,41",
                    "55,65",
                    "45,60",
                    "50,60",
                ],
            }
        )

    def test_aggregate_indexer_info_base_case(self, sample_df):
        result = aggregate_indexer_info(sample_df)
        expected_org = ["X", "Y", "W"]
        expected_locations = ["20,20", "40,40", "40,60"]
        assert list(result["org"]) == expected_org
        assert list(result["destination_loc"]) == expected_locations
        assert list(result["indexer"]) == ["A", "B", "C"]

    def test_aggregate_indexer_info_empty_df(self):
        df = pd.DataFrame(columns=["indexer", "org", "destination_loc"])
        result = aggregate_indexer_info(df)
        assert result.empty
        assert list(result.columns) == ["indexer", "org", "destination_loc"]

    def test_aggregate_indexer_info_with_nans(self):
        df = pd.DataFrame(
            {
                "indexer": ["A", "A", "B", "B", "B"],
                "org": [np.nan, "X", "Y", np.nan, np.nan],
                "destination_loc": ["10,20", np.nan, np.nan, np.nan, np.nan],
            }
        )
        result = aggregate_indexer_info(df)
        expected_org = ["X", "Y"]
        expected_locations = ["0,20", np.nan]
        assert list(result["org"]) == expected_org
        assert list(result["destination_loc"]) == expected_locations


class TestMergeAndPrepareDataframes:
    @pytest.fixture
    def indexer_uptime(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x123"],
                "uptime": [99.5, 98.7, 97.0],
                "observed_duration_full": [100, 200, 300],
                "uptime_duration_full": [99, 197, 291],
            }
        )

    @pytest.fixture
    def indexer_rankings(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x789"],
                "ranking": [1, 2, 4],
                "% up_y": [95, 96, 97],
            }
        )

    @pytest.fixture
    def agg_df(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0x456"],
                "Coefficient": [0.5, 0.3, np.nan],
                "Standard Error": [0.05, 0.03, 0.01],
                "p-value": [0.01, 0.02, np.nan],
            }
        )

    @pytest.fixture
    def indexer_success_rate(self):
        return pd.DataFrame(
            {"indexer": ["0xABC", "0xXYZ", "0xDEF"], "success_rate": [90, 85, 80]}
        )

    @pytest.fixture
    def stake_to_fees(self):
        return pd.DataFrame(
            {
                "indexer": ["0xABC", "0xXYZ", "0xGHI"],
                "stake_fees_ratio": [100, 200, 300],
            }
        )

    def test_merge_base_case(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
    ):
        # Compute result
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
        )

        # Test that all indexers are present
        assert set(result["indexer"]) == {"0x123", "0xABC", "0xXYZ"}

        # Ensure existing_dips_agreements column is as expected
        assert "existing_dips_agreements" in result.columns
        assert all(result["existing_dips_agreements"] == 0)

        # Ensure avg_sync_duration column is as expected
        assert "avg_sync_duration" in result.columns
        assert all(pd.isna(result["avg_sync_duration"]))

        # Ensure indexing_agreement_acceptance_latency column is as expected
        assert "indexing_agreement_acceptance_latency" in result.columns
        assert all(pd.isna(result["indexing_agreement_acceptance_latency"]))

        # Columns correctly dropped
        assert "% up_y" not in result.columns
        assert "observed_duration_full" not in result.columns
        assert "uptime_duration_full" not in result.columns

    def test_merge__missing_indexer(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
    ):
        # Remove an indexer from one DataFrame to simulate missing data
        indexer_uptime.drop(
            indexer_uptime.index[indexer_uptime["indexer"] == "0xABC"], inplace=True
        )
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
        )
        # '0xABC' should not be present as it was removed from `indexer_uptime`
        assert "0xABC" not in result["indexer"].values

    def test_merge_no_common_indexers(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
    ):
        # Create a completely new set of indexers across the dataframes
        indexer_uptime["indexer"] = ["0xAAA", "0xBBB", "0xCCC"]

        # Compute the result
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
        )

        # Check that the result is not empty
        assert not result.empty

        # Check that all rows from indexer_uptime are present
        assert len(result) == len(indexer_uptime)
        assert set(result["indexer"]) == set(indexer_uptime["indexer"])

        # Check that columns from other DataFrames are present but contain only NaN values
        for col in [
            "ranking",
            "Coefficient",
            "Standard Error",
            "p-value",
            "success_rate",
            "stake_fees_ratio",
        ]:
            assert col in result.columns
            assert result[col].isna().all()

    def test_merge_additional_columns(
        self,
        indexer_uptime,
        indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
    ):
        # Add new columns to multiple input dataframes
        indexer_uptime["new_col_1"] = np.random.randn(len(indexer_uptime))
        indexer_rankings["new_col_2"] = np.random.randn(len(indexer_rankings))
        agg_df["new_col_3"] = np.random.randn(len(agg_df))

        # Compute result
        result = merge_and_prepare_dataframes(
            indexer_uptime,
            indexer_rankings,
            agg_df,
            indexer_success_rate,
            stake_to_fees,
        )

        # Check that all expected columns are present
        expected_columns = {
            "indexer",
            "uptime",
            "ranking",
            "Coefficient",
            "Standard Error",
            "p-value",
            "success_rate",
            "stake_fees_ratio",
            "existing_dips_agreements",
            "avg_sync_duration",
            "indexing_agreement_acceptance_latency",
        }
        assert all(col in result.columns for col in expected_columns)

        # Check that new columns are present too
        new_expected_columns = {"new_col_1", "new_col_2", "new_col_3"}
        assert all(col in result.columns for col in new_expected_columns)


class TestNormalizeMetrics:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "Latency Coefficient + Error Confidence Interval": [
                    -5,
                    0,
                    5,
                    10,
                    12.121212,
                ],
                "% up_x": [0, 10, 50, 75.7575, 99.9],
                "existing_dips_agreements": [0, 100, 31, 35, 50],
                "stake_to_fees_iqr_deviation": [-5.15, 0, 1.125, 3, 120],
                "average_status": [0, 1, 50, 75.7575, 99.9],
                "avg_sync_duration": [10, 200, 300, 400.457, 1000],
                "indexing_agreement_acceptance_latency": [0, 0.5, 2, 12, 24],  # hours
                "other_column": ["A", 1, "B", 12.12, np.nan],
            }
        )

    def test_normalize_metrics_full_run_base_case(self, sample_df):
        # Compute the result
        result = normalize_metrics(sample_df)

        # Check all expected columns are present.
        expected_columns = [
            # Original columns
            "Latency Coefficient + Error Confidence Interval",
            "% up_x",
            "existing_dips_agreements",
            "stake_to_fees_iqr_deviation",
            "average_status",
            "avg_sync_duration",
            "indexing_agreement_acceptance_latency",
            "other_column",
            # New columns
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_existing_dips_agreements",
            "norm_stake_to_fees_iqr_deviation",
            "norm_success_rate",
            "norm_avg_sync_duration",
            "norm_indexing_agreement_acceptance_latency",
        ]
        for col in expected_columns:
            assert col in result.columns

        # Check all normalized values are between 0 and 1
        normalized_columns = [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_existing_dips_agreements",
            "norm_stake_to_fees_iqr_deviation",
            "norm_success_rate",
            "norm_avg_sync_duration",
            "norm_indexing_agreement_acceptance_latency",
        ]
        for col in normalized_columns:
            assert result[col].between(0, 1).all()

    def test_normalize_generic(self):
        # Test the normalize_generic function
        series = pd.Series([-1000, 0, 345.234, 4, 5000])
        result = normalize_generic(series)
        assert result.min() == 0
        assert result.max() == 1
        assert len(result) == len(series)

    def test_normalize_uptime_and_success_rate(self):
        # Test the normalize_uptime_and_success_rate function
        series = pd.Series([0, 12.121212, 98, 99, 100])
        result = normalize_uptime_and_success_rate(series)
        assert result.max() == 1
        assert result.min() == 0
        assert len(result) == len(series)

    def test_normalize_indexing_agreement_acceptance_latency(self):
        # Test with a pandas Series input
        latencies = pd.Series([0, 1, 2, 12, 24])
        results = normalize_indexing_agreement_acceptance_latency(latencies)

        assert len(results) == 5
        assert all(0 <= r <= 1 for r in results)
        # Test with a single value
        single_result = normalize_indexing_agreement_acceptance_latency(pd.Series([60]))
        assert 0 <= single_result.iloc[0] <= 1

        # Test that lower latencies result in higher normalized values
        assert results.iloc[0] > results.iloc[-1]

        # Test with all same values
        same_values = normalize_indexing_agreement_acceptance_latency(
            pd.Series([60, 60, 60])
        )
        assert all(r == 0 for r in same_values)

    def test_empty_dataframe(self, sample_df):
        # Test with an empty DataFrame
        empty_df = pd.DataFrame(columns=sample_df.columns)
        result = normalize_metrics(empty_df)
        assert result.empty
        expected_columns = list(empty_df.columns) + [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_existing_dips_agreements",
            "norm_stake_to_fees_iqr_deviation",
            "norm_success_rate",
            "norm_avg_sync_duration",
            "norm_indexing_agreement_acceptance_latency",
        ]
        assert set(result.columns) == set(expected_columns)

    def test_all_same_values(self, sample_df):
        # Test with all values being the same
        sample_df.loc[:, :] = 1000

        # Call normalize_metrics function
        result = normalize_metrics(sample_df)

        norm_columns = [
            "norm_lat_lin_reg_coefficient",
            "norm_uptime_score",
            "norm_existing_dips_agreements",
            "norm_stake_to_fees_iqr_deviation",
            "norm_success_rate",
            "norm_avg_sync_duration",
            "norm_indexing_agreement_acceptance_latency",
        ]

        # Check for normalization results where input values are the same
        for column in norm_columns:
            if column in [
                "norm_stake_to_fees_iqr_deviation",
            ]:
                assert (
                    result[column] == 0
                ).all(), f"Column {column} is not 0 for identical input values"

            elif column in [
                "norm_existing_dips_agreements",
                "norm_avg_sync_duration",
                "norm_lat_lin_reg_coefficient",
            ]:
                assert (
                    result[column] == 1
                ).all(), f"Column {column} is not 0 for identical input values"

            # For the logistic normalization (indexing agreement acceptance latency)
            elif column == "norm_indexing_agreement_acceptance_latency":
                assert (
                    result[column] == 0
                ).all(), "(result[column] == 0).all() not true"

    def test_negative_values(self, sample_df):
        # Test with negative values
        sample_df.loc[0] = [-1, -1, -1, -1, -1, -1, -1, -1]
        sample_df.loc[1] = [-100, -50, -75, -25, -10, -5, -1, -1]
        sample_df.loc[2] = [0, 0, 0, 0, 0, 0, 0, 0]
        sample_df.loc[3] = [1, 1, 1, 1, 1, 1, 1, 1]
        sample_df.loc[4] = [-1000, 0, 1000, -500, 500, -250, 250, 0]

        # Compute result
        result = normalize_metrics(sample_df)

        # Check negative numbers don't create np.nan's in the result
        assert not result.isnull().any().any()

        norm_columns = result.columns[result.columns.str.startswith("norm_")]
        for col in norm_columns:
            min_val = result[col].min()
            max_val = result[col].max()

            # Make sure results are normalized correctly.
            assert min_val >= 0 and max_val <= 1
            assert not result[col].isin([np.inf, -np.inf]).any()

    def test_all_negative_values(self, sample_df):
        # Test with all negative values
        sample_df.loc[:, :] = -1
        result = normalize_metrics(sample_df)

        # Check that the function handles all negative values as expected
        norm_columns = [col for col in result.columns if col.startswith("norm_")]

        for col in norm_columns:
            assert (
                result[col].between(0, 1).all()
            ), f"Column {col} contains values outside [0, 1] range"

        assert (
            not result[norm_columns].isnull().any().any()
        ), "Result contains unexpected NaN values"

    def test_nan_values(self, sample_df):
        # Test with NaN values
        sample_df.loc[0] = [np.nan] * len(sample_df.columns)
        result = normalize_metrics(sample_df)

        # Check that NaN values are not present in other rows of normalized columns
        assert (
            not result.iloc[1:, result.columns.str.startswith("norm_")]
            .isnull()
            .any()
            .any()
        )

    def test_extreme_values_in_latency(self):
        # Test with extreme values
        latencies = pd.Series([0, 5, np.inf, -100, 7])
        results = normalize_indexing_agreement_acceptance_latency(latencies)

        assert len(results) == 5, "len(results) != 5"
        assert all(0 <= r <= 1 for r in results), "Values not all between 0 and 1"

        # Check that 0 latency results in the highest score
        assert results[0] == results.max(), "0 latency didn't give the highest score"

        # Check that infinite latency results in the lowest score
        assert results[2] == results.min(), "inf latency didn't give the lowest score"

        # Check that negative latency is treated as 0 (highest score)
        assert results[3] == results[0], "negative latency didn't give the lowest score"

        # Check that other values are ordered correctly
        assert results[0] > results[1] > results[4], "values not ordered correctly"


class TestCalculateWeightedScore:
    @pytest.fixture
    def sample_weights(self):
        return {"metric1": 0.5, "metric2": 0.3, "metric3": 0.2}

    def test_basic_calculation(self, sample_weights):
        # Test the function with all metrics present
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        result = calculate_weighted_score(row, sample_weights)
        expected = (0.8 * 0.5 + 0.6 * 0.3 + 0.4 * 0.2) / 1.0
        assert np.isclose(result, expected)

    def test_missing_metric(self, sample_weights):
        # Test the function when one metric is missing (NaN)
        row = pd.Series(
            {"norm_metric1": 0.8, "norm_metric2": np.nan, "norm_metric3": 0.4}
        )
        result = calculate_weighted_score(row, sample_weights)
        expected = ((0.8 * 0.5) + (0 * 0.3) + (0.4 * 0.2)) / (0.5 + 0.2)
        assert np.isclose(result, expected)

    def test_all_metrics_missing(self, sample_weights):
        # Test the function when all metrics are missing (NaN)
        row = pd.Series(
            {"norm_metric1": np.nan, "norm_metric2": np.nan, "norm_metric3": np.nan}
        )
        with pytest.raises(ValueError, match="Total weight cannot be 0."):
            calculate_weighted_score(row, sample_weights)

    def test_zero_weights(self):
        # Test the function when all weights are zero
        weights = {"metric1": 0, "metric2": 0, "metric3": 0}
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        with pytest.raises(ValueError, match="Total weight cannot be 0."):
            calculate_weighted_score(row, weights)

    def test_partial_weights(self):
        # Test the function when some weights are zero
        weights = {"metric1": 0.5, "metric2": 0, "metric3": 0.5}
        row = pd.Series({"norm_metric1": 0.8, "norm_metric2": 0.6, "norm_metric3": 0.4})
        result = calculate_weighted_score(row, weights)
        expected = (0.8 * 0.5 + 0.4 * 0.5) / 1.0
        assert np.isclose(result, expected)

    def test_extra_metrics_in_row(self, sample_weights):
        # Test the function when the row contains extra metrics not in weights
        row = pd.Series(
            {
                "norm_metric1": 0.8,
                "norm_metric2": 0.6,
                "norm_metric3": 0.4,
                "norm_metric4": 1.0,
                "other_column": "value",
            }
        )
        result = calculate_weighted_score(row, sample_weights)
        expected = (0.8 * 0.5 + 0.6 * 0.3 + 0.4 * 0.2) / 1.0
        assert np.isclose(result, expected)

    @pytest.mark.parametrize(
        "row_data, weights, expected",
        [
            (
                {"norm_metric1": 1.0, "norm_metric2": 1.0},
                {"metric1": 1, "metric2": 1},
                1.0,
            ),
            (
                {"norm_metric1": 0.0, "norm_metric2": 0.0},
                {"metric1": 1, "metric2": 1},
                0.0,
            ),
            (
                {"norm_metric1": 0.5, "norm_metric2": 0.5},
                {"metric1": 1, "metric2": 1},
                0.5,
            ),
        ],
    )
    def test_edge_cases(self, row_data, weights, expected):
        # Test various edge cases
        row = pd.Series(row_data)
        result = calculate_weighted_score(row, weights)
        assert np.isclose(result, expected)
