import numpy as np
import pandas as pd
import pytest

from iisa.iisa_functions import (
    _normalize_generic,
    _normalize_indexing_agreement_acceptance_latency,
    _normalize_uptime_and_success_rate,
    calculate_weighted_score,
    normalize_metrics,
)


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
        result = _normalize_generic(series)
        assert result.min() == 0
        assert result.max() == 1
        assert len(result) == len(series)

    def test_normalize_uptime_and_success_rate(self):
        # Test the normalize_uptime_and_success_rate function
        series = pd.Series([0, 12.121212, 98, 99, 100])
        result = _normalize_uptime_and_success_rate(series)
        assert result.max() == 1
        assert result.min() == 0
        assert len(result) == len(series)

    def test_normalize_indexing_agreement_acceptance_latency(self):
        # Test with a pandas Series input
        latencies = pd.Series([0, 1, 2, 12, 24])
        results = _normalize_indexing_agreement_acceptance_latency(latencies)

        assert len(results) == 5
        assert all(0 <= r <= 1 for r in results)
        # Test with a single value
        single_result = _normalize_indexing_agreement_acceptance_latency(
            pd.Series([60])
        )
        assert 0 <= single_result.iloc[0] <= 1

        # Test that lower latencies result in higher normalized values
        assert results.iloc[0] > results.iloc[-1]

        # Test with all same values
        same_values = _normalize_indexing_agreement_acceptance_latency(
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
        results = _normalize_indexing_agreement_acceptance_latency(latencies)

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
