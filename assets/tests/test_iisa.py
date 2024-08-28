import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pytest
from iisa.iisa import (
    initialize_data_manager,
    process_subgraph,
    DataManager,
)

@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "indexer": ["A", "B", "C"],
            "deployment_hash": ["hash1", "hash2", "hash3"],
            "score": [0.8, 0.6, 0.7],
        }
    )

@pytest.fixture
def mock_url_to_ip():
    return "0.0.0.0"  # Return a dummy IP for any URL

@pytest.fixture
def mock_bigquery_provider():
    mock = MagicMock()
    return mock


class TestInitializeDataManager:
    """
    This class verifies the initialize_data_manager function creates/returns
    a DataManager instance with the correct configuration and handles errors.
    """
    def test_initialize_data_manager(self, mock_bigquery_provider, mock_url_to_ip):
        """
        This test verifies:
        1. The function returns a DataManager instance.
        2. The BigQueryProvider is created with the correct parameters.
        3. The returned DataManager uses the created BigQueryProvider.
        4. The DataManager's attributes are correctly set and populated.
        """
        def mock_perform_linear_regression(*args):
            """
            Creates and returns mock data for linear regression results.

            This function simulates the output of a linear regression process,
            providing mock data for:
            1. filtered_bigquery_data: A DataFrame with query and indexer information.
            2. indexer_rankings: A DataFrame with indexer rankings and scores.
            """
            filtered_data = pd.DataFrame(
                {
                    "query_id": ["id1", "id2"],
                    "deployment_hash": ["hash1", "hash2"],
                    "indexer": ["indexer1", "indexer2"],
                    "indexer_network": ["net1", "net2"],
                    "fee": [0.1, 0.2],
                    "response_time_ms": [100, 200],
                    "distance_miles": [100, 200],
                    "sampled_query_id_hashed_mod_integer_root": [0, 1],
                }
            )
            indexer_rankings = pd.DataFrame(
                {
                    "indexer": ["indexer1", "indexer2", "indexer3"],
                    "rank": [1, 2, 3],
                    "score": [0.9, 0.8, 0.7],
                }
            )

            # Set the attributes if 'self' is passed as the first argument
            if args and hasattr(args[0], "filtered_bigquery_data"):
                args[0].filtered_bigquery_data = filtered_data
            if args and hasattr(args[0], "indexer_rankings"):
                args[0].indexer_rankings = indexer_rankings

            return filtered_data, indexer_rankings

        def mock_fetch_bigquery_data(self):
            """
            Creates and returns mock data simulating BigQuery fetch results.

            This function generates mock data for:
            1. bigquery_data: A DataFrame with various indexer and query metrics.
            2. filtered_bigquery_data: A subset of bigquery_data (first two rows).
            3. indexer_rankings: A DataFrame with indexer rankings and scores.
            """
            mock_data = pd.DataFrame(
                {
                    "query_id": ["id1", "id2", "id3"],
                    "deployment_hash": ["hash1", "hash2", "hash3"],
                    "indexer": ["indexer1", "indexer2", "indexer3"],
                    "indexer_network": ["net1", "net2", "net3"],
                    "org": ["org1", "org2", "org3"],
                    "fee": [0.1, 0.2, 0.3],
                    "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
                    "blocks_behind": [1, 2, 3],
                    "response_time_ms": [100, 200, 300],
                    "status": ["200 OK", "200 OK", "200 OK"],
                    "day_partition": ["2024-01-01", "2024-01-02", "2024-01-03"],
                    "subgraph_network": ["network1", "network2", "network3"],
                    "url": ["url1", "url2", "url3"],
                    "origin_loc": ["0,20", "40,40", "60,60"],
                    "destination_loc": ["20,40", "40,60", "60,80"],
                    "loc": ["0,20", "40,40", "60,60"],
                    "distance_miles": [100, 200, 300],
                    "sampled_query_id_hashed_mod_integer_root": [0, 1, 2],
                }
            )
            filtered_mock_data = mock_data.iloc[:2].copy()
            indexer_rankings_mock_data = pd.DataFrame(
                {
                    "indexer": ["indexer1", "indexer2", "indexer3"],
                    "rank": [1, 2, 3],
                    "score": [0.9, 0.8, 0.7],
                }
            )

            # Assign the mock data to the appropriate attributes
            self.bigquery_data = mock_data
            self.filtered_bigquery_data = filtered_mock_data
            self.indexer_rankings = indexer_rankings_mock_data
            return mock_data

        # Apply patches for the test
        with patch("iisa.iisa.BigQueryProvider", mock_bigquery_provider):
            with patch("iisa.iisa_functions.url_to_ip", mock_url_to_ip):
                with patch(
                    "iisa.iisa.derive_timestamps",
                    return_value=(
                        datetime.now(),
                        datetime.now(),
                        "2024-01-01T00:00:00Z",
                        "2024-01-28T23:59:59Z",
                    ),
                ):
                    with patch(
                        "iisa.iisa_functions.perform_linear_regression",
                        side_effect=mock_perform_linear_regression,
                    ):
                        with patch.object(
                            DataManager, "fetch_bigquery_data", mock_fetch_bigquery_data
                        ):
                            result = initialize_data_manager()

        # Verify that the result is an instance of DataManager
        assert isinstance(result, DataManager)

        # Verify BigQueryProvider was called with the correct parameters
        mock_bigquery_provider.assert_called_once_with("graph-mainnet", "US")

        # Verify the DataManager is using the created BigQueryProvider
        assert result.bigquery == mock_bigquery_provider.return_value

        # Verify that the bigquery_data attribute is not None and that bigquery_data contains some rows.
        assert result.bigquery_data is not None
        assert len(result.bigquery_data) > 0

        # Verify that 'day_partition' exists in bigquery_data
        assert "day_partition" in result.bigquery_data.columns

        # Verify that 'destination_loc' exists in bigquery_data and contains string values
        assert "destination_loc" in result.bigquery_data.columns
        assert result.bigquery_data["destination_loc"].dtype == "object"

        # Verify that filtered_bigquery_data is not None
        assert result.filtered_bigquery_data is not None

        # Verify that indexer_rankings is not None
        assert result.indexer_rankings is not None

    @patch("iisa.iisa.BigQueryProvider")
    def test_initialize_data_manager_exception_handling(self, mock_bigquery_provider):
        """
        This test verifies that initialize_data_manager handles exceptions gracefully.
        """
        # Set up the mock to raise an exception when instantiated
        mock_bigquery_provider.return_value.fetch_initial_query_results.side_effect = (
            Exception("Simulated error")
        )

        # Verify the function raises the expected exception
        with pytest.raises(Exception) as exc_info:
            initialize_data_manager()

        # Verify the exception message matches
        assert str(exc_info.value) == "Simulated error"

        # Verify the BigQueryProvider was instantiated exactly once
        assert mock_bigquery_provider.call_count == 1

