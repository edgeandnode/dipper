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


class TestProcessSubgraph:
    """
    This class verifies the process_subgraph function creates a DataProcessor
    instance and returns the expected results for added/cancelled indexers.
    """

    @patch("iisa.iisa.DataProcessor")
    def test_process_subgraph(
        self, mock_data_processor, sample_data, mock_bigquery_provider
    ):
        """
        Test the process_subgraph function creates a DataProcessor instance and returns the expected results.

        Expected results:
        1. processor.added_indexers
        2. processor.cancelled_indexers
        """
        # Set up mock DataProcessor instance
        mock_instance = mock_data_processor.return_value
        mock_instance.added_indexers = [
            ("indexer1", "test_subgraph"),
            ("indexer2", "test_subgraph"),
        ]
        mock_instance.cancelled_indexers = [("indexer3", "test_subgraph")]

        # Define test input parameters
        subgraph_id = "test_subgraph"
        prices = {"indexer1": 10, "indexer2": 20, "indexer3": 15}
        existing_agreements = {
            "indexer1": ["subgraph1"],
            "indexer2": ["subgraph2"],
            "indexer3": ["test_subgraph"],
        }
        pending_agreements = {"indexer4": ["subgraph3"]}
        blacklist = ["blacklisted_indexer"]

        # Apply patch for the test
        with patch(
            "iisa.iisa.BigQueryProvider",
            return_value=mock_bigquery_provider.return_value,
        ):
            # Process the subgraph
            added, cancelled = process_subgraph(
                sample_data,
                subgraph_id,
                prices,
                existing_agreements,
                pending_agreements,
                blacklist,
            )

        # Verify an instance of DataProcessor was created with expected parameters
        mock_data_processor.assert_called_once_with(
            data=sample_data,
            subgraph_id=subgraph_id,
            prices=prices,
            bigquery=mock_bigquery_provider.return_value,
            existing_agreements=existing_agreements,
            pending_agreements=pending_agreements,
            blacklist=blacklist,
        )

        # Verify the function returns the expected added and cancelled indexer pairs
        assert added == [("indexer1", "test_subgraph"), ("indexer2", "test_subgraph")]
        assert cancelled == [("indexer3", "test_subgraph")]

        # Verify pairs are associated with the expected respective subgraphs
        assert all(pair[1] == subgraph_id for pair in added)
        assert all(pair[1] == subgraph_id for pair in cancelled)


class TestDataManager:
    """
    This class contains tests to ensure that the DataManager class
    correctly initializes, fetches data, and provides access to its data.
    """

    @patch("iisa.iisa.BigQueryProvider")
    @patch("iisa.iisa.derive_timestamps")
    def test_data_manager_constructor(
        self, mock_derive_timestamps, mock_bigquery_provider
    ):
        """
        Tests the initialization of the DataManager class to ensure it sets up with
        default values and proper initialization of dependencies.

        This test checks:
        1. That DataManager uses the expected default number of days for data fetching.
        2. That BigQueryProvider is properly instantiated.
        3. That the derive_timestamps function is called with the appropriate parameters, and the
           return values are correctly used to set the start and end dates and timestamps.
        4. That internal data attributes (bigquery_data, indexer_rankings, etc...) are initialized to
           None, to verify the class is in the correct state before further data fetching.
        """
        # Mock the return value of derive_timestamps
        mock_derive_timestamps.return_value = (
            datetime(2024, 1, 1),
            datetime(2024, 1, 28),
            "2024-01-01T00:00:00Z",
            "2024-01-28T23:59:59Z",
        )

        with patch("iisa.iisa.DataManager.fetch_bigquery_data"):
            # Initialize DataManager with mocked BigQueryProvider
            dm = DataManager(bigquery=mock_bigquery_provider.return_value)

        # Verify default values
        assert dm.num_days == 28
        assert dm.bigquery == mock_bigquery_provider.return_value

        # Check date calculations
        assert dm.start_date == datetime(2024, 1, 1)
        assert dm.end_date == datetime(2024, 1, 28)
        assert dm.start_ts == "2024-01-01T00:00:00Z"
        assert dm.end_ts == "2024-01-28T23:59:59Z"

        # Verify initial data attributes
        assert dm.bigquery_data is None
        assert dm.indexer_rankings is None
        assert dm.indexer_success_rate is None
        assert dm.indexer_uptime is None
        assert dm.stake_to_fees is None
        assert dm.filtered_bigquery_data is None

        # Ensure derive_timestamps was called with correct argument
        mock_derive_timestamps.assert_called_once_with(28)

    @patch("iisa.iisa.DataManager.fetch_bigquery_data")
    def test_update_and_fetch_data_method(self, mock_fetch, mock_bigquery_provider):
        """
        This test verifies:
        1. The update_and_fetch_data method updates the start and end dates.
        2. The fetch_bigquery_data method is called after updating dates.
        """
        # Initialize a DataManager instance
        dm = DataManager(bigquery=mock_bigquery_provider.return_value)

        # Set initial variables
        initial_start_date = dm.start_date
        initial_end_date = dm.end_date

        # Reset mock_fetch to clear the call from initialization
        mock_fetch.reset_mock()

        # Call update_and_fetch_data
        with patch(
            "iisa.iisa.derive_timestamps",
            return_value=(
                dm.start_date + timedelta(days=1),
                dm.end_date + timedelta(days=1),
                "",
                "",
            ),
        ):
            dm.update_and_fetch_data()

        # Verify date updates
        assert dm.start_date >= initial_start_date
        assert dm.end_date > initial_end_date

        # Verify fetch_bigquery_data was called
        mock_fetch.assert_called_once()

    def test_get_data(self, mock_bigquery_provider):
        """
        This test verifies:
        1. The get_data method returns the bigquery_data.
        2. The returned data matches the explicitly defined mock data.
        """
        # Define mock data
        mock_data = pd.DataFrame(
            {
                "indexer": ["indexer1", "indexer2", "indexer3"],
                "score": [0.9, 0.8, 0.7],
                "query_count": [100, 200, 300],
            }
        )

        # Mock the fetch_bigquery_data method to avoid actual data fetching
        with patch("iisa.iisa.DataManager.fetch_bigquery_data"):
            # Initialize a DataManager instance
            dm = DataManager(bigquery=mock_bigquery_provider.return_value)

            # Manually set the bigquery_data attribute
            dm.bigquery_data = mock_data

        # Call get_data method
        result = dm.get_data()

        # Verify returned data is the same as the mock data
        pd.testing.assert_frame_equal(result, mock_data)

        # Additional assertions
        assert result.shape == (3, 3)
        assert list(result.columns) == ["indexer", "score", "query_count"]
        assert result["indexer"].tolist() == ["indexer1", "indexer2", "indexer3"]
        assert result["score"].tolist() == [0.9, 0.8, 0.7]
        assert result["query_count"].tolist() == [100, 200, 300]

    def test_get_indexer_rankings(self, mock_bigquery_provider):
        """
        This test verifies:
        1. The get_indexer_rankings method returns the indexer rankings.
        """
        # Initialize a DataManager instance
        with patch("iisa.iisa.DataManager.fetch_bigquery_data"):
            dm = DataManager(bigquery=mock_bigquery_provider.return_value)
        sample_rankings = pd.DataFrame({"indexer": ["A", "B"], "rank": [1, 2]})
        dm.indexer_rankings = sample_rankings

        # Call get_indexer_rankings method
        result = dm.get_indexer_rankings()

        # Verify returned data is the same as the sample data.
        pd.testing.assert_frame_equal(result, sample_rankings)
