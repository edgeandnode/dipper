from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
import numpy as np

from iisa.iisa import DataManager, DataProcessor, GeoipResolver, NetworkProvider
from iisa.time import TimestampStr
from tests.__fixtures__ import network as network_fixture


def process_subgraph(
    data,
    subgraph_id,
    prices,
    existing_agreements,
    pending_agreements,
    blacklist,
    *,
    bigquery_provider,
):
    processor = DataProcessor(
        data=data,
        subgraph_id=subgraph_id,
        prices=prices,
        bigquery=bigquery_provider,
        existing_agreements=existing_agreements,
        pending_agreements=pending_agreements,
        blacklist=blacklist,
    )
    return processor.added_indexers, processor.cancelled_indexers


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
def mock_regression_results():
    filtered_df = pd.DataFrame(
        {
            "indexer": ["indexer1", "indexer2", "indexer3"],
            "coefficient": [0.1, 0.2, 0.3],
            "p_value": [0.01, 0.02, 0.03],
        }
    )
    rankings_df = pd.DataFrame(
        {"indexer": ["indexer1", "indexer2", "indexer3"], "rank": [1, 2, 3]}
    )
    return filtered_df, rankings_df


@pytest.fixture
def mock_combined_query_results():
    return pd.DataFrame(
        {
            "query_id": [
                "855e9b7776ebb2e8-MAN",
                "855e3da797201b9f-FRA",
                "855e42a084ae0a23-ARN",
            ],
            "deployment_hash": ["hash1", "hash2", "hash3"],
            "indexer": ["indexer1", "indexer2", "indexer3"],
            "indexer_network": ["net1", "net2", "net3"],
            "org": ["hetzner", "amazon aws", "google"],
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


@pytest.fixture
def mock_bigquery_provider(mock_combined_query_results):
    mock = MagicMock()
    mock.return_value.fetch_initial_query_results.return_value = pd.DataFrame(
        {
            "deployment_hash": ["hash1", "hash2", "hash3"],
            "indexer": ["indexer1", "indexer2", "indexer3"],
            "num_rows": [1000, 2000, 3000],
        }
    )
    mock.return_value.fetch_combined_query_results.return_value = (
        mock_combined_query_results
    )
    mock.return_value.fetch_initial_stake_to_fees.return_value = pd.DataFrame(
        {
            "indexer": ["indexer1", "indexer2", "indexer3"],
            "stake_to_fees": [1.0, 2.0, 3.0],
        }
    )
    return mock


@pytest.fixture
def mock_network_provider():
    ## Given
    resolver = GeoipResolver()
    provider = NetworkProvider(geoip=resolver)

    # Initialize the network provider with test data
    test_data = network_fixture.load_fixture_data()
    provider.set_snapshot(test_data)

    return provider


class TestInitializeDataManager:
    """
    This class verifies the initialize_data_manager function creates/returns
    a DataManager instance with the correct configuration and handles errors.
    """

    def test_initialize_data_manager(
        self, mock_bigquery_provider, mock_network_provider
    ):
        """
        This test verifies:
        1. The function returns a DataManager instance.
        2. The BigQueryProvider is created with the correct parameters.
        3. The returned DataManager uses the created BigQueryProvider.
        4. The DataManager's attributes are correctly set and populated.
        """

        def mock_perform_latency_linear_regression(*args):
            """
            Creates and returns mock data for latency linear regression results.

            This function simulates the output of a linear regression process,
            providing mock data for:
            1. filtered_data: A DataFrame with query and indexer information.
            2. latency_linear_regression_indexer_rankings: A DataFrame with indexer rankings and scores.
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
            latency_linear_regression_indexer_rankings = pd.DataFrame(
                {
                    "indexer": ["indexer1", "indexer2", "indexer3"],
                    "rank": [1, 2, 3],
                    "score": [0.9, 0.8, 0.7],
                }
            )

            # Set the attributes if 'self' is passed as the first argument
            if args and hasattr(args[0], "filtered_data"):
                args[0].filtered_data = filtered_data
            if args and hasattr(args[0], "latency_linear_regression_indexer_rankings"):
                args[
                    0
                ].latency_linear_regression_indexer_rankings = (
                    latency_linear_regression_indexer_rankings
                )

            return filtered_data, latency_linear_regression_indexer_rankings

        def mock__fetch_and_process_data(
            bigquery_provider,
            network_provider,
            start_date: date,
            start_ts: TimestampStr,
            num_days: int,
            target_rows: int = 20_000_000,
        ):
            """
            Creates and returns mock data simulating BigQuery fetch results.

            This function generates mock data for:
            1. bigquery_data: A DataFrame with various indexer and query metrics.
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
            latency_linear_regression_indexer_rankings_mock_data = pd.DataFrame(
                {
                    "indexer": ["indexer1", "indexer2", "indexer3"],
                    "rank": [1, 2, 3],
                    "score": [0.9, 0.8, 0.7],
                }
            )
            latency_linear_regression_results_df_mock_data = pd.DataFrame(
                {
                    "Variable": ["var1", "var2", "var3"],
                    "Coefficient": [0.1, 0.2, 0.3],
                    "Standard Error": [0.01, 0.02, 0.03],
                    "p-value": [0.001, 0.002, 0.003],
                }
            )

            return (
                mock_data,
                latency_linear_regression_indexer_rankings_mock_data,
                latency_linear_regression_results_df_mock_data,
            )

        # Apply patches for the test
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
                "iisa.iisa_functions.perform_latency_linear_regression",
                side_effect=mock_perform_latency_linear_regression,
            ):
                with patch(
                    "iisa.iisa._fetch_and_process_data",
                    mock__fetch_and_process_data,
                ):
                    result = DataManager(
                        bigquery=mock_bigquery_provider.return_value,
                        network=mock_network_provider,
                    )

        # Verify that the result is an instance of DataManager
        assert isinstance(result, DataManager)

        # Verify that the bigquery_data attribute is not None and that bigquery_data contains some rows.
        assert result.bigquery_data is not None
        assert len(result.bigquery_data) > 0

        # Verify that 'day_partition' exists in bigquery_data
        assert "day_partition" in result.bigquery_data.columns

        # Verify that 'destination_loc' exists in bigquery_data and contains string values
        assert "destination_loc" in result.bigquery_data.columns
        assert result.bigquery_data["destination_loc"].dtype == "object"

        # Verify that latency_linear_regression_indexer_rankings is not None
        assert result.latency_linear_regression_indexer_rankings is not None

        # Verify that latency_linear_regression_results_df is not None
        assert result.latency_linear_regression_results_df is not None

        # Verify the date and timestamp attributes
        assert isinstance(result.start_date, datetime)
        assert isinstance(result.end_date, datetime)
        assert isinstance(result.start_ts, str)
        assert isinstance(result.end_ts, str)

    def test_initialize_data_manager_exception_handling(
        self, mock_bigquery_provider, mock_network_provider
    ):
        """
        This test verifies that initialize_data_manager handles exceptions gracefully.
        """
        # Set up the mock to raise an exception when instantiated
        mock_bigquery_provider.return_value.fetch_initial_query_results.side_effect = (
            Exception("Simulated error")
        )

        # Verify the function raises the expected exception
        with pytest.raises(Exception) as exc_info:
            DataManager(
                bigquery=mock_bigquery_provider.return_value,
                network=mock_network_provider,
            )

            # Verify the exception message matches
        assert str(exc_info.value) == "Simulated error"


class TestProcessSubgraph:
    """
    This class verifies the process_subgraph function creates a DataProcessor
    instance and returns the expected results for added/cancelled indexers.
    """

    @pytest.mark.skip(reason="Flaky test: high dependency on internal details")
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
            weights=None,
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

    @patch("iisa.iisa.derive_timestamps")
    def test_data_manager_constructor(
        self, mock_derive_timestamps, mock_bigquery_provider, mock_network_provider
    ):
        """
        Tests the initialization of the DataManager class to ensure it sets up with
        default values and proper initialization of dependencies.

        This test checks:
        1. That DataManager uses the expected default number of days for data fetching.
        2. That BigQueryProvider and NetworkProvider are properly instantiated.
        3. That the derive_timestamps function is called with the appropriate parameters, and the
           return values are correctly used to set the start and end dates and timestamps.
        4. That internal data attributes are initialized correctly.
        """
        # Mock the return value of derive_timestamps
        mock_derive_timestamps.return_value = (
            datetime(2024, 1, 1),
            datetime(2024, 1, 28),
            "2024-01-01T00:00:00Z",
            "2024-01-28T23:59:59Z",
        )

        # Mock data for _fetch_and_process_data
        mock_bigquery_data = pd.DataFrame({"mock": [1, 2, 3]})
        mock_indexer_rankings = pd.DataFrame({"mock": [4, 5, 6]})
        mock_results_df = pd.DataFrame({"mock": [7, 8, 9]})

        with patch(
            "iisa.iisa._fetch_and_process_data",
            return_value=(mock_bigquery_data, mock_indexer_rankings, mock_results_df),
        ):
            # Initialize DataManager
            dm = DataManager(
                bigquery=mock_bigquery_provider.return_value,
                network=mock_network_provider,
            )

        # Verify default values
        assert dm.num_days == 28

        # Check date calculations
        assert dm.start_date == datetime(2024, 1, 1)
        assert dm.end_date == datetime(2024, 1, 28)
        assert dm.start_ts == "2024-01-01T00:00:00Z"
        assert dm.end_ts == "2024-01-28T23:59:59Z"

        # Verify initial data attributes
        assert dm.bigquery_data is mock_bigquery_data
        assert dm.latency_linear_regression_indexer_rankings is mock_indexer_rankings
        assert dm.latency_linear_regression_results_df is mock_results_df

        # Verify that BigQueryProvider and NetworkProvider are correctly set
        assert dm._bq is mock_bigquery_provider.return_value
        assert dm._network is mock_network_provider

        # Ensure derive_timestamps was called with correct argument
        mock_derive_timestamps.assert_called_once_with(28, None)

    @patch("iisa.iisa._fetch_and_process_data", return_value=(None, None, None))
    def test_fetch_and_update(
        self, mock_fetch, mock_bigquery_provider, mock_network_provider
    ):
        """
        This test verifies:
        1. The update_and_fetch_data method updates the start and end dates.
        2. The fetch_data method is called after updating dates.
        """
        # Initialize a DataManager instance
        dm = DataManager(
            bigquery=mock_bigquery_provider.return_value, network=mock_network_provider
        )

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
            dm.fetch_data_and_update()

        # Verify date updates
        assert dm.start_date >= initial_start_date
        assert dm.end_date > initial_end_date

        # Verify fetch_data was called
        mock_fetch.assert_called_once()

    def test_get_data(self, mock_bigquery_provider, mock_network_provider):
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

        # Mock the fetch_data method to avoid actual data fetching
        with patch(
            "iisa.iisa._fetch_and_process_data", return_value=(mock_data, None, None)
        ):
            # Initialize a DataManager instance
            dm = DataManager(
                bigquery=mock_bigquery_provider.return_value,
                network=mock_network_provider,
            )

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

    def test_get_latency_linear_regression_indexer_rankings(
        self, mock_bigquery_provider, mock_network_provider
    ):
        """
        This test verifies:
        1. The get_latency_linear_regression_indexer_rankings method returns the indexer rankings.
        """
        # Create sample data for the mock return value
        sample_bigquery_data = pd.DataFrame({"column1": [1, 2, 3]})
        sample_indexer_rankings = pd.DataFrame(
            {"indexer": ["A", "B", "C"], "rank": [1, 2, 3]}
        )
        sample_results_df = pd.DataFrame({"result": [4, 5, 6]})

        # Initialize a DataManager instance
        with patch(
            "iisa.iisa._fetch_and_process_data",
            return_value=(
                sample_bigquery_data,
                sample_indexer_rankings,
                sample_results_df,
            ),
        ):
            dm = DataManager(
                bigquery=mock_bigquery_provider,
                network=mock_network_provider,
            )

        # Call get_latency_linear_regression_indexer_rankings method
        result = dm.get_latency_linear_regression_indexer_rankings()

        # Verify returned data is the same as the sample data.
        pd.testing.assert_frame_equal(result, sample_indexer_rankings)


class TestDataProcessor:
    """
    This class contains a range of unit tests to ensure that the DataProcessor class functions as intended.
    """

    @pytest.fixture
    def sample_data(self):
        """
        Fixture to create a sample DataFrame for testing.
        """
        return pd.DataFrame(
            {
                "indexer": ["A", "B", "C"],
                "deployment_hash": ["hash1", "hash2", "hash3"],
                "score": [0.8, 0.6, 0.7],
                "destination_loc": ["loc1", "loc2", "loc3"],
                "org": ["org1", "org2", "org3"],
                "existing_dips_agreements": [1, 2, 3],
                "weighted_score": [0.9, 0.7, 0.8],
                "lat_lin_reg_coefficient": [0.1, 0.2, 0.3],
                "uptime_score": [0.9, 0.8, 0.7],
                "stake_to_fees_iqr_deviation": [0.1, 0.2, 0.3],
                "success_rate": [0.95, 0.90, 0.85],
                "avg_sync_duration": [100, 200, 300],
                "indexing_agreement_acceptance_latency": [10, 20, 30],
            }
        )

    @pytest.fixture
    def mock_bigquery_provider(self):
        return MagicMock()

    @pytest.mark.skip(reason="Flaky test: high dependency on internal details")
    def test_data_processor_constructor(self, sample_data, mock_bigquery_provider):
        """
        Test the initialization of the DataProcessor class.

        This test verifies:
        1. The constructor correctly sets all instance variables with provided parameters.
        2. Default values are applied when optional parameters are not provided.
        3. The BigQueryProvider is properly instantiated.
        4. The _process_data method is called once.
        5. The blacklist is properly applied.
        6. pending_agreements are correctly set.
        7. The 'data' DataFrame maintains its original content, while adding the new columns.
        8. Optional parameters (existing_agreements, pending_agreements, blacklist) default empty if not set.

        The test uses mock objects for BigQueryProvider and patch decorators for _process_data
        and derive_timestamps to avoid actual data fetching and ensure consistent test behavior.
        """
        # Define test input parameters
        subgraph_id = "test_subgraph"
        prices = {"A": 10, "B": 20, "C": 15}
        existing_agreements = {"A": ["subgraph1"], "B": ["subgraph2"]}
        pending_agreements = {"C": ["subgraph3"]}
        blacklist = ["D"]

        # Create a DataProcessor instance
        processor = DataProcessor(
            data=sample_data,
            subgraph_id=subgraph_id,
            prices=prices,
            bigquery=mock_bigquery_provider,
            existing_agreements=existing_agreements,
            pending_agreements=pending_agreements,
            blacklist=blacklist,
        )

        # Verify that all instance variables are set correctly
        assert processor.subgraph_id == subgraph_id
        assert processor.prices == prices
        assert processor.bigquery == mock_bigquery_provider
        assert processor.existing_agreements == existing_agreements
        assert processor.pending_agreements == pending_agreements
        assert processor.blacklist == blacklist

        # Verify default values for optional parameters
        processor_default = DataProcessor(
            data=sample_data,
            subgraph_id=subgraph_id,
            prices=prices,
            bigquery=mock_bigquery_provider,
        )
        assert processor_default.existing_agreements == {}
        assert processor_default.pending_agreements == {}
        assert processor_default.blacklist == []

    @pytest.mark.parametrize(
        "initial_group, current_group, expected_added, expected_cancelled",
        [
            (
                ["A", "B"],  # initial_group
                ["A", "C"],  # current_group
                {"test_subgraph": ["C"]},  # expected_added
                {"test_subgraph": ["B"]},  # expected_cancelled
            ),
            (
                [],  # initial_group
                ["A", "B"],  # current_group
                {"test_subgraph": ["A", "B"]},  # expected_added
                {},  # expected_cancelled (no cancellations)
            ),
            (
                ["A", "B", "C"],  # initial_group
                [],  # current_group
                {},  # expected_added (no additions)
                {"test_subgraph": ["A", "B", "C"]},  # expected_cancelled
            ),
            (
                ["A", "B"],  # initial_group
                ["A", "B"],  # current_group
                {},  # expected_added (no additions)
                {},  # expected_cancelled (no cancellations)
            ),
            (
                ["A"],  # initial_group
                ["B"],  # current_group
                {"test_subgraph": ["B"]},  # expected_added
                {"test_subgraph": ["A"]},  # expected_cancelled
            ),
        ],
    )
    def test_get_indexer_selections(
        self,
        sample_data,
        initial_group,
        current_group,
        expected_added,
        expected_cancelled,
        mock_bigquery_provider,
    ):
        """
        This test verifies the get_indexer_selections method correctly identifies the
        recent added and cancelled indexers.
        """
        with patch("iisa.iisa.DataProcessor._process_data"):
            # Create a DataProcessor instance
            processor = DataProcessor(
                data=sample_data,
                subgraph_id="test_subgraph",
                prices={"A": 10, "B": 20, "C": 15},
                bigquery=mock_bigquery_provider.return_value,
            )

        processor.initial_group = initial_group
        processor.current_group = current_group

        # Call the method under test
        added, cancelled = processor.get_indexer_selections()

        # Sort the lists within the dictionaries
        added_sorted = {k: sorted(v) for k, v in added.items()}
        cancelled_sorted = {k: sorted(v) for k, v in cancelled.items()}
        expected_added_sorted = {k: sorted(v) for k, v in expected_added.items()}
        expected_cancelled_sorted = {
            k: sorted(v) for k, v in expected_cancelled.items()
        }

        # Verify the results by comparing sorted dictionaries
        assert (
            added_sorted == expected_added_sorted
        ), f"Expected added: {expected_added_sorted}, but got: {added_sorted}"
        assert (
            cancelled_sorted == expected_cancelled_sorted
        ), f"Expected cancelled: {expected_cancelled_sorted}, but got: {cancelled_sorted}"

    def test_get_indexer_selections_empty_groups(
        self, sample_data, mock_bigquery_provider
    ):
        """
        Test get_indexer_selections method when both initial_group and current_group are empty.

        This test verifies that the method handles the scenario where both the initial_group
        and current_group are empty (represented as an empty list and an empty set respectively).
        It ensures that the method returns empty lists for both added and cancelled indexers
        when there are no indexers in either group.
        """
        with patch("iisa.iisa.DataProcessor._process_data"):
            processor = DataProcessor(
                data=sample_data,
                subgraph_id="test_subgraph",
                prices={"A": 10, "B": 20, "C": 15},
                bigquery=mock_bigquery_provider.return_value,
            )

        processor.initial_group = []
        processor.current_group = set()

        added, cancelled = processor.get_indexer_selections()

        # Verify that no indexers were added or cancelled.
        assert added == {}
        assert cancelled == {}

    @patch("iisa.iisa.DataProcessor._fetch_number_of_indexer_agreements")
    @patch("iisa.iisa.DataProcessor._get_current_group")
    @patch("iisa.iisa.DataProcessor._normalize_and_score")
    @patch("iisa.iisa.DataProcessor._assign_indexers_to_subgraph")
    def test_process_data(
        self,
        mock_assign,
        mock_normalize,
        mock_get_group,
        mock_fetch,
        sample_data,
        mock_bigquery_provider,
    ):
        """
        Test the _process_data method of the DataProcessor class.

        This test verifies that:
        1. The _process_data method calls the methods in the correct order.
        2. Each method is called exactly once during processing.
        3. The _process_data method handles the data correctly, passing results between methods.
        4. The current_group and initial_group are properly set and updated.
        5. The data is correctly sorted by weighted_score.
        """
        # Create a DataProcessor instance
        processor = DataProcessor(
            data=sample_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15},
            bigquery=mock_bigquery_provider.return_value,
        )

        # Reset all mock call counts after initialization
        mock_fetch.reset_mock()
        mock_get_group.reset_mock()
        mock_normalize.reset_mock()
        mock_assign.reset_mock()

        # Set up mock return values
        mock_fetch.return_value = pd.DataFrame(
            {"indexer": ["A", "B", "C"], "existing_dips_agreements": [1, 2, 3]}
        )
        mock_get_group.return_value = ["A", "B"]
        mock_normalize.return_value = pd.DataFrame(
            {"indexer": ["A", "B", "C"], "weighted_score": [0.8, 0.7, 0.9]}
        )

        # Call the method under test
        processor._process_data()

        # Verify that all expected methods were called only once
        assert mock_fetch.call_count == 1
        assert mock_get_group.call_count == 1
        assert mock_normalize.call_count == 1
        assert mock_assign.call_count == 1

        # Verify the order of method calls
        expected_call_order = [
            call._fetch_number_of_indexer_agreements(),
            call._get_current_group(),
            call._normalize_and_score(),
            call._assign_indexers_to_subgraph(),
        ]
        actual_calls = (
            mock_fetch.mock_calls
            + [mock_get_group.mock_calls[0]]
            + mock_normalize.mock_calls
            + mock_assign.mock_calls
        )
        assert actual_calls == expected_call_order

        # Verify that the current_group and initial_group are set correctly
        assert processor.current_group == ["A", "B"]
        assert processor.initial_group == ["A", "B"]

    def test_fetch_number_of_indexer_agreements(
        self, sample_data, mock_bigquery_provider
    ):
        """
        This test verifies the _fetch_number_of_indexer_agreements method updates the
        'existing_dips_agreements' column based on the existing_agreements.
        """
        # Create a DataProcessor instance with specific existing agreements
        with patch("iisa.iisa.DataProcessor._process_data"):
            processor = DataProcessor(
                data=sample_data,
                subgraph_id="test_subgraph",
                prices={"A": 10, "B": 20, "C": 15},
                existing_agreements={
                    "subgraph1": ["A", "B", "A"],
                    "subgraph2": ["A", "B"],
                    "subgraph3": ["A"],
                },
                bigquery=mock_bigquery_provider.return_value,
            )

        # Call the method under test
        updated_data = processor._fetch_number_of_indexer_agreements()

        # Verify that 'existing_dips_agreements' are updated correctly
        assert (
            updated_data.loc[
                updated_data["indexer"] == "A", "existing_dips_agreements"
            ].iloc[0]
            == 4
        ), "A issue"
        assert (
            updated_data.loc[
                updated_data["indexer"] == "B", "existing_dips_agreements"
            ].iloc[0]
            == 2
        ), "B issue"
        assert (
            updated_data.loc[
                updated_data["indexer"] == "C", "existing_dips_agreements"
            ].iloc[0]
            == 0
        ), "C issue"

    @pytest.fixture
    def processor(self, sample_data, mock_bigquery_provider):
        return DataProcessor(
            data=sample_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25},
            bigquery=mock_bigquery_provider.return_value,
        )

    def test_get_current_group_normal_case(self, processor):
        """
        Test _get_current_group with multiple indexers assigned to the subgraph.
        """
        processor.existing_agreements = {
            "test_subgraph": ["A", "B", "D"],
            "other_subgraph": ["A", "C"],
            "another_subgraph": ["D"],
        }
        result = processor._get_current_group()
        expected = ["A", "B", "D"]
        assert set(result) == set(expected)

    def test_get_current_group_no_assigned_indexers(self, processor):
        """
        Test _get_current_group when no indexers are assigned to the subgraph.
        """
        processor.existing_agreements = {
            "A": ["other_subgraph"],
            "B": ["another_subgraph"],
            "C": ["yet_another_subgraph"],
        }
        result = processor._get_current_group()
        assert result == []

    def test_get_current_group_empty_agreements(self, processor):
        """
        Test _get_current_group with empty existing_agreements.
        """
        processor.existing_agreements = {}
        result = processor._get_current_group()
        assert result == []

    def test_get_current_group_subgraph_not_in_agreements(
        self, processor, mock_bigquery_provider
    ):
        """
        Test _get_current_group when the subgraph 'test_subgraph' is not in any agreement.
        """
        processor.existing_agreements = {
            "A": ["other_subgraph1", "other_subgraph2"],
            "B": ["other_subgraph3", "other_subgraph4"],
        }
        result = processor._get_current_group()
        assert result == []

    @patch("iisa.iisa.normalize_metrics")
    @patch("iisa.iisa.calculate_weighted_score")
    def test_normalize_and_score(
        self, mock_calculate_score, mock_normalize, sample_data, mock_bigquery_provider
    ):
        """
        Test the _normalize_and_score method.

        This test verifies that:
        1. The method calls normalize_metrics with the correct input.
        2. It applies calculate_weighted_score to each row of the normalized data.
        3. The resulting DataFrame contains a 'weighted_score' column with expected values.
        4. The method handles the data flow correctly, passing results between functions.
        5. The weights used in calculate_weighted_score match the expected structure.
            - They are passed as a dictionary
            - They contain all expected metric keys
            - The sum of weights is approximately 1.0
        6. The number and type of arguments passed to calculate_weighted_score are correct.
        7. The method produces the expected output structure and values.

        Note: This test does not verify specific weight values or exception handling for
        normalization and score calculation, as these are implementation details that may change.
        """
        # Create a DataProcessor instance
        with patch("iisa.iisa.DataProcessor._process_data"):
            processor = DataProcessor(
                data=sample_data,
                subgraph_id="test_subgraph",
                prices={"A": 10, "B": 20, "C": 15},
                bigquery=mock_bigquery_provider.return_value,
            )

        # Set up mock return values
        normalized_data = sample_data.copy()
        for metric in [
            "lat_lin_reg_coefficient",
            "uptime_score",
            "existing_dips_agreements",
            "stake_to_fees_iqr_deviation",
            "success_rate",
            "avg_sync_duration",
            "indexing_agreement_acceptance_latency",
        ]:
            normalized_data[f"norm_{metric}"] = normalized_data[metric]
        mock_normalize.return_value = normalized_data
        mock_calculate_score.return_value = 0.8

        # Call the _normalize_and_score method
        result = processor._normalize_and_score()

        # Verify normalize_metrics was called with correct input
        mock_normalize.assert_called_once()
        pd.testing.assert_frame_equal(mock_normalize.call_args[0][0], sample_data)

        # Verify calculate_weighted_score was called for each row
        assert mock_calculate_score.call_count == len(sample_data)

        # Check weights structure
        for call_args in mock_calculate_score.call_args_list:
            args, kwargs = call_args
            assert len(args) == 2
            assert isinstance(args[1], dict)
            weights = args[1]
            expected_metrics = [
                "lat_lin_reg_coefficient",
                "uptime_score",
                "existing_dips_agreements",
                "stake_to_fees_iqr_deviation",
                "success_rate",
                "avg_sync_duration",
                "indexing_agreement_acceptance_latency",
            ]
            assert all(metric in weights for metric in expected_metrics)
            assert pytest.approx(sum(weights.values())) == 1.0

        # Verify 'weighted_score' column exists and contains expected values
        assert "weighted_score" in result.columns
        expected_scores = pd.Series(
            [0.8] * len(sample_data), name="weighted_score", index=result.index
        )
        pd.testing.assert_series_equal(result["weighted_score"], expected_scores)

    def test_assign_indexers_to_subgraph(self, sample_data, mock_bigquery_provider):
        """
        Test the _assign_indexers_to_subgraph method of DataProcessor.

        This test verifies:
        1. The method calls _add_indexers_to_group when there are fewer than 3 indexers.
        2. The method calls _replace_underperforming_indexers when there are 3 or more indexers.
        """
        with patch("iisa.iisa.DataProcessor._add_indexers_to_group") as mock_add:
            with patch(
                "iisa.iisa.DataProcessor._replace_underperforming_indexers"
            ) as mock_replace:
                processor = DataProcessor(
                    data=sample_data,
                    subgraph_id="test_subgraph",
                    prices={"A": 10, "B": 20, "C": 15},
                    bigquery=mock_bigquery_provider.return_value,
                )

                # Test with fewer than 3 indexers
                processor.current_group = ["A", "B"]
                processor._assign_indexers_to_subgraph()
                assert mock_add.call_count > 0
                mock_replace.assert_not_called()

                # Reset mocks
                mock_add.reset_mock()
                mock_replace.reset_mock()

                # Test with 3 or more indexers
                processor.current_group = ["A", "B", "C"]
                processor._assign_indexers_to_subgraph()
                mock_add.assert_not_called()
                mock_replace.assert_called_once()

    @pytest.mark.parametrize(
        "initial_group, expected_calls, expected_final_group",
        [
            (
                [],  # initial_group
                3,  # expected_calls
                ["B", "C", "D"],  # expected_final_group
            ),
            (
                ["A"],  # initial_group
                2,  # expected_calls
                ["A", "B", "C"],  # expected_final_group
            ),
            (
                ["A", "B"],  # initial_group
                1,  # expected_calls
                ["A", "B", "B"],  # expected_final_group
            ),
            (
                ["A", "B", "C"],  # initial_group
                0,  # expected_calls
                ["A", "B", "C"],  # expected_final_group
            ),
        ],
    )
    def test_add_indexers_to_group(
        self,
        sample_data,
        initial_group,
        expected_calls,
        expected_final_group,
        mock_bigquery_provider,
    ):
        """
        Test the _add_indexers_to_group method of DataProcessor.

        This test verifies:
        1. The method adds indexers to the group until there are 3 indexers in the group.
        2. The method stops adding indexers if no suitable candidates are found.
        3. The method behaves correctly with different initial group sizes.
        """
        processor = DataProcessor(
            data=sample_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25},
            bigquery=mock_bigquery_provider.return_value,
        )

        with patch(
            "iisa.iisa.DataProcessor._find_best_replacement_or_select_best_indexer"
        ) as mock_select:
            mock_select.side_effect = ["B", "C", "D", None]
            processor.current_group = initial_group.copy()

            processor._add_indexers_to_group()

            assert processor.current_group == expected_final_group
            assert mock_select.call_count == expected_calls

            # Check intermediate states
            for i in range(expected_calls):
                mock_select.assert_any_call()

        # Test when no suitable indexers are found
        with patch(
            "iisa.iisa.DataProcessor._find_best_replacement_or_select_best_indexer",
            return_value=None,
        ):
            processor.current_group = ["A"]
            processor._add_indexers_to_group()
            assert processor.current_group == ["A"]

    def test_meets_decentralization_requirements(self, mock_bigquery_provider):
        """
        Test the _meets_decentralization_requirements method of DataProcessor.

        This test verifies:
        1. The method returns True when there are fewer than 2 indexers in the current group.
        2. The method correctly evaluates decentralization based on locations and organizations.
        3. A group that does not _meets_decentralization_requirements will not be marked as true.

        Note:
        _meets_decentralization_requirements accepts new_indexer as an input perameter.
        """
        processor = DataProcessor(
            data=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D"],
                    "destination_loc": ["loc1", "loc1", "loc2", "loc3"],
                    "org": ["org1", "org1", "org2", "org3"],
                }
            ),
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25},
            bigquery=mock_bigquery_provider.return_value,
        )

        # Test with fewer than 2 indexers
        processor.current_group = ["A"]
        assert processor._meets_decentralization_requirements("B")

        # Test with 2 indexers, same location and org
        processor.current_group = ["A", "B"]
        assert processor._meets_decentralization_requirements("C")

        # Test with 2 indexers, different location and org
        processor.current_group = ["A", "C"]
        assert processor._meets_decentralization_requirements("D")

        # Test with 2 indexers, adding one with same location and org
        processor.current_group = ["A", "C"]
        assert processor._meets_decentralization_requirements("B")

        # Test with 3 of the same indexer.
        processor.current_group = ["A", "A"]
        assert not processor._meets_decentralization_requirements("A")

    def test_meets_decentralization_requirements_edge_cases(
        self, mock_bigquery_provider
    ):
        """
        Test _meets_decentralization_requirements with various edge cases.
        """
        processor = DataProcessor(
            data=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D", "E", "F"],
                    "destination_loc": ["loc1", "loc1", "loc2", "loc2", "loc3", "loc3"],
                    "org": ["org1", "org2", "org1", "org2", "org3", "org1"],
                }
            ),
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25, "E": 30, "F": 35},
            bigquery=mock_bigquery_provider.return_value,
        )

        # Test with empty current group
        assert processor._meets_decentralization_requirements("A")

        # Test with indexer 'A' selected twice due to some error
        processor.current_group = ["A", "A"]
        assert processor._meets_decentralization_requirements("E")

        # Test with many indexers
        processor.current_group = ["A", "B", "C", "D", "E", "F"]
        assert processor._meets_decentralization_requirements("F")

        # Additional test: Check that it returns False when decentralization requirements are not met
        processor.current_group = ["A", "B"]
        assert not processor._meets_decentralization_requirements("A")

    def test_replace_underperforming_indexers(
        self, sample_data, mock_bigquery_provider
    ):
        """
        Test the _replace_underperforming_indexers method of DataProcessor.

        This test verifies:
        1. The method replaces an indexer when a better replacement is found.
        2. The method does not replace any indexer when no better replacement is found.
        """
        processor = DataProcessor(
            data=sample_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25},
            bigquery=mock_bigquery_provider.return_value,
        )

        with patch(
            "iisa.iisa.DataProcessor._find_best_replacement_or_select_best_indexer"
        ) as mock_find, patch(
            "iisa.iisa.DataProcessor._calculate_group_score"
        ) as mock_score:
            mock_find.side_effect = ["D", None, None]
            mock_score.side_effect = [0.7, 0.8, 0.7, 0.7]

            processor.current_group = ["A", "B", "C"]
            processor._replace_underperforming_indexers()

            # Verify that the worst indexer in the current group has been replaced with the best available indexer
            assert processor.current_group == ["B", "C", "D"]
            assert mock_find.call_count == 3
            assert mock_score.call_count == 2

    def test_find_best_replacement_or_select_best_indexer(self, mock_bigquery_provider):
        """
        Test the _find_best_replacement_or_select_best_indexer method of DataProcessor.

        This test verifies:
        1. The method returns the best replacement that meets decentralization requirements.
        2. The method returns None when no suitable replacement is found.
        3. The method will not try to replace an indexer with one that is already blacklisted.
        """
        processor = DataProcessor(
            data=pd.DataFrame(
                {
                    "indexer": ["A", "B", "C", "D", "E"],
                    "weighted_score": [0.9, 0.8, 0.7, 0.6, 0.5],
                    "destination_loc": ["loc1", "loc2", "loc3", "loc4", "loc5"],
                    "org": ["org1", "org2", "org3", "org4", "org5"],
                }
            ),
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25, "E": 30},
            bigquery=mock_bigquery_provider.return_value,
        )

        processor.current_group = ["A", "B", "C"]
        processor.blacklist = ["E"]

        with patch(
            "iisa.iisa.DataProcessor._meets_decentralization_requirements"
        ) as mock_decentralization:
            mock_decentralization.side_effect = [True]

            result = processor._find_best_replacement_or_select_best_indexer()

            # Verify the best replacement is D, not E, due to blacklisting.
            assert result == "D"

            # Verify the number of decentralization requirement checks
            assert mock_decentralization.call_count == 1

    def test_calculate_group_score(self, mock_bigquery_provider):
        """
        Test the _calculate_group_score method of the DataProcessor class.

        This test verifies that:
        1. The method correctly calculates group scores for different scenarios:
        2. The method produces consistent results for each scenario.

        The test uses raw, non-normalized sample data to create a DataProcessor instance,
        sets predefined weights, and then calls _calculate_group_score with different
        parameters to test various scenarios.
        """
        # raw non-normalized sample data
        raw_data = pd.DataFrame(
            {
                "indexer": ["A", "B", "C", "D"],
                "destination_loc": ["0,0", "0,0", "0,0", "0,0"],
                "org": ["org1", "org7", "org3", "org2"],
                "existing_dips_agreements": [1, 2, 3, 4],
                "lat_lin_reg_coefficient": [0.1, 0.2, 0.3, 0.4],
                "uptime_score": [0.9, 0.8, 0.7, 0.6],
                "stake_to_fees_iqr_deviation": [0.1, 0.2, 0.3, 0.4],
                "success_rate": [0.95, 0.90, 0.85, 0.80],
                "avg_sync_duration": [100, 200, 300, 400],
                "indexing_agreement_acceptance_latency": [10, 20, 30, 40],
            }
        )

        processor = DataProcessor(
            data=raw_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15, "D": 25},
            bigquery=mock_bigquery_provider.return_value,
        )

        processor.weights = {
            "lat_lin_reg_coefficient": 0.2424,
            "uptime_score": 0.1667,
            "existing_dips_agreements": 0.1212,
            "stake_to_fees_iqr_deviation": 0.1023,
            "success_rate": 0.0625,
            "avg_sync_duration": 0.0625,
            "indexing_agreement_acceptance_latency": 0.2424,
        }

        original_data = processor.data.copy()

        normal_score = processor._calculate_group_score(["A", "B", "C"])
        exclude_score = processor._calculate_group_score(
            ["A", "C"], indexer_to_exclude="B"
        )
        include_score = processor._calculate_group_score(
            ["A", "B"], indexer_to_include="D"
        )

        # How allclose() works: It considers two values a and b to be "close" if: |a - b| <= (atol + rtol * |b|)
        assert np.allclose(normal_score, 0.19696666666666665, rtol=1e-9, atol=1e-9)
        assert np.allclose(exclude_score, 0.07576666666666666, rtol=1e-9, atol=1e-9)
        assert np.allclose(include_score, 0.19033333333333335, rtol=1e-9, atol=1e-9)

        # Verify that the original data was not modified
        pd.testing.assert_frame_equal(processor.data, original_data)

    def test_update_blacklist_cancel_indexing_agreements(
        self, sample_data, mock_bigquery_provider
    ):
        """
        Test the update_blacklist_cancel_indexing_agreements method of DataProcessor.

        This test verifies:
        1. The method correctly identifies agreements to be cancelled based on the new blacklist.
        2. The method returns the correct dictionary of cancelled agreements.
        3. The method updates the internal blacklist of the DataProcessor.
        """
        # Initialize DataProcessor
        processor = DataProcessor(
            data=sample_data,
            subgraph_id="test_subgraph",
            prices={"A": 10, "B": 20, "C": 15},
            existing_agreements={
                "subgraph1": ["A"],
                "subgraph2": ["A", "B"],
                "subgraph3": ["B"],
                "subgraph4": ["A", "D"],
                "subgraph5": ["B"],
                "subgraph6": ["F"],
                "subgraph7": ["A"],
                "subgraph9": ["B", "F"],
                "subgraph10": ["A", "C"],
                "subgraph11": ["E"],
                "subgraph12": ["B", "E"],
                "subgraph14": ["E"],
                "subgraph15": ["E"],
                "subgraph16": ["F"],
                "subgraph20": ["C"],
                "subgraph23": ["F"],
                "subgraph40": ["C"],
                "subgraph41": ["F"],
                "subgraph45": ["F"],
                "subgraph70": ["C"],
                "subgraph100": ["C"],
            },
            pending_agreements={
                "subgraph13": ["B"],
                "subgraph70": ["G"],
                "subgraph90": ["I"],
            },
            blacklist=["H"],
            bigquery=mock_bigquery_provider.return_value,
        )

        # update the blacklist to cancel agreements
        new_blacklist = ["H", "B", "E", "NOT_IN_LIST"]

        # Call update_blacklist_cancel_indexing_agreements with new new_blacklist
        newly_cancelled_agreements = (
            processor.update_blacklist_cancel_indexing_agreements(new_blacklist)
        )
        expected_newly_cancelled_agreements = {
            "B": ["subgraph2", "subgraph3", "subgraph5", "subgraph9", "subgraph12"],
            "E": ["subgraph11", "subgraph12", "subgraph14", "subgraph15"],
        }

        # Check state after update
        print("Newly cancelled indexing agreements: ", newly_cancelled_agreements)
        assert newly_cancelled_agreements == expected_newly_cancelled_agreements

        # Verify that the blacklist has been updated
        assert processor.blacklist == new_blacklist

        # Verify that 'H' and 'NOT_IN_LIST' don't appear in cancelled agreements
        assert "H" not in newly_cancelled_agreements
        assert "NOT_IN_LIST" not in newly_cancelled_agreements
