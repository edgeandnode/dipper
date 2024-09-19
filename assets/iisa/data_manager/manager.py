import logging
from datetime import date
from typing import Optional, Tuple, cast

import pandera as pa
from pandera.typing import DataFrame, Series

from .processing import (
    adjust_rows,
    aggregate_indexer_info,
    calculate_distances,
    calculate_indexer_stake_to_fees,
    calculate_indexer_success_rate,
    calculate_indexer_uptime,
    filter_successful_queries,
    hash_sampled_queries,
    iterative_filter,
    merge_and_prepare_dataframes,
    merge_in_indexers_info,
    merge_in_query_geolocation_info,
    perform_latency_linear_regression,
    strategic_sample,
)
from ..bq import BigQueryProvider
from ..network import NetworkProvider
from ..time import TimestampStr, derive_timestamps
from ..typing import DeploymentIdField, HttpUrlField, IndexerIdField, QueryIdField

__all__ = [
    "DataManager",
    "RequestHistoryDataFrame",
    "IndexerRankingsDataFrame",
    "LinearRegressionResultsDataFrame",
    "DEFAULT_NUM_DAYS",
    "DEFAULT_TARGET_ROWS",
]

# Constants
DEFAULT_NUM_DAYS = 28
DEFAULT_TARGET_ROWS = 20_000_000

# Constants for iterative filtering
ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS = 2
ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER = 1
ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER = 250
ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT = 250

# Module-level logger
logger = logging.getLogger(__name__)


class RequestHistorySchema(pa.DataFrameModel):
    """
    Schema for the `DataManager` "data" data frame.

    This is a partial schema. Not all columns are included.
    """

    query_id: Series[str] = QueryIdField()
    deployment_hash: Series[str] = DeploymentIdField()
    indexer: Series[str] = IndexerIdField()
    url: Series[str] = HttpUrlField()


RequestHistoryDataFrame = DataFrame[RequestHistorySchema]


class IndexerRankingsSchema(pa.DataFrameModel):
    """
    Schema for the `DataManager` "latency_linear_regression_indexer_rankings" data frame.
    """

    indexer: Series[str] = IndexerIdField()
    rank: Series[int] = pa.Field(ge=1)
    score: Series[float] = pa.Field(ge=0.0, le=1.0)


IndexerRankingsDataFrame = DataFrame[IndexerRankingsSchema]


class LinearRegressionResultsSchema(pa.DataFrameModel):
    """
    Schema for the `DataManager` "latency_linear_regression_results" data frame.
    """

    variable: Series[str]
    coefficient: Series[float]
    standard_error: Series[float]
    p_value: Series[float]


LinearRegressionResultsDataFrame = DataFrame[LinearRegressionResultsSchema]


def _fetch_and_process_data(
    bigquery: BigQueryProvider,
    network: NetworkProvider,
    *,
    start_date: date,
    start_ts: TimestampStr,
    num_days: int,
    target_rows: int = DEFAULT_TARGET_ROWS,
) -> Tuple[
    RequestHistoryDataFrame,
    IndexerRankingsDataFrame,
    LinearRegressionResultsDataFrame,
]:
    """
    Fetch data from BigQuery and Network providers, process it, and return the results.

    Parameters:
        bigquery: BigQueryProvider instance.
        network: NetworkProvider instance.
        start_date: Start date for the data fetch.
        start_ts: Start timestamp for the data fetch.
        num_days: Number of days to look back for data.
        target_rows: Target number of rows to fetch from the combined query.

    Returns:
        - A dataframe containing the combined queries processed data.
        - Indexer rankings based on linear regression.
    """
    # Fetch the initial query results using the initial query as input
    # initial_query_results_pandas will contain [deployment_hash, indexer, num_rows]
    logger.debug("Fetching initial query results")
    initial_query_results_pandas = bigquery.fetch_initial_query_results(
        start_date, num_days
    )

    # Figure out how many queries to take from each [indexer, subgraph] combination to target n queries overall
    target_rows_per_subgraph = adjust_rows(
        initial_query_results_pandas,
        target_rows,
    )

    # Fetch the combined query results using the combined query as input
    # combined_queries will contain ['query_id', 'deployment_hash', 'fee', 'timestamp', 'blocks_behind',
    # 'response_time_ms', 'indexer', 'status', 'day_partition', 'subgraph_network', 'url']
    logger.debug("Fetching combined query results")
    combined_queries = bigquery.fetch_combined_query_results(
        start_date, num_days, target_rows_per_subgraph
    )

    # Get the network indexers data as a pandas DataFrame
    logger.debug("Fetching network indexers data")
    indexers_df = network.indexers()

    # Merge the indexers info with the combined query data
    combined_queries = merge_in_indexers_info(combined_queries, indexers_df)

    # Extract IATA codes from the combined query data and merge in the IATA information
    # with the combined query data, adds column ['IATA_code'] to combined_queries
    combined_queries = merge_in_query_geolocation_info(combined_queries)

    # Set data_for_uptime_calculations to be a filtered version of the combined_queries DataFrame
    data_for_uptime_calculations = combined_queries[
        ["indexer", "status", "timestamp"]
    ].copy()

    # Apply the vectorized Haversine function to calculate the distance in miles
    combined_queries = calculate_distances(combined_queries)

    # Filter the data to only include rows where status is '200 OK'
    combined_queries = filter_successful_queries(combined_queries)

    # Specify the columns for regression and filter the DataFrame to include
    # only the specified columns for regression, then sanitize the data
    # removing rows with NaN values in the numeric columns
    predictor = ["response_time_ms"]
    categorical = ["indexer", "deployment_hash", "indexer_network", "query_id"]
    numeric = ["distance_miles", "fee"]
    filtered_data = combined_queries[predictor + categorical + numeric]
    filtered_data = filtered_data.dropna(subset=numeric)

    # Apply iterative filtering
    filtered_data = iterative_filter(
        filtered_data,
        ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS,
        ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER,
        ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER,
        ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT,
    )

    # Sample the query IDs to create a balanced representation across indexers
    # Uniform random sampling of query_id for each indexer on each subgraph.
    filtered_data, integer_root = strategic_sample(
        filtered_data, target_rows_per_subgraph
    )

    # Hash the sampled query IDs to the hash mod of the integer root
    filtered_data = hash_sampled_queries(filtered_data, integer_root)

    # update categorical to use the hashed query id's instead of the raw query id's
    categorical = [
        "indexer",
        "deployment_hash",
        "indexer_network",
        "sampled_query_id_hashed_mod_integer_root",
    ]

    # Perform linear regression on the results from the combined query
    (
        latency_linear_regression_indexer_rankings,
        latency_linear_regression_results_df,
    ) = perform_latency_linear_regression(
        filtered_data, predictor, categorical, numeric
    )

    # Calculate indexer query success rate and uptime
    indexer_success_rate = calculate_indexer_success_rate(combined_queries)
    indexer_uptime = calculate_indexer_uptime(data_for_uptime_calculations)

    # Get the initial stake to fees query results as a dataframe
    # df headers are:
    # "indexer",
    # "recent_slashable_stake",
    # "total_query_fees_sum",
    # "stake_to_fees"
    logger.debug("Fetching initial stake to fees query results")
    initial_stake_query_pandas = bigquery.fetch_initial_stake_to_fees(start_ts)

    # Calculate stake to fees ratio
    stake_to_fees = calculate_indexer_stake_to_fees(initial_stake_query_pandas)

    # Group by 'indexer' and aggregate unique 'org' and 'destination_loc' values
    agg_df = aggregate_indexer_info(combined_queries)

    # Merge all data into the main dataframe
    bigquery_data = merge_and_prepare_dataframes(
        indexer_uptime,
        latency_linear_regression_indexer_rankings,
        agg_df,
        indexer_success_rate,
        stake_to_fees,
    )

    return cast(
        Tuple[
            RequestHistoryDataFrame,
            IndexerRankingsDataFrame,
            LinearRegressionResultsDataFrame,
        ],
        (
            bigquery_data,
            latency_linear_regression_indexer_rankings,
            latency_linear_regression_results_df,
        ),
    )


class DataManager:
    """
    DataManager is responsible for fetching, processing, and analyzing BigQuery data on a daily basis.
    This class is instantiated once and reused as needed to ensure efficient data management throughout its lifecycle.

    Responsibilities:
    - Fetches data from BigQuery using specified queries and parameters.
    - Processes the retrieved data by applying various transformations and calculations.
    - Performs statistical analysis and machine learning tasks such as linear regression.
    - Aggregates and merges additional information from multiple data sources.
    - Prepares the data for further use by other components or services.
    """

    def __init__(
        self,
        bigquery: BigQueryProvider,
        network: NetworkProvider,
        *,
        num_days: int = DEFAULT_NUM_DAYS,
        end_date: Optional[date] = None,
    ) -> None:
        # Dependencies
        self._bq = bigquery
        self._network = network

        # Initialize the number of days to look back
        self.num_days: int = num_days
        self.end_date: Optional[date] = end_date

        # Initialize the data and indexer rankings
        self._data: Optional[RequestHistoryDataFrame] = None
        self._latency_linear_regression_indexer_rankings: Optional[
            IndexerRankingsDataFrame
        ] = None
        self._latency_linear_regression_results: Optional[
            LinearRegressionResultsDataFrame
        ] = None

    def fetch_data_and_update(
        self,
        *,
        num_days: Optional[int] = None,
        end_date: Optional[date] = None,
        target_rows: Optional[int] = None,
    ) -> None:
        """
        Fetch the latest data from BigQuery and update the data and indexer rankings information.

        Parameters:
            num_days (optional): Number of days to look back for data. Defaults to the instance attribute.
            end_date (optional): End date for the data fetch. Defaults to the instance attribute.
            target_rows (optional): Target number of rows to fetch from the combined query. Defaults to 20,000,000.
        """
        # If no num_days/end_date is provided, use the default value from the instance attribute
        num_days = num_days or self.num_days
        end_date = end_date or self.end_date
        target_rows = target_rows or DEFAULT_TARGET_ROWS

        # Derive the start and end dates based on the number of days and the end date
        # and fetch and process data
        (start_date, end_date, start_ts, end_ts) = derive_timestamps(num_days, end_date)
        (
            self._data,
            self._latency_linear_regression_indexer_rankings,
            self._latency_linear_regression_results,
        ) = _fetch_and_process_data(
            self._bq,
            self._network,
            start_date=start_date,
            start_ts=start_ts,
            num_days=num_days,
            target_rows=target_rows,
        )

    def get_data(self) -> Optional[RequestHistoryDataFrame]:
        """
        Return the cached data.
        """
        return self._data

    def get_latency_linear_regression_indexer_rankings(
        self,
    ) -> Optional[IndexerRankingsDataFrame]:
        """
        Return the indexer rankings from the latency linear regression.
        """
        return self._latency_linear_regression_indexer_rankings

    def get_latency_linear_regression_results(
        self,
    ) -> Optional[LinearRegressionResultsDataFrame]:
        """
        Return the results dataframe from the latency linear regression.
        """
        return self._latency_linear_regression_results
