"""
The "BigQuery" provider.
"""

from datetime import date
from typing import NewType

from bigframes import pandas as bpd
from bigframes.pandas import DataFrame

QueryStr = NewType("QueryStr", str)

ActiveIndexersDataframe = NewType("ActiveIndexersDataframe", DataFrame)
StakeToFeesDataFrame = NewType("StakeToFeesDataFrame", DataFrame)
CombinedQueryResultsDataFrame = NewType("CombinedQueryResultsDataFrame", DataFrame)
UrlDataFrame = NewType("UrlDataFrame", DataFrame)
InitialQueryResultsDataFrame = NewType("InitialQueryResultDataFrame", DataFrame)


class BigQueryProvider:
    """
    A class that provides read access to Google BigQuery DB.
    """

    def __init__(self, project: str, location: str):
        self.project_id = project
        self.location = location

        # Configure BigQuery project and location
        bpd.options.bigquery.project = project
        bpd.options.bigquery.location = location

    def _read_gbq_dataframe(self, query: QueryStr) -> DataFrame:
        """
        Run read query in Google BigQuery and convert to pandas' DataFrame.

        :param query: The query string
        :return: The read dataset
        """
        return bpd.read_gbq(query, project_id=self.project_id).to_pandas()

    def fetch_active_indexers(self, start_ts: str) -> ActiveIndexersDataframe:
        """
        Execute query to fetch active indexers from BigQuery, then return the results as a DataFrame.
        """
        query = self._get_active_indexers_query(start_ts)
        dataframe = self._read_gbq_dataframe(query)
        return ActiveIndexersDataframe(dataframe)

    def fetch_initial_stake_to_fees(self, start_ts: str) -> StakeToFeesDataFrame:
        """
        Get the initial stake to fees query
        """
        query = _get_initial_stake_to_fees_query(start_ts)
        dataframe = self._read_gbq_dataframe(query)
        return StakeToFeesDataFrame(dataframe)

    def fetch_combined_query_results(
        self, start_date: date, num_days: int, rows_to_use: int
    ) -> CombinedQueryResultsDataFrame:
        """
        Fetch the combined query results.
        """
        query = _get_combined_query(start_date, num_days, rows_to_use)
        dataframe = self._read_gbq_dataframe(query)
        return CombinedQueryResultsDataFrame(dataframe)

    def fetch_url_data(self, start_date: date, num_days: int) -> UrlDataFrame:
        """
        Fetch the url query results.
        """
        query = _get_url_query(start_date, num_days)
        dataframe = self._read_gbq_dataframe(query)
        return UrlDataFrame(dataframe)

    def fetch_initial_query_results(
        self, start_date: date, num_days: int
    ) -> InitialQueryResultsDataFrame:
        """
        Fetch the initial query results.
        """
        query = _get_initial_query(start_date, num_days)
        dataframe = self._read_gbq_dataframe(query)
        dataframe = dataframe.sort_values(by="num_rows", ascending=False)
        return InitialQueryResultsDataFrame(dataframe)


def _get_combined_query(start_date: date, num_days: int, rows_to_use: int) -> QueryStr:
    """
    Construct the combined query to fetch detailed data.
    """
    return QueryStr(f"""
    WITH production_metrics_gateway_subgraph_queries AS (
        WITH initial_data AS (
            SELECT
                day_timestamp AS day_partition,
                subgraph_deployment_ipfs_hash AS deployment_hash,
                subgraph_chain_indexed AS subgraph_network,
                subgraph_deployment_chain AS indexer_network
            FROM production_metrics.prod_metrics_gateway_subgraph_queries
            WHERE subgraph_deployment_ipfs_hash IS NOT NULL
            AND subgraph_chain_indexed IS NOT NULL
            AND subgraph_deployment_chain IS NOT NULL
        ),
        non_dupe_data AS (
            SELECT DISTINCT * FROM initial_data
        ),
        mode_subgraph_networks AS (
            SELECT
                deployment_hash,
                subgraph_network,
                COUNT(subgraph_network) AS freq
            FROM non_dupe_data
            GROUP BY deployment_hash, subgraph_network
        ),
        aggregated_data AS (
            SELECT
                n.deployment_hash,
                ARRAY_AGG(n.indexer_network) AS indexer_network_list,
                ARRAY_AGG(DISTINCT n.subgraph_network) AS subgraph_network_list,
                COUNT(DISTINCT n.indexer_network) AS number_of_unique_indexer_networks,
                COUNT(n.indexer_network) AS number_of_indexer_networks,
                ARRAY_AGG(s.subgraph_network ORDER BY s.freq DESC LIMIT 1)[OFFSET(0)] AS mode_subgraph_network
            FROM non_dupe_data n
            LEFT JOIN mode_subgraph_networks s
            ON n.deployment_hash = s.deployment_hash
            GROUP BY n.deployment_hash
        )
        SELECT
            deployment_hash,
            CASE
                WHEN ARRAY_LENGTH(indexer_network_list) = 1 THEN indexer_network_list[OFFSET(0)]
                ELSE 'arbitrum'
            END AS indexer_network,
            CASE
                WHEN ARRAY_LENGTH(subgraph_network_list) = 1 THEN subgraph_network_list[OFFSET(0)]
                ELSE mode_subgraph_network
            END AS subgraph_network
        FROM aggregated_data
        WHERE deployment_hash IS NOT NULL
        AND deployment_hash <> ''
        ORDER BY number_of_unique_indexer_networks DESC
    ),
    
    combined_indexer_dimensions AS (
        WITH indexer_dimensions AS (
            SELECT
                day AS day_partition,
                indexer_wallet AS indexer,
                indexer_url AS url,
                'mainnet-gateway' AS indexer_network
            FROM internal_metrics.indexer_dimensions_daily
            WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        ),
        indexer_dimensions_arbitrum AS (
            SELECT
                day AS day_partition,
                indexer_wallet AS indexer,
                indexer_url AS url,
                'mainnet-thegraph-arbitrum' AS indexer_network
            FROM internal_metrics.indexer_dimensions_arbitrum_daily
            WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        ),
        combined_data AS (
            SELECT * FROM indexer_dimensions
            UNION ALL
            SELECT * FROM indexer_dimensions_arbitrum
        )
        SELECT
            day_partition,
            indexer,
            url,
            CASE
                WHEN indexer_network = 'mainnet-thegraph-arbitrum' THEN 'arbitrum'
                WHEN indexer_network = 'mainnet-gateway' THEN 'mainnet'
            END AS indexer_network
        FROM combined_data
        WHERE indexer IS NOT NULL AND url IS NOT NULL
        GROUP BY day_partition, indexer, url, indexer_network
        ORDER BY day_partition
    ),
    
    metrics_indexer_attempts AS (
        WITH BasicFilter AS (
            SELECT
                query_id,
                deployment AS deployment_hash,
                query_fee AS fee,
                query_ts AS timestamp,
                CAST(blocks_behind AS INT64) AS blocks_behind,
                SAFE_CAST(response_time_ms AS INT64) AS response_time_ms,
                indexer,
                statushistory,
                day_partition,
                RAND() as rnd
            FROM internal_metrics.metrics_indexer_attempts
            WHERE day_partition BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
            AND deployment IN (SELECT deployment_hash FROM production_metrics_gateway_subgraph_queries)
        ),
        FilteredRows AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY deployment_hash, indexer ORDER BY rnd) as row_num
            FROM BasicFilter
        )
        SELECT
            query_id,
            deployment_hash,
            fee,
            timestamp,
            blocks_behind,
            response_time_ms,
            indexer,
            status,
            day_partition
        FROM FilteredRows
        WHERE row_num <= {rows_to_use}
    )
    
    SELECT
        m.query_id,
        m.deployment_hash,
        m.fee,
        m.timestamp,
        m.blocks_behind,
        m.response_time_ms,
        m.indexer,
        m.status,
        m.day_partition,
        pm.subgraph_network,
        c.url
    FROM metrics_indexer_attempts m
    LEFT JOIN production_metrics_gateway_subgraph_queries pm
    ON m.deployment_hash = pm.deployment_hash
    LEFT JOIN combined_indexer_dimensions c
    ON m.indexer = c.indexer AND m.day_partition = c.day_partition AND pm.indexer_network = c.indexer_network
    WHERE pm.indexer_network = 'arbitrum'
    ORDER BY m.timestamp;
    """)


def _get_initial_query(start_date: date, num_days: int) -> QueryStr:
    """
    Construct the initial query to fetch basic filter data.
    """
    return f"""
    WITH BasicFilter AS (
        SELECT
            deployment AS deployment_hash,
            indexer,
            COUNT(*) AS num_rows
        FROM internal_metrics.metrics_indexer_attempts
        WHERE day_partition BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
        GROUP BY deployment_hash, indexer
    ),
    TotalQueries AS (
        SELECT
            deployment_hash,
            indexer,
            num_rows
        FROM BasicFilter
    )
    SELECT
        deployment_hash,
        indexer,
        num_rows
    FROM TotalQueries;
    """


def _get_url_query(start_date: date, num_days: int) -> QueryStr:
    """
    Construct the query to fetch IP data.
    """
    return QueryStr(f"""
    SELECT
        day AS day_partition,
        indexer_wallet AS indexer,
        indexer_url AS url,
        'arbitrum' AS indexer_network
    FROM internal_metrics.indexer_dimensions_arbitrum_daily
    WHERE day BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND DATE_ADD('{start_date.strftime("%Y-%m-%d")}', INTERVAL {num_days} DAY)
    AND indexer_wallet IS NOT NULL AND indexer_url IS NOT NULL
    GROUP BY day, indexer_wallet, indexer_url
    ORDER BY day_partition
    """)


def _get_initial_stake_to_fees_query(start_ts: str) -> QueryStr:
    """
    Construct the initial query to fetch the stake to fees data.
    """
    return QueryStr(f"""
        SELECT indexer,
            recent_slashable_stake,
            SUM(query_fees_sum) AS total_query_fees_sum,
            recent_slashable_stake / SUM(query_fees_sum) as stake_to_fees
        FROM (
            SELECT  id.indexer_wallet AS indexer,
                    id.staked_tokens - id.locked_tokens as recent_slashable_stake,
                    SUM(mia.query_fee) AS query_fees_sum
            FROM internal_metrics.indexer_dimensions_arbitrum id
            INNER JOIN internal_metrics.metrics_indexer_attempts mia ON id.indexer_wallet = mia.indexer
            WHERE TIMESTAMP(mia.day_partition) > '{start_ts}'
            GROUP BY id.indexer_wallet, id.staked_tokens - id.locked_tokens, mia.day_partition
        ) as aggregated_data
        GROUP BY indexer, recent_slashable_stake;
        """)


def _get_active_indexers_query(start_ts: str) -> QueryStr:
    """
    Construct the initial query to fetch the indexers that have been active more more than the thawind period
    """
    return QueryStr(f"""
    SELECT DISTINCT indexer_wallet AS indexer
    FROM (
        SELECT
            indexer_wallet,
            MIN(staked_tokens) OVER (PARTITION BY indexer_wallet) AS min_staked_tokens
        FROM internal_metrics.indexer_dimensions_arbitrum_daily
        WHERE TIMESTAMP(day) BETWEEN TIMESTAMP_SUB(TIMESTAMP('{start_ts}'), INTERVAL 1 MONTH) AND TIMESTAMP('{start_ts}')
    ) AS subquery
    WHERE min_staked_tokens > 0;
    """)
