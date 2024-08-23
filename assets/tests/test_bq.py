from datetime import datetime
from iisa.bq import (
    _get_combined_query,
    _get_initial_query,
    _get_url_query,
    _get_initial_stake_to_fees_query,
)


class TestGetCombinedQuery:
    def test_basic_query(self):
        # Given a start date, a number of days and a number of rows to use
        start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

        # When _get_combined_query is called
        query = _get_combined_query(start_date, 10, 20000000)

        # Then the query should match the expected output
        expected_query = """
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
                WHERE day BETWEEN '2024-01-01' AND DATE_ADD('2024-01-01', INTERVAL 10 DAY)
            ),
            indexer_dimensions_arbitrum AS (
                SELECT
                    day AS day_partition,
                    indexer_wallet AS indexer,
                    indexer_url AS url,
                    'mainnet-thegraph-arbitrum' AS indexer_network
                FROM internal_metrics.indexer_dimensions_arbitrum_daily
                WHERE day BETWEEN '2024-01-01' AND DATE_ADD('2024-01-01', INTERVAL 10 DAY)
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
                    status,
                    day_partition,
                    RAND() as rnd
                FROM internal_metrics.metrics_indexer_attempts
                WHERE day_partition BETWEEN '2024-01-01' AND DATE_ADD('2024-01-01', INTERVAL 10 DAY)
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
            WHERE row_num <= 20000000
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
        """
        # Remove excess whitespace and new lines for comparison
        assert "".join(query.split()) == "".join(expected_query.split())


class TestGetInitialQuery:
    def test_basic_query(self):
        # Given a start date and a number of days
        start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

        # When get_initial_query is called
        query = _get_initial_query(start_date, 10)

        # Then the query should match the expected output
        expected_query = """
        WITH BasicFilter AS (
            SELECT
                deployment AS deployment_hash,
                indexer,
                COUNT(*) AS num_rows
            FROM internal_metrics.metrics_indexer_attempts
            WHERE day_partition BETWEEN '2024-01-01' AND DATE_ADD('2024-01-01', INTERVAL 10 DAY)
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
        # Remove excess whitespace and new lines for comparison
        assert "".join(query.split()) == "".join(expected_query.split())


class TestGetUrlQuery:
    def test_get_url_query(self):
        # Given a start date, a number of days and a number of rows to use
        start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")

        # When get_combined_query is called
        query = _get_url_query(start_date, 10)

        # Then the query should match the expected output
        expected_query = """
        SELECT
            day AS day_partition,
            indexer_wallet AS indexer,
            indexer_url AS url,
            'arbitrum' AS indexer_network
        FROM internal_metrics.indexer_dimensions_arbitrum_daily
        WHERE day BETWEEN '2024-01-01' AND DATE_ADD('2024-01-01', INTERVAL 10 DAY)
        AND indexer_wallet IS NOT NULL AND indexer_url IS NOT NULL
        GROUP BY day, indexer_wallet, indexer_url
        ORDER BY day_partition
        """
        # Remove excess whitespace and new lines for comparison
        assert "".join(query.split()) == "".join(expected_query.split())


class TestGetInitialStakeToFeesQuery:
    def test_basic_query(self):
        # Given a start timestamp
        start_ts = "2024-01-01T00:00:00Z"

        # When _get_initial_stake_to_fees_query is called
        query = _get_initial_stake_to_fees_query(start_ts)

        # Then the query should match the expected output
        expected_query = """
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
            WHERE TIMESTAMP(mia.day_partition) > '2024-01-01T00:00:00Z'
            GROUP BY id.indexer_wallet, id.staked_tokens - id.locked_tokens, mia.day_partition
        ) as aggregated_data
        GROUP BY indexer, recent_slashable_stake;
        """
        # Remove excess whitespace and new lines for comparison
        assert "".join(query.split()) == "".join(expected_query.split())
