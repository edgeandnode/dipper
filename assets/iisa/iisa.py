import logging
from datetime import date
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from .bq import BigQueryProvider
from .geoip import GeoipResolver
from .iisa_functions import (
    adjust_rows,
    aggregate_indexer_info,
    calculate_distances,
    calculate_indexer_stake_to_fees,
    calculate_indexer_success_rate,
    calculate_indexer_uptime,
    calculate_weighted_score,
    filter_columns,
    filter_successful_queries,
    hash_sampled_queries,
    iterative_filter,
    merge_and_prepare_dataframes,
    merge_in_indexers_info,
    merge_in_query_geolocation_info,
    normalize_metrics,
    perform_latency_linear_regression,
    strategic_sample,
)
from .network import NetworkProvider
from .time import TimestampStr, derive_timestamps

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DATA_MANAGER_NUM_DAYS = 28

# Constants for iterative filtering
ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS = 2
ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER = 1
ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER = 250
ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT = 250


def _fetch_and_process_data(
    bigquery: BigQueryProvider,
    network: NetworkProvider,
    *,
    start_date: date,
    start_ts: TimestampStr,
    num_days: int,
    target_rows: int = 20_000_000,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
    combined_queries = bigquery.fetch_combined_query_results(
        start_date, num_days, target_rows_per_subgraph
    )

    # Get the network indexers data as a pandas DataFrame
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

    # Specify the columns for regression
    predictor = ["response_time_ms"]
    categorical = ["indexer", "deployment_hash", "indexer_network", "query_id"]
    numeric = ["distance_miles", "fee"]

    # Filter the DataFrame to include only the specified columns for regression
    filtered_data = filter_columns(combined_queries, predictor + categorical + numeric)

    # Filter the DataFrame to include only the rows that have non nan values for numeric columns such as 'distance_miles'
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
        filtered_data,
        latency_linear_regression_indexer_rankings,
        latency_linear_regression_results_df,
    ) = perform_latency_linear_regression(
        filtered_data, predictor, categorical, numeric
    )

    # Calculate indexer query success rate
    indexer_success_rate = calculate_indexer_success_rate(combined_queries)

    # Calculate indexer uptime
    indexer_uptime = calculate_indexer_uptime(data_for_uptime_calculations)

    # Get the initial stake to fees query results as a dataframe
    # df headers are:
    # "indexer",
    # "recent_slashable_stake",
    # "total_query_fees_sum",
    # "stake_to_fees"
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

    return (
        bigquery_data,
        latency_linear_regression_indexer_rankings,
        latency_linear_regression_results_df,
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

    Usage:
    - Use self.method() to call functions defined within this class.
      This allows invoking methods that belong to the specific instance of the class.

    - Use external_module.method(self) to call functions imported from an external module.
      This allows passing the instance of the class (self) to the external method for it to operate on.
    """

    def __init__(
        self,
        bigquery: BigQueryProvider,
        network: NetworkProvider,
        *,
        num_days: int = DATA_MANAGER_NUM_DAYS,
        end_date: Optional[date] = None,
    ) -> None:
        # Dependencies
        self._bq = bigquery
        self._network = network

        # Initialize the number of days to look back
        self.num_days: int = num_days
        self.end_date: Optional[date] = end_date

        # Initialize the data and indexer rankings
        self._data: Optional[pd.DataFrame] = None
        self._latency_linear_regression_indexer_rankings: Optional[pd.DataFrame] = None
        self._latency_linear_regression_results: Optional[pd.DataFrame] = None

    def fetch_data_and_update(
        self, *, num_days: Optional[int] = None, end_date: Optional[date] = None
    ) -> None:
        """
        Fetch the latest data from BigQuery and update the data and indexer rankings information.

        Parameters:
            num_days (optional): Number of days to look back for data. Defaults to the instance attribute.
            end_date (optional): End date for the data fetch. Defaults to the instance attribute.
        """
        # If no num_days/end_date is provided, use the default value from the instance attribute
        num_days = num_days or self.num_days
        end_date = end_date or self.end_date

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
        )

    def get_data(self) -> Optional[pd.DataFrame]:
        """
        Return the cached  data.
        """
        # TODO: Type-annotate this dataframe
        return self._data

    def get_latency_linear_regression_indexer_rankings(self) -> Optional[pd.DataFrame]:
        """
        Return the indexer rankings from the latency linear regression.
        """
        # TODO: Type-annotate this dataframe
        return self._latency_linear_regression_indexer_rankings

    def get_latency_linear_regression_results(self) -> Optional[pd.DataFrame]:
        """
        Return the results dataframe from the latency linear regression.
        """
        # TODO: Type-annotate this dataframe
        return self._latency_linear_regression_results


class DataProcessor:
    """
    DataProcessor is responsible for processing the data from the DataManager class,
    including score calculations, normalization of scores, using custom weightings
    to get an overall weighted score, and selecting the best indexers for subgraphs.
    It also handles indexers that are blacklisted, replacing underperforming indexers,
    and periodically optimizing indexer groups based on quality of service.

    This class has a job lifetime, meaning it is instantiated and used for the specific
    task of adding or replacing an indexer from being assigned to a subgraph_id, then it dies.
    After death the class can be reinstantiated again immediately to add or replace another
    indexer from being assigned to another subgraph_id. This class will figure out weather to
    add or replace an indexer depending on the number of existing indexers serving data on the
    subgraph in question and the quality of the existing indexers serving data on a subgraph compared
    to the best alternative.
    """

    def __init__(
        self,
        data,
        subgraph_id,
        prices,
        bigquery: BigQueryProvider,
        existing_agreements=None,
        pending_agreements=None,
        declined_indexers=None,
        blacklist=None,
        weights=None,
    ):
        """
        Initialize the DataProcessor class with data, subgraph ID, indexer prices, existing indexer
        agreements, and an indexer blacklist.

        Parameters:
            data (DataFrame): Dataset containing indexer information.
            subgraph_id (str): Identifier for the subgraph being processed.
            prices (dict): Pricing info for indexer services.
            existing_agreements (dict, optional): Dictionary of current subgraph-indexer relationships.
            blacklist (list, optional): List of blacklisted indexers.
        """
        # Providers
        self._bq = bigquery

        # Initialize class variables with provided parameters
        self.data = pd.DataFrame(data)
        self.subgraph_id = subgraph_id
        self.prices = prices
        self.existing_agreements = (
            existing_agreements or {}
        )  # Key=subgraph, value=indexer
        self.pending_agreements = (
            pending_agreements or {}
        )  # Key=subgraph, value=indexer
        self.declined_indexers = declined_indexers or {}  # Key=subgraph, value=indexer
        self.blacklist = blacklist or []
        self.weights = weights or {
            "lat_lin_reg_coefficient": 0.2424,
            "uptime_score": 0.1667,
            "existing_dips_agreements": 0.1212,
            "stake_to_fees_iqr_deviation": 0.1023,
            "success_rate": 0.0625,
            "avg_sync_duration": 0.0625,
            "indexing_agreement_acceptance_latency": 0.2424,
            # "initial/ongoing_sync_price" : 0.09 <- future weight, above weights will change slightly when implemented
        }

        # Process the data, we can then call update_blacklist_cancel_indexing_agreements,
        # or get_indexer_selections later after this constructor has finished running.
        self._process_data()

    def update_blacklist_cancel_indexing_agreements(self, new_blacklist):
        """
        Cancels all outstanding indexing agreements for indexers on the blacklist.

        Perameters:
        blacklisted_indexers (list): A list of indexers that have been blacklisted.

        Returns:
        dict: A dictionary where keys are blacklisted indexers and values are lists of subgraphs
              from which they were removed.

        Note:
        - This method does not curently attempt to reassign indexers to the subgraph after
          cancellation of the indexing agreement from the blacklisted indexers. Instead we can loop through
          all of the subgraphs while calling the process_subgraph function. Which will detect when a subgraph
          has less than the threshold number of indexers assigned to it and reassign an appropriate indexer.
          We would do this loop at frequent intervalls anyway, because it will be important to reassign indexing
          agreements to high quality indexers after an indexers quality has slipped based on their updated
          weighted_score. # TODO we could address the above note, as if all indexers on a subgraph got
          blacklisted simutanousely, there could be a longer than nescessary latency while we reassign new indexers.
        - Although it would likely take some time for new indexers to accept the agreements and finish syncing, so this
          additional latency while we wait for the for loop to get to the subgraph, might not be a huge issue.
        """
        #
        self.blacklist = new_blacklist

        cancelled_agreements = {}

        for subgraph, indexers in self.existing_agreements.items():
            for indexer in indexers:
                if indexer in new_blacklist:
                    # If indexer not already in cancelled_agreements, create new key-value
                    if indexer not in cancelled_agreements:
                        cancelled_agreements[indexer] = []
                    # Add subgraphs that the blacklisted indexer will be cancelled from receiving DIPS for.
                    cancelled_agreements[indexer].append(subgraph)

        return cancelled_agreements

    def get_indexer_selections(self):
        """
        Returns the indexers that have recently been assigned to or removed from the subgraph.

        This method compares the initial and current groups of indexers to determine
        which indexers have been added or removed.

        Returns:
            tuple: Two dictionaries:
                - added_dict: A dictionary where the key is the subgraph_id and the value is a list of newly added indexers
                - cancelled_dict: A dictionary where the key is the subgraph_id and the value is a list of removed indexers

        Note:
            If no indexers were added or removed, the respective dictionary will be empty.
        """
        # Compare initial and current groups to determine changes
        added = set(self.current_group) - set(self.initial_group)
        cancelled = set(self.initial_group) - set(self.current_group)

        # Create dictionaries with subgraph_id as key and list of indexers as value
        added_dict = {self.subgraph_id: list(added)} if added else {}
        cancelled_dict = {self.subgraph_id: list(cancelled)} if cancelled else {}

        # Return two separate dictionaries
        return added_dict, cancelled_dict

    def _process_data(self):
        """
        Process data by normalizing metrics and calculating weighted scores.
        """
        # Update the number of existing agreements for each indexer
        self.data = self._fetch_number_of_indexer_agreements()

        # Get the current group of indexers for the subgraph using '_get_current_group'
        self.current_group = self._get_current_group()
        self.initial_group = list(self.current_group)

        # Normalize metrics and calculate scores
        self.data = self._normalize_and_score()

        # Call _assign_indexers_to_subgraph to assign/replace/remove an indexer on the subgraph.
        self._assign_indexers_to_subgraph()

    def _fetch_number_of_indexer_agreements(self):
        """
        Fetch and update the number of existing agreements for each indexer based on current assignments.

        This method updates the 'existing_dips_agreements' field in the df to reflect the number of
        current agreements each indexer has, as specified in the existing_agreements attribute passed by the rust server.
        """
        agreement_counts = {}
        # Count the occurrences of each indexer in existing agreements
        for subgraph_indexers in self.existing_agreements.values():
            for indexer in subgraph_indexers:
                if indexer in agreement_counts:
                    agreement_counts[indexer] += 1
                else:
                    agreement_counts[indexer] = 1

        # Update 'existing_dips_agreements' for all indexers at once
        self.data["existing_dips_agreements"] = (
            self.data["indexer"].map(agreement_counts).fillna(0).astype(int)
        )

        return self.data

    def _get_current_group(self):
        """
        Get the current group of indexers assigned to a subgraph (data from self.existing_agreements).

        Returns:
            list: A list containing the indexer assigned to 'self.subgraph_id', or an empty list if no indexer is assigned.
        """
        # Check if the subgraph_id exists in the agreements and return the corresponding indexers
        return self.existing_agreements.get(self.subgraph_id, [])

    def _normalize_and_score(self):
        """
        Normalize metrics assessing indexer quality and calculate weighted scores.

        This method attempts to normalize the data and calculate weighted scores.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        try:
            normalized_data = normalize_metrics(self.data)
        except Exception as e:
            logger.error(
                f"Unexpected error when trying normalize_metrics(self.data): {e}"
            )
            normalized_data = self.data

        try:
            normalized_data["weighted_score"] = normalized_data.apply(
                lambda row: calculate_weighted_score(row, self.weights), axis=1
            )
        except Exception as e:
            logger.error(f"Unexpected error when trying calculate_weighted_score: {e}")
            normalized_data["weighted_score"] = np.nan

        return normalized_data

    def _assign_indexers_to_subgraph(self):
        """
        Assign indexers to subgraph based on weighted scores and decentralization requirements.

        Use the methods _add_indexers_to_group and _replace_underperforming_indexers to
        assign indexers to the subgraph in question.
        """
        # If the current indexer group has less than 3 indexers, call '_add_indexers_to_group'
        if len(self.current_group) < 3:
            self._add_indexers_to_group()

        # If the current indexer group has more than 3 indexers, call '_remove_indexers_from_group'
        if len(self.current_group) > 3:
            self._remove_indexers_from_group()

        # Otherwise, call '_replace_underperforming_indexers' which will search for a suitable replacement
        if len(self.current_group) == 3:
            self._replace_underperforming_indexers()

    def _add_indexers_to_group(self):
        """
        Add indexers to the group to meet the required number of indexers.
        """
        # While the group has less than 3 indexers, select the best indexer to add using _find_best_replacement_or_select_best_indexer
        while len(self.current_group) < 3:
            next_indexer = self._find_best_replacement_or_select_best_indexer()

            # Add the best indexer to the group
            if next_indexer:
                self.current_group.append(next_indexer)

            # If there are no indexers available, do nothing.
            else:
                break

    def _meets_decentralization_requirements(self, new_indexer):
        """
        Check if adding the new indexer meets decentralisation requirements.

        This method is called either when adding indexers to a group with less than 3 indexers,
        or when finding a replacement for an existing indexer in a group of 3 or more.

        The final group must have at least 2 unique organizations and 2 unique locations.
        """
        # If the current group has fewer than 2 indexers, no decentralisation check is needed.
        if len(self.current_group) < 2:
            return True

        # Create a new group including the new indexer
        new_group = self.current_group + [new_indexer]

        # Get unique locations and organizations for the new group
        locations = self.data[self.data["indexer"].isin(new_group)][
            "destination_loc"
        ].unique()
        orgs = self.data[self.data["indexer"].isin(new_group)]["org"].unique()

        # Return 'True' if decentralisation requirements are hit
        if len(locations) >= 2 and len(orgs) >= 2:
            return True

        # Otherwise 'False'
        return False

    def _remove_indexers_from_group(self):
        """
        Remove the worst indexers from the current group until the current group only has 3 indexers.
        """
        while len(self.current_group) > 3:
            indexer_scores = []
            for indexer in self.current_group:
                # Calculate each indexers score as if the indexer had 1 less indexing agreement
                score = self._calculate_indexer_score(indexer)
                indexer_scores.append((indexer, score))

            # Sort indexers by score, worst (lowest score) first
            indexer_scores.sort(key=lambda x: x[1], reverse=False)

            for indexer, _ in indexer_scores:
                temp_group = self.current_group.copy()
                temp_group.remove(indexer)

                if self._meets_decentralization_requirements_indexer_removal(
                    temp_group
                ):
                    self.current_group.remove(indexer)
                    break
            else:
                break

    def _calculate_indexer_score(self, indexer):
        """
        Calculate the score for an individual indexer as if they had one less indexing agreement.
        """
        # Check if the indexer exists in self.data
        indexer_data = self.data[self.data["indexer"] == indexer]

        if indexer_data.empty:
            logger.warning(
                f"Indexer {indexer} not found in self.data. Returning lowest possible score."
            )
            return 0

        # Create a copy of the data for this indexer
        indexer_data = indexer_data.copy()

        # Reduce the indexer's agreement count by 1
        indexer_data["existing_dips_agreements"] = (
            indexer_data["existing_dips_agreements"] - 1
        ).clip(lower=0)

        # Normalize only the necessary metrics for this indexer
        normalized_data = normalize_metrics(indexer_data)

        # Calculate the weighted score for this indexer
        score = calculate_weighted_score(normalized_data.iloc[0], self.weights)

        return score

    def _meets_decentralization_requirements_indexer_removal(self, group):
        """
        Check if the group meets decentralisation requirements after removing an indexer.

        The group must have at least 2 unique organizations and 2 unique locations.
        """
        if len(group) < 2:
            return False

        locations = self.data[self.data["indexer"].isin(group)][
            "destination_loc"
        ].unique()
        orgs = self.data[self.data["indexer"].isin(group)]["org"].unique()

        # Return 'True' if decentralisation requirements are hit
        if len(locations) >= 2 and len(orgs) >= 2:
            return True

        # Otherwise 'False'
        return False

    def _replace_underperforming_indexers(self):
        """
        Replace underperforming indexers if the group score can be improved by more than 10%.
        This method updates the current_group but does not modify the DataFrame, as the
        DataProcessor instance is short-lived and the DataFrame state isn't used after processing.
        """
        worst_indexer = None
        worst_score_improvement = None
        best_replacement = None

        # For each indexer in the current group
        for indexer in self.current_group:
            # Check the most appropriate replacement indexer to replace the indexer in question.
            new_indexer = self._find_best_replacement_or_select_best_indexer()

            if new_indexer:
                # Create a temp copy of the current group, remove the old indexer from it, add the new indexer.
                temp_group = self.current_group.copy()
                temp_group.remove(indexer)
                temp_group.append(new_indexer)

                # Calculate group score of old group as if the removed indexer had 1 less indexing agreement.
                group_score_before = self._calculate_group_score(
                    self.current_group, indexer_to_exclude=indexer
                )

                # Calculate group score of new group as if the replacement indexer had 1 more indexing agreement.
                group_score_after = self._calculate_group_score(
                    temp_group, indexer_to_include=new_indexer
                )

                # Calculate how much better the new group is than the old group.
                score_improvement = group_score_after - group_score_before

                # If new group is >= 10% better than old group
                if score_improvement >= group_score_before * 0.1:
                    # And score improvement is the best available, take note of the indexer to be replaced
                    # and the indexer to do the replacement.
                    if (
                        worst_score_improvement is None
                        or score_improvement > worst_score_improvement
                    ):
                        worst_score_improvement = score_improvement
                        worst_indexer = indexer
                        best_replacement = new_indexer

        # Once the best replacement has been found, remove old indexer from group & add new indexer to group.
        if best_replacement and worst_indexer:
            self.current_group.remove(worst_indexer)
            self.current_group.append(best_replacement)

    def _find_best_replacement_or_select_best_indexer(self):
        """
        This function is used when either:

            - Finding the best replacement for an indexer in the current group.
              (assuming the group has reached capacity)

            - Selecting the best indexer to add to the current group.
              (assuming the group capcitiy has not yet been reached)

        Will not attempt to assign an indexing agreement to an indexer under the following conditions:
        1. The indexer is already in the current group.
        2. The indexer is blacklisted.
        3. The indexer has pending agreements that they have not yet accepted.
        4. The indexer has previously declined an indexing agreement for this subgraph.

        Note:
        - declined_indexers is intended to contain only those indexers that declined within the last
        x days (x=10 seems like a good starting point) and which subgraph they declined.

        Example of declined_indexers structure:
        {
            "subgraph1": ["indexer1", "indexer2"],
            "subgraph2": ["indexer1"]
        }
        In the example above we would not attempt to offer an indexing agreement to:
            - indexer1 for either subgraph1 or subgraph2.
            - indexer2 for subgraph1

        Returns:
        str or None: The best indexer, or None if no suitable candidate is found.
        """

        def flatten_list_of_lists(list_of_lists):
            """
            In the context being used here:
            - This function returns a list of indexers that have pending agreements.
            """
            flattened_list = []
            for sublist in list_of_lists:
                for item in sublist:
                    flattened_list.append(item)
            return flattened_list

        unpickable_indexers = set(
            self.current_group
            + self.blacklist
            + flatten_list_of_lists(self.pending_agreements.values())
            + self.declined_indexers.get(self.subgraph_id, [])
        )

        # The candiates we could select are those that are not unpickable
        candidates = self.data[~self.data["indexer"].isin(unpickable_indexers)].copy()

        # Sort the candidates by weighted score, highest score first.
        candidates.sort_values(by="weighted_score", ascending=False, inplace=True)

        # Iterate through the list of candidates, return the first (best) candidate that meets decentralization requirements
        for indexer in candidates["indexer"]:
            if self._meets_decentralization_requirements(indexer):
                return indexer

        return None

    def _calculate_group_score(
        self, group, indexer_to_exclude=None, indexer_to_include=None
    ):
        """
        Temporarily adjust the number of indexing agreements for specified indexers and calculate
        the average weighted score of the new indexer group.

        This method is intended to have only one of [indexer_to_exclude, indexer_to_include] passed
        into it at a time, at most.
        """
        score = None
        try:
            if indexer_to_exclude:
                # Temporarily adjust the data to reflect the indexer losing an agreement
                self.data.loc[
                    self.data["indexer"] == indexer_to_exclude,
                    "existing_dips_agreements",
                ] -= 1
                self._recalculate_metrics_and_scores()

            if indexer_to_include:
                # Temporarily adjust the data to reflect the indexer gaining an agreement
                self.data.loc[
                    self.data["indexer"] == indexer_to_include,
                    "existing_dips_agreements",
                ] += 1
                self._recalculate_metrics_and_scores()

            # Calculate the average weighted score of the new indexer group
            score = self.data[self.data["indexer"].isin(group)]["weighted_score"].mean()

        finally:
            if indexer_to_exclude:
                # Revert the temporary change
                self.data.loc[
                    self.data["indexer"] == indexer_to_exclude,
                    "existing_dips_agreements",
                ] += 1
                self._recalculate_metrics_and_scores()

            if indexer_to_include:
                # Revert the temporary change
                self.data.loc[
                    self.data["indexer"] == indexer_to_include,
                    "existing_dips_agreements",
                ] -= 1
                self._recalculate_metrics_and_scores()

        return score

    def _recalculate_metrics_and_scores(self):
        """
        Helper method to recalculate metrics and scores.
        """
        self.data = normalize_metrics(self.data)
        self.data["weighted_score"] = self.data.apply(
            calculate_weighted_score, axis=1, weights=self.weights
        )


# This block serves as a functional test and an example implementation
if __name__ == "__main__":

    def process_subgraph(
        data,
        subgraph_id,
        prices,
        existing_agreements,
        pending_agreements,
        blacklist,
        weights=None,
        bigquery=BigQueryProvider("graph-mainnet", "US"),
    ):
        return DataProcessor(
            data=data,
            subgraph_id=subgraph_id,
            prices=prices,
            existing_agreements=existing_agreements,
            pending_agreements=pending_agreements,
            blacklist=blacklist,
            bigquery=bigquery,
            weights=weights
            or {
                "lat_lin_reg_coefficient": 0.2424,
                "uptime_score": 0.1667,
                "existing_dips_agreements": 0.1212,
                "stake_to_fees_iqr_deviation": 0.1023,
                "success_rate": 0.0625,
                "avg_sync_duration": 0.0625,
                "indexing_agreement_acceptance_latency": 0.2424,
            },
        )

    try:
        # Initialize DataManager (done once at project creation)
        # This also performs the initial data fetch
        geoip = GeoipResolver()
        network_provider = NetworkProvider(geoip=geoip)
        bigquery_provider = BigQueryProvider("graph-mainnet", "US")
        data_manager = DataManager(bigquery=bigquery_provider, network=network_provider)

        # Simulate periodic data update (should be done once every 24 hours)
        data_manager.fetch_data_and_update()

        # Get the latest data
        data = data_manager.get_data()
        if data is None:
            raise ValueError("DataManager initial fetch required")

        # Save the data to a CSV file
        data.to_csv("DataManager_GetData_DataFrame.csv", index=False)

        # Example values
        subgraph_id = "QmSubgraph1"
        prices = {"0xIndexer1": 10, "0xIndexer2": 20, "0xIndexer3": 15}
        existing_agreements = {
            "0xIndexer1": ["QmSubgraph1", "QmSubgraph2"],
            "0xIndexer2": ["QmSubgraph3"],
        }
        pending_agreements = {"0xIndexer3": ["QmSubgraph4"]}
        blacklist = ["0xBlacklistedIndexer"]

        # Process subgraph
        try:
            processor = process_subgraph(
                data,
                subgraph_id,
                prices,
                existing_agreements,
                pending_agreements,
                blacklist,
            )
            added, cancelled = processor.get_indexer_selections()
            print(f"Initial processing - Added: {added}, Cancelled: {cancelled}")

        except Exception as e:
            print(f"An error occurred during new subgraph processing: {e}")

        # Simulate updates with new data:
        new_subgraph_id = "QmNewSubgraphId"
        new_prices = {
            **prices,
            "0xIndexer2": 22,  # Update existing price
            "0xIndexer4": 25,  # Add new price
        }
        new_existing_agreements = {
            **existing_agreements,
            "0xNewIndexer": ["QmNewSubgraph"],  # Add a new agreement
            "0xIndexer1": existing_agreements["0xIndexer1"]
            + ["QmNewSubgraph2"],  # Update existing
        }
        new_pending_agreements = {
            **pending_agreements,
            "0xIndexer4": ["QmSubgraph5"],  # Add a new pending agreement
        }
        new_blacklist = ["0xBlacklistedIndexer", "0xNewBlacklistedIndexer"]
        new_weights = {
            "lat_lin_reg_coefficient": 0.1,
            "uptime_score": 0.1,
            "existing_dips_agreements": 0.1,
            "stake_to_fees_iqr_deviation": 0.1,
            "success_rate": 0.1,
            "avg_sync_duration": 0.1,
            "indexing_agreement_acceptance_latency": 0.4,
        }

        # Process new subgraph
        try:
            processor = process_subgraph(
                data,
                new_subgraph_id,
                new_prices,
                new_existing_agreements,
                new_pending_agreements,
                new_blacklist,
                weights=new_weights,
            )
            added, cancelled = processor.get_indexer_selections()
            print(f"New subgraph processing - Added: {added}, Cancelled: {cancelled}")

        except Exception as e:
            print(f"An error occurred during new subgraph processing: {e}")

        # Demonstrate blackisting indexers.
        updated_blacklist = [
            "0xBlacklistedIndexer",
            "0xNewBlacklistedIndexer",
            "0xAnotherBlacklistedIndexer" "0xIndexer1",
            "0xIndexer2",
            "0xNewIndexer",
        ]

        # Create a DataProcessor instance
        try:
            data_processor = DataProcessor(
                data,
                new_subgraph_id,
                new_prices,
                existing_agreements=new_existing_agreements,
                pending_agreements=new_pending_agreements,
                blacklist=new_blacklist,
                weights=new_weights,
                bigquery=BigQueryProvider("graph-mainnet", "US"),
            )

            # Update and reprocess data
            cancelled_agreements = (
                data_processor.update_blacklist_cancel_indexing_agreements(
                    updated_blacklist
                )
            )

            print(
                f"After update_blacklist_cancel_indexing_agreements - Cancelled: {cancelled_agreements}"
            )

        except Exception as e:
            print(f"An error occurred during creation of DataProcessor instance: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
