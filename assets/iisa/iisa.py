from typing import Optional

import pandas as pd
import numpy as np

from .bq import BigQueryProvider
from .iisa_functions import (
    adjust_rows,
    apply_location_details,
    merge_dataframes,
    extract_iata_code,
    merge_in_iata_geolocation_info,
    process_combined_query_pandas,
    split_locations,
    calculate_distances,
    drop_intermediate_columns,
    filter_status,
    apply_round_distance,
    filter_columns,
    iterative_filter,
    strategic_sample,
    hash_sampled_queries,
    perform_linear_regression,
    calculate_indexer_success_rate,
    calculate_indexer_uptime,
    calculate_stake_to_fees,
    aggregate_indexer_info,
    merge_and_prepare_dataframes,
    normalize_metrics,
    calculate_weighted_score,
)
from .time import derive_timestamps

# Constants
DATA_MANAGER_NUM_DAYS = 28
DATA_PROCESSOR_NUM_DAYS = 28

# Constants for iterative filtering
ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS = 2
ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER = 1
ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER = 250
ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT = 250


def initialize_data_manager():
    """
    Initialize and return a new DataManager instance with a configured BigQueryProvider.

    This function creates a BigQueryProvider for "graph-mainnet" project in "US" region,
    and uses it to initialize a new DataManager instance.

    Returns:
        DataManager: An initialized DataManager instance.
    """
    bigqueryprovider = BigQueryProvider("graph-mainnet", "US")
    return DataManager(bigquery=bigqueryprovider)


def process_subgraph(
    data, subgraph_id, prices, existing_agreements, pending_agreements, blacklist
):
    bigqueryprovider = BigQueryProvider("graph-mainnet", "US")
    processor = DataProcessor(
        data=data,
        subgraph_id=subgraph_id,
        prices=prices,
        bigquery=bigqueryprovider,
        existing_agreements=existing_agreements,
        pending_agreements=pending_agreements,
        blacklist=blacklist,
    )
    return processor.added_indexers, processor.cancelled_indexers


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
        num_days=DATA_MANAGER_NUM_DAYS,
        bigquery: Optional[BigQueryProvider] = None,
    ):
        self.num_days = num_days
        self.bigquery = bigquery or BigQueryProvider("graph-mainnet", "US")

        # Initialize timestamps
        (self.start_date, self.end_date, self.start_ts, self.end_ts) = (
            derive_timestamps(self.num_days)
        )

        # Initialize data attributes
        self.bigquery_data = None
        self.indexer_rankings = None
        self.indexer_success_rate = None
        self.indexer_uptime = None
        self.stake_to_fees = None
        self.filtered_bigquery_data = None

        # Fetch initial data upon instantiation
        self.fetch_bigquery_data()

    def fetch_bigquery_data(self):
        """
        Fetch data from BigQuery and cache it for the application's runtime.
        """
        # Fetch the initial query results using the initial query as input
        initial_query_results_pandas = self.bigquery.fetch_initial_query_results(
            self.start_date, self.num_days
        )

        # Figure out how many queries to take from each [indexer, subgraph] combination to target n queries overall
        target_rows_per_subgraph = adjust_rows(
            initial_query_results_pandas,
            target_rows=20_000_000,
        )

        # Fetch the combined query results using the combined query as input
        self.bigquery_data = self.bigquery.fetch_combined_query_results(
            self.start_date, self.num_days, target_rows_per_subgraph
        )

        # Fetch the URL data query results using the URL query as input
        unique_urls_indexers_pandas = self.bigquery.fetch_url_data(
            self.start_date, self.num_days
        )

        # Extract location/org details from the URL data. We should then have a df containing
        # [['location', 'org', 'loc', 'ip']]  = [["country/reigon/city", "org", "lat,long", "ip"]]
        unique_urls_indexers_pandas = apply_location_details(
            unique_urls_indexers_pandas
        )

        # Merge the information contained inside unique_urls_indexers_pandas with combined_query_pandas
        self.bigquery_data = merge_dataframes(
            self.bigquery_data, unique_urls_indexers_pandas
        )

        # Extract IATA codes from the combined query data
        self.bigquery_data = extract_iata_code(self.bigquery_data)

        # Merge the IATA information with the combined query data
        self.bigquery_data = merge_in_iata_geolocation_info(self.bigquery_data)

        # Process the combined query DataFrame
        self.bigquery_data = process_combined_query_pandas(self.bigquery_data)

        # Split origin_loc and destination_loc into latitude and longitude
        self.bigquery_data = split_locations(self.bigquery_data)

        # Apply the vectorized Haversine function
        self.bigquery_data = calculate_distances(self.bigquery_data)

        # Drop the intermediate columns
        self.bigquery_data = drop_intermediate_columns(self.bigquery_data)

        # Filter the data to only include rows where status is '200 OK'
        self.bigquery_data = filter_status(self.bigquery_data)

        # Round the distance in miles
        self.bigquery_data = apply_round_distance(self.bigquery_data)

        # Specify the columns for regression
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network", "query_id"]
        numeric = ["distance_miles", "fee"]
        all_columns = predictor + categorical + numeric

        # Filter the DataFrame to include only the specified columns for regression
        self.filtered_bigquery_data = filter_columns(self.bigquery_data, all_columns)

        # Filter the DataFrame to include only the rows that have non nan values for numeric columns such as 'distance_miles'
        self.filtered_bigquery_data = self.filtered_bigquery_data.dropna(subset=numeric)

        # Apply iterative filtering
        self.filtered_bigquery_data = iterative_filter(
            self.filtered_bigquery_data,
            ITERATIVE_FILTER_MIN_DEPLOYMENT_INDEXERS,
            ITERATIVE_FILTER_MIN_DEPLOYMENTS_PER_INDEXER,
            ITERATIVE_FILTER_MIN_QUERIES_PER_INDEXER,
            ITERATIVE_FILTER_MIN_QUERIES_PER_DEPLOYMENT,
        )

        # Sample the query IDs to create a balanced representation across indexers
        self.filtered_bigquery_data, integer_root = strategic_sample(
            self.filtered_bigquery_data, target_rows_per_subgraph
        )

        # Hash the sampled query IDs to the hash mod of the integer root
        self.filtered_bigquery_data = hash_sampled_queries(
            self.filtered_bigquery_data, integer_root
        )

        # update categorical to use the hashed query id's instead of the raw query id's
        categorical = [
            "indexer",
            "deployment_hash",
            "indexer_network",
            "sampled_query_id_hashed_mod_integer_root",
        ]

        # Perform linear regression on the results from the combined query
        self.filtered_bigquery_data, self.indexer_rankings = perform_linear_regression(
            self.filtered_bigquery_data, predictor, categorical, numeric
        )

        # Calculate indexer query success rate
        self.indexer_success_rate = calculate_indexer_success_rate(self.bigquery_data)

        # Calculate indexer uptime
        self.indexer_uptime = calculate_indexer_uptime(self.bigquery_data)

        # Get the initial stake to fees query results as a dataframe
        # df headers are:
        # "indexer",
        # "recent_slashable_stake",
        # "total_query_fees_sum",
        # "stake_to_fees"
        initial_stake_query_pandas = self.bigquery.fetch_initial_stake_to_fees(
            self.start_ts
        )

        # Calculate stake to fees ratio
        self.stake_to_fees = calculate_stake_to_fees(initial_stake_query_pandas)

        # Group by 'indexer' and aggregate unique 'org' and 'destination_loc' values
        agg_df = aggregate_indexer_info(self.bigquery_data)

        # Merge all data into the main dataframe
        self.bigquery_data = merge_and_prepare_dataframes(
            self.indexer_uptime,
            self.indexer_rankings,
            agg_df,
            self.indexer_success_rate,
            self.stake_to_fees,
        )

    def update_and_fetch_data(self):
        """
        Update timestamps and fetch the latest data from BigQuery.
        """
        (self.start_date, self.end_date, self.start_ts, self.end_ts) = (
            derive_timestamps(self.num_days)
        )
        self.fetch_bigquery_data()

    def get_data(self):
        """
        Return the cached BigQuery data.
        """
        return self.bigquery_data

    def get_indexer_rankings(self):
        """
        Return the indexer rankings.
        """
        return self.indexer_rankings


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
        num_days=DATA_PROCESSOR_NUM_DAYS,
        bigquery=None,
        existing_agreements=None,
        pending_agreements=None,
        blacklist=None,
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
        # Initialize class variables with provided parameters
        self.data = pd.DataFrame(data)
        self.subgraph_id = subgraph_id
        self.prices = prices
        self.num_days = num_days
        self.bigquery = bigquery or BigQueryProvider("graph-mainnet", "US")
        self.existing_agreements = existing_agreements or {}
        self.pending_agreements = pending_agreements or {}
        self.blacklist = blacklist or []

        if "destination_loc" not in self.data.columns:
            self.data["destination_loc"] = "unknown"
        if "org" not in self.data.columns:
            self.data["org"] = "unknown"

        # Initialize timestamps
        (self.start_date, self.end_date, self.start_ts, self.end_ts) = (
            derive_timestamps(self.num_days)
        )

        # initialize the current group and initial group
        self.current_group = self._get_current_group()
        self.initial_group = list(self.current_group)

        # Begin the process of assigning/replacing indexers for the subgraph
        self._process_data()

        # After processing, store the results
        self.added_indexers, self.cancelled_indexers = self.get_indexer_selections()

    def get_indexer_selections(self):
        """
        Returns the indexer-subgraph pairs that have recently been assigned or cancelled.
        This method should be called after data processing to fetch any updates.
        """
        # Handle None values
        current_group = self.current_group or set()
        initial_group = self.initial_group or set()

        # Compare initial and current groups to determine changes
        added = set(current_group) - set(initial_group)
        cancelled = set(initial_group) - set(current_group)

        # Format results as pairs
        added_pairs = [(indexer, self.subgraph_id) for indexer in added]
        cancelled_pairs = [(indexer, self.subgraph_id) for indexer in cancelled]

        return added_pairs, cancelled_pairs

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

        # Sort data by weighted score
        self.data.sort_values(by="weighted_score", ascending=True, inplace=True)

        # Call _assign_indexers_to_subgraph to assign/replace an indexer on the subgraph.
        self._assign_indexers_to_subgraph()

    def _fetch_number_of_indexer_agreements(self):
        """
        Fetch and update the number of existing agreements for each indexer based on current assignments.

        This method updates the 'existing_dips_agreements' field in the df to reflect the number of
        current agreements each indexer has, as specified in the existing_agreements attribute passed by the rust server.
        """
        # Create a copy of the data to avoid modifying the original
        updated_data = self.data.copy()

        # Create a dictionary to store the number of agreements for each indexer
        agreement_counts = {
            indexer: len(subgraphs)
            for indexer, subgraphs in self.existing_agreements.items()
        }

        # Update 'existing_dips_agreements' for all indexers at once
        updated_data["existing_dips_agreements"] = (
            updated_data["indexer"].map(agreement_counts).fillna(0).astype(int)
        )

        return updated_data

    def _get_current_group(self):
        """
        Get the current group of indexers assigned to a subgraph (data from self.existing_agreements).
        """
        # Return a list of indexers currently assigned to 'self.subgraph_id'
        return [
            indexer
            for indexer, subgraphs in self.existing_agreements.items()
            if self.subgraph_id in subgraphs
        ]

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
            normalized_data = self.data

        weights = {
            "lin_reg_coefficient": 0.2424,
            "uptime_score": 0.1667,
            "existing_dips_agreements": 0.1212,
            "stake_to_fees_iqr_deviation": 0.1023,
            "success_rate": 0.0625,
            "avg_sync_duration": 0.0625,
            "indexing_agreement_acceptance_latency": 0.2424,
        }

        try:
            normalized_data["weighted_score"] = normalized_data.apply(
                lambda row: calculate_weighted_score(row, weights), axis=1
            )
        except Exception as e:
            normalized_data["weighted_score"] = np.nan

        return normalized_data

    def _assign_indexers_to_subgraph(self):
        """
        Assign indexers to subgraph based on weighted scores and diversity requirements.

        Use the methods _add_indexers_to_group and _replace_underperforming_indexers to
        assign indexers to the subgraph in question.
        """
        # If the current indexer group assigned has less than 3 indexers, call '_add_indexers_to_group'
        if len(self.current_group) < 3:
            self._add_indexers_to_group()

        # Otherwise, call '_replace_underperforming_indexers' which will search for a suitable replacement
        else:
            self._replace_underperforming_indexers()

    def _add_indexers_to_group(self):
        """
        Add indexers to the group to meet the required number of indexers.
        """
        # While the group has less than 3 indexers, select the best indexer to add using _select_next_best_indexer
        while len(self.current_group) < 3:
            next_indexer = self._select_next_best_indexer()

            # Add the best indexer to the group
            if next_indexer:
                self.current_group.append(next_indexer)

            # If there are no indexers available, do nothing.
            else:
                break

    def _select_next_best_indexer(self):
        """
        Select the next best indexer based on weighted scores and diversity requirements.
        """
        # Iterate through the DataFrame to find the best indexer based on scores/diversity requirements.
        for _, row in self.data.iterrows():
            if (
                row["indexer"] not in self.current_group
                and row["indexer"] not in self.blacklist
            ):
                if self._meets_diversity_requirements(row["indexer"]):
                    return row["indexer"]

        return None

    def _meets_diversity_requirements(self, new_indexer):
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
            new_indexer = self._find_best_replacement(indexer)

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

    def _find_best_replacement(self, indexer_to_replace):
        """
        Find the best replacement for an indexer in the group.
        """
        # Filter out candidates that are already in the group, on the blacklist, or have pending agreements that they
        # have not yet accepted.
        candidates = self.data[
            ~self.data["indexer"].isin(
                self.current_group
                + list(self.blacklist)
                + list(self.pending_agreements)
            )
        ].copy()

        # Sort the remaining candidates by weighted score, highest score first.
        candidates = candidates.sort_values(by="weighted_score", ascending=False)

        # Iterate through the list of candidates, return the first (best) candidate that meets diversity requirements
        for index, row in candidates.iterrows():
            new_group = [i for i in self.current_group if i != indexer_to_replace]
            if self._meets_diversity_requirements(new_group, row["indexer"]):
                return row["indexer"]

        return None

    def _calculate_group_score(
        self, group, indexer_to_exclude=None, indexer_to_include=None
    ):
        """
        Temporarily adjust the number of indexing agreements for specified indexers and calculate
        the average weighted score of the new indexer group.

        This method is intended to have only one of [indexer_to_exclude, indexer_to_include] passed
        into it, at most.
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

    def update_and_reprocess_data(
        self,
        new_data=None,
        new_prices=None,
        new_existing_agreements=None,
        new_pending_agreements=None,
        new_blacklist=None,
    ):
        """
        Update the class variables with new data, prices, existing agreements, pending agreements
        and blacklist in real-time. If new data comes in then call _process_data a second time using
        the new data.
        """
        updated = False

        # Update live data from DataManager class as it comes in, in real-time.
        if new_data is not None:
            updated = True
            self.data = pd.DataFrame(new_data)

        # Update live indexer prices
        if new_prices is not None:
            updated = True
            self.prices = new_prices

        # Update live existing agreements
        if new_existing_agreements is not None:
            updated = True
            self.existing_agreements = new_existing_agreements

        # Update live pending agreements
        if new_pending_agreements is not None:
            updated = True
            self.pending_agreements = new_pending_agreements

        # Manage live blacklist updates
        if new_blacklist is not None:
            updated = True
            # Capture newly blacklisted indexers before updating the class attribute
            newly_blacklisted = set(new_blacklist) - set(self.blacklist)
            self.blacklist = new_blacklist

            # Cancel indexing agreements for newly blacklisted indexers
            for indexer in newly_blacklisted:
                self._cancel_indexing_agreements(indexer)

        # Reprocess the data if there was an update
        if updated:
            self._process_data()

    def _cancel_indexing_agreements(self, indexer):
        """
        Remove the specified indexer from any current indexing groups and update the dataset.
        """
        if indexer in self.current_group:
            self.current_group.remove(indexer)
            self.data.loc[self.data["indexer"] == indexer, "subgraph"] = None

        # Find replacements
        self._assign_indexers_to_subgraph()


# This block serves as a functional test and an example implementation
if __name__ == "__main__":
    try:
        # Initialize DataManager (done once at project creation)
        # This also performs the initial data fetch
        data_manager = initialize_data_manager()

        # Simulate periodic data update (should be done once every 24 hours)
        data_manager.update_and_fetch_data()

        # Get the latest data
        data = data_manager.get_data()

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
            added, cancelled = process_subgraph(
                data,
                subgraph_id,
                prices,
                existing_agreements,
                pending_agreements,
                blacklist,
            )
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
        new_blacklist = [x for x in blacklist if x != "0xBlacklistedIndexer"] + [
            "0xNewBlacklistedIndexer"
        ]

        # Process new subgraph
        try:
            added, cancelled = process_subgraph(
                data,
                new_subgraph_id,
                new_prices,
                new_existing_agreements,
                new_pending_agreements,
                new_blacklist,
            )
            print(f"New subgraph processing - Added: {added}, Cancelled: {cancelled}")

        except Exception as e:
            print(f"An error occurred during new subgraph processing: {e}")

        # Demonstrate updating an existing subgraph with update_and_reprocess_data
        updated_data = data_manager.get_data()
        updated_prices = {**new_prices, "0xIndexer5": 30}
        updated_existing_agreements = {
            **new_existing_agreements,
            "0xIndexer5": ["QmSubgraph6"],
        }
        updated_pending_agreements = {
            **new_pending_agreements,
            "0xIndexer6": ["QmSubgraph7"],
        }
        updated_blacklist = new_blacklist + ["0xAnotherBlacklistedIndexer"]

        # Create a DataProcessor instance for the subgraph we want to update
        try:
            data_processor = DataProcessor(
                data,
                new_subgraph_id,
                new_prices,
                existing_agreements=new_existing_agreements,
                pending_agreements=new_pending_agreements,
                blacklist=new_blacklist,
            )

            # Update and reprocess data
            data_processor.update_and_reprocess_data(
                new_data=updated_data,
                new_prices=updated_prices,
                new_existing_agreements=updated_existing_agreements,
                new_pending_agreements=updated_pending_agreements,
                new_blacklist=updated_blacklist,
            )

            # Get the updated results
            added, cancelled = data_processor.get_indexer_selections()
            print(
                f"After update_and_reprocess_data - Added: {added}, Cancelled: {cancelled}"
            )

        except Exception as e:
            print(f"An error occurred during data processing update: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")
