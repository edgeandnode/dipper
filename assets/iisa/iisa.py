import pandas as pd
from .bq import BigQueryProvider

from . import iisa_functions


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

    def __init__(self, num_days, bigquery: BigQueryProvider):
        self.num_days = num_days
        self.bigquery = bigquery

        # Initialize timestamps
        (self.start_date, self.end_date, self.start_ts, self.end_ts) = (
            iisa_functions.derive_timestamps(self.num_days)
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

    def update_network_topology(self, indexers):
        self.indexers = indexers

    def fetch_bigquery_data(self):
        """
        Fetch data from BigQuery and cache it for the application's runtime.
        """
        # Fetch the initial query results using the initial query as input
        initial_query_results_pandas = self.bigquery.fetch_initial_query_results(
            self.start_date, self.num_days
        )

        # Figure out how many queries to take from each [indexer, subgraph] combination to target n queries overall
        rows_to_use = iisa_functions.adjust_rows(
            initial_query_results_pandas,
            target_rows=20_000_000,
        )

        # Fetch the combined query results using the combined query as input
        self.bigquery_data = self.bigquery.fetch_combined_query_results(
            self.start_date, self.num_days, rows_to_use
        )

        # Fetch the URL data query results using the URL query as input
        unique_urls_indexers_pandas = self.bigquery.fetch_url_data(
            self.start_date, self.num_days
        )

        # Extract location/org details from the URL data. We should then have a df containing
        # [['location', 'org', 'loc', 'ip']]  = [["country/reigon/city", "org", "lat,long", "ip"]]
        unique_urls_indexers_pandas = iisa_functions.apply_location_details(
            unique_urls_indexers_pandas
        )

        # Merge the information contained inside unique_urls_indexers_pandas with combined_query_pandas
        self.bigquery_data = iisa_functions.merge_dataframes(
            self.bigquery_data, unique_urls_indexers_pandas
        )

        # Create a DataFrame containing the IATA codes and their counts
        iata_df = iisa_functions.extract_iata_codes(self.bigquery_data)

        # Apply location and details extraction to the IATA codes in the DataFrame
        iata_df = iisa_functions.apply_iata_details(iata_df)

        # Extract IATA codes from the combined query data
        self.bigquery_data = iisa_functions.extract_iata_code(self.bigquery_data)

        # Merge the IATA information with the combined query data
        self.bigquery_data = iisa_functions.merge_iata_info(self.bigquery_data, iata_df)

        # Process the combined query DataFrame
        self.bigquery_data = iisa_functions.process_combined_query_pandas(
            self.bigquery_data
        )

        # Split origin_loc and destination_loc into latitude and longitude
        self.bigquery_data = iisa_functions.split_locations(self.bigquery_data)

        # Apply the vectorized Haversine function
        self.bigquery_data = iisa_functions.calculate_distances(self.bigquery_data)

        # Drop the intermediate columns
        self.bigquery_data = iisa_functions.drop_intermediate_columns(
            self.bigquery_data
        )

        # Filter the data to only include rows where status is '200 OK'
        self.bigquery_data = iisa_functions.filter_status(self.bigquery_data)

        # Round the distance in miles
        self.bigquery_data = iisa_functions.apply_round_distance(self.bigquery_data)

        # Specify the columns for regression
        predictor = ["response_time_ms"]
        categorical = ["indexer", "deployment_hash", "indexer_network", "query_id"]
        numeric = ["distance_miles", "fee"]
        all_columns = predictor + categorical + numeric

        # Filter the DataFrame to include only the specified columns for regression
        self.filtered_bigquery_data = iisa_functions.filter_columns(
            self.bigquery_data, all_columns
        )

        # Filter the DataFrame to include only the rows that have non nan values for numeric columns such as 'distance_miles'
        self.filtered_bigquery_data = self.filtered_bigquery_data.dropna(subset=numeric)

        # Apply iterative filtering iterative_filter(df, a, b, c, d)
        # `df`: DataFrame to filter.
        # `a`: Each deployment must be served by at least a indexers.
        # `b`: Each indexer must serve at least b deployments.
        # `c`: Each indexer must serve at least c queries.
        # `d`: Each subgraph deployment must be queried at least d times.
        self.filtered_bigquery_data = iisa_functions.iterative_filter(
            self.filtered_bigquery_data, # `df`
            2, # `a`
            1, # `b`
            250, # `c`
            250, # `d`
        )

        # Sample the query IDs to create a balanced representation across indexers
        self.filtered_bigquery_data, integer_root = iisa_functions.strategic_sample(
            self.filtered_bigquery_data, rows_to_use
        )

        # Hash the sampled query IDs to the hash mod of the integer root
        self.filtered_bigquery_data = iisa_functions.hash_sampled_queries(
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
        self.filtered_bigquery_data, self.indexer_rankings = (
            iisa_functions.perform_linear_regression(
                self.filtered_bigquery_data, predictor, categorical, numeric
            )
        )

        # Calculate indexer query success rate
        self.indexer_success_rate = iisa_functions.calculate_indexer_success_rate(
            self.bigquery_data
        )

        # Calculate indexer uptime
        self.indexer_uptime = iisa_functions.calculate_indexer_uptime(
            self.bigquery_data
        )

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
        self.stake_to_fees = iisa_functions.calculate_stake_to_fees(
            initial_stake_query_pandas
        )

        # Group by 'indexer' and aggregate unique 'org' and 'destination_loc' values
        agg_df = iisa_functions.aggregate_indexer_info(self.bigquery_data)

        # Merge all data into the main dataframe
        self.bigquery_data = iisa_functions.merge_and_prepare_dataframes(
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
            iisa_functions.derive_timestamps(self.num_days)
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
        num_days,
        bigquery: BigQueryProvider,
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
        self.existing_agreements = (
            existing_agreements if existing_agreements is not None else {}
        )
        self.pending_agreements = (
            pending_agreements if pending_agreements is not None else {}
        )
        self.blacklist = blacklist if blacklist is not None else []
        self.bigquery = bigquery
        self.num_days = num_days

        # Initialize timestamps
        (self.start_date, self.end_date, self.start_ts, self.end_ts) = (
            iisa_functions.derive_timestamps(self.num_days)
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
        # Compare initial and current groups to determine changes
        added = set(self.current_group) - set(self.initial_group)
        cancelled = set(self.initial_group) - set(self.current_group)

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
        # Update 'existing_dips_agreements' for each indexer in 'existing_agreements' (class variable)
        for indexer in self.existing_agreements:
            self.data.loc[
                self.data["indexer"] == indexer, "existing_dips_agreements"
            ] = self.existing_agreements[indexer]

        return self.data

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
        """
        self.data = iisa_functions.normalize_metrics(self.data)
        weights = {
            "lin_reg_coefficient": 0.2424,
            "uptime_score": 0.1667,
            "existing_dips_agreements": 0.1212,
            "stake_to_fees_iqr_deviation": 0.1023,
            "success_rate": 0.0625,
            "avg_sync_duration": 0.0625,
            "indexing_agreement_acceptance_latency": 0.2424,
        }
        self.data["weighted_score"] = self.data.apply(
            iisa_functions.calculate_weighted_score, axis=1, weights=weights
        )

        return self.data

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

                # Removed... kept commented out incase needs to be reinstated later.
                # self.data.loc[self.data["indexer"] == next_indexer, "subgraph"] = self.subgraph_id

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
        """
        # If the current group has fewer than 2 indexers, no decentralisation check is needed.
        if len(self.current_group) < 2:
            return True

        # Otherwise, we check if the new_group meets our decentralisation requirements.
        new_group = self.current_group + [new_indexer]
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

            # Removed... kept commented out incase needs to be reinstated later.
            # self.data.loc[self.data["indexer"] == worst_indexer, "subgraph"] = None
            # self.data.loc[self.data["indexer"] == best_replacement, "subgraph"] = self.subgraph_id

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
        if indexer_to_exclude:
            # Temporarily adjust the data to reflect the indexer losing an agreement
            self.data.loc[
                self.data["indexer"] == indexer_to_exclude, "existing_dips_agreements"
            ] -= 1
            self.data = iisa_functions.normalize_metrics(self.data)
            self.data["weighted_score"] = self.data.apply(
                iisa_functions.calculate_weighted_score, axis=1, weights=self.weights
            )

        if indexer_to_include:
            # Temporarily adjust the data to reflect the indexer gaining an agreement
            self.data.loc[
                self.data["indexer"] == indexer_to_include, "existing_dips_agreements"
            ] += 1
            self.data = iisa_functions.normalize_metrics(self.data)
            self.data["weighted_score"] = self.data.apply(
                iisa_functions.calculate_weighted_score, axis=1, weights=self.weights
            )

        # Calculate the average weighted score of the new indexer group
        score = self.data[self.data["indexer"].isin(group)]["weighted_score"].mean()

        if indexer_to_exclude:
            # Revert the temporary change
            self.data.loc[
                self.data["indexer"] == indexer_to_exclude, "existing_dips_agreements"
            ] += 1
            self.data = iisa_functions.normalize_metrics(self.data)
            self.data["weighted_score"] = self.data.apply(
                iisa_functions.calculate_weighted_score, axis=1, weights=self.weights
            )

        if indexer_to_include:
            # Revert the temporary change
            self.data.loc[
                self.data["indexer"] == indexer_to_include, "existing_dips_agreements"
            ] -= 1
            self.data = iisa_functions.normalize_metrics(self.data)
            self.data["weighted_score"] = self.data.apply(
                iisa_functions.calculate_weighted_score, axis=1, weights=self.weights
            )

        return score

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


if __name__ == "__main__":
    # Create a DataManager instance:
    bigqueryprovider = BigQueryProvider("graph-mainnet", "US")

    # Inject bigqueryprovider dependency into DataManager class
    data_manager = DataManager(
        28, bigqueryprovider
    )  # DataManager(num_days (used when assessing how many days worth of data to bring in from BigQuery), bigqueryprovider (injected dependency))

    # Fetch fresh data whenever needed, e.g. once per day.
    # data_manager.update_and_fetch_data()

    # Extract the data from the class.
    data = data_manager.get_data()

    # Create a DataProcessor instance:
    # DataProcessor takes:
    # (data, subgraph_id, prices, num_days, bigqueryprovider, existing_agreements=None, pending_agreements=None, blacklist=None,)
    data_processor = DataProcessor(
        data,  # data
        [],  # subgraph_id
        [],  # prices
        28,  # num_days
        bigqueryprovider,
        {},  # existing_agreements
        {},  # pending_agreements
        [],  # blacklist
    )

    # Update with new data, prices, agreements, pending agreements and blacklist dynamically
    # (new_data, new_prices, new_existing_agreements, new_pending_agreements, new_blacklist)
    # data_processor.update_and_reprocess_data(
    #    [],  # new_data
    #    [],  # new_prices
    #    [],  # new_existing_agreements
    #    [],  # new_pending_agreements
    #    [],  # new_blacklist
    # )

    # Access the results immediately after instantiation
    added_indexers = data_processor.added_indexers
    cancelled_indexers = data_processor.cancelled_indexers
