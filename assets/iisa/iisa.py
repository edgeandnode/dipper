import logging

import numpy as np
import pandas as pd

from .bq import BigQueryProvider
from .iisa_functions import (
    calculate_weighted_score,
    normalize_metrics,
)

# Module-level logger
logger = logging.getLogger(__name__)


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
