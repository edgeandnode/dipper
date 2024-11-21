from typing import Optional

from ..perf import PerfHistoryDataFrame
from ..typing import DeploymentId, IndexerId


def select_one(
    perf: PerfHistoryDataFrame,
    deployment_id: DeploymentId,
    candidate_pool: list[IndexerId],
    existing_agreements: Optional[dict[DeploymentId, IndexerId]] = None,
    rejected_indexers: Optional[dict[DeploymentId, IndexerId]] = None,
) -> Optional[IndexerId]:
    """
    Selects a single candidate indexer for indexing a Subgraph deployment.

    :param perf: The curated performance history data.
    :param deployment_id: The Subgraph deployment ID.
    :param candidate_pool: A list of candidate indexers to select from.
    :param existing_agreements: A dictionary of existing agreements. Maps deployment IDs to indexer IDs.
    :param rejected_indexers: A dictionary of rejected indexers. Maps deployment IDs to indexer IDs.
    :return: The selected indexer ID. If no suitable indexer is found, returns None.
    """
    raise NotImplementedError


def select_many(
    perf: PerfHistoryDataFrame,
    deployment_id: DeploymentId,
    candidate_pool: list[IndexerId],
    n: int,
    existing_agreements: Optional[dict[DeploymentId, IndexerId]] = None,
    rejected_indexers: Optional[dict[DeploymentId, IndexerId]] = None,
) -> list[IndexerId]:
    """
    Selects multiple candidate indexers for indexing a Subgraph deployment.

    :param perf: The curated performance history data.
    :param deployment_id: The Subgraph deployment ID.
    :param candidate_pool: A list of candidate indexers to select from.
    :param n: The target number of indexers to select.
    :param existing_agreements: A dictionary of existing agreements. Maps deployment IDs to indexer IDs.
    :param rejected_indexers: A dictionary of rejected indexers. Maps deployment IDs to indexer IDs.
    :return: A list of selected indexer IDs.
             It can return less than `n` indexers if not enough suitable indexers are found.
    """
    raise NotImplementedError
