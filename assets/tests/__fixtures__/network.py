import json
import os
from pathlib import Path
from typing import Iterable

from iisa.network import (
    Indexer,
    IndexerId,
    IndexerSubgraph,
    IndexerSubgraphVersion,
)
from iisa.typing import DeploymentId, HttpUrlStr, SubgraphId


def _parse_fixture_data(data):
    # Get the indexers info from the mock dataset
    indexers = {}
    for subgraph in data:
        subgraph_id = subgraph["id"]

        for version in subgraph["versions"]:
            version_number = version["version"]
            deployment = version["subgraphDeployment"]
            deployment_id = deployment["ipfsHash"]

            for allocation in deployment["indexerAllocations"]:
                indexer_id = allocation["indexer"]["id"]
                indexer_url = allocation["indexer"]["url"]

                # If the indexer is not in the indexers dict, add it
                if indexer_id not in indexers:
                    indexers[indexer_id] = Indexer(
                        indexer_id=IndexerId(indexer_id),
                        url=HttpUrlStr(indexer_url),
                        subgraphs={},
                    )

                # Get the indexer from the indexers dict
                indexer = indexers[indexer_id]

                # If the subgraph is not in the indexer's subgraphs list, add it
                if subgraph_id not in indexer.subgraphs:
                    indexer.subgraphs[subgraph_id] = IndexerSubgraph(
                        subgraph_id=SubgraphId(subgraph_id),
                        versions=[],
                    )

                # Get the subgraph from the indexer's subgraphs list and add the version if it's not there
                subgraph = indexer.subgraphs[subgraph_id]
                if version_number not in subgraph.versions:
                    subgraph.versions.append(
                        IndexerSubgraphVersion(
                            version=version_number,
                            deployment_id=DeploymentId(deployment_id),
                        )
                    )

    return indexers


def load_fixture_data() -> Iterable[Indexer]:
    """Loads the fixture JSON data from the filesystem."""
    data = Path(os.path.dirname(__file__)) / "network.json"
    with open(data) as f:
        data = json.load(f)

    return _parse_fixture_data(data).values()
