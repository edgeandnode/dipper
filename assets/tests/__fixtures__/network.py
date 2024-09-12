import json
import os
from pathlib import Path
from typing import Iterable

from iisa.network import (
    Indexer,
    IndexerId,
)
from iisa.typing import HttpUrlStr


def _parse_fixture_data(data):
    # Get the indexers info from the mock dataset
    indexers = {}
    for subgraph in data:
        for version in subgraph["versions"]:
            deployment = version["subgraphDeployment"]

            for allocation in deployment["indexerAllocations"]:
                indexer_id = allocation["indexer"]["id"]
                indexer_url = allocation["indexer"]["url"]

                # If the indexer is not in the indexers dict, add it
                if indexer_id not in indexers:
                    indexers[indexer_id] = Indexer(
                        indexer_id=IndexerId(indexer_id),
                        url=HttpUrlStr(indexer_url),
                    )

    return indexers


def load_fixture_data() -> Iterable[Indexer]:
    """Loads the fixture JSON data from the filesystem."""
    data = Path(os.path.dirname(__file__)) / "network.json"
    with open(data) as f:
        data = json.load(f)

    return _parse_fixture_data(data).values()
