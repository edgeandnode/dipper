import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pytest
from iisa.iisa import (
    initialize_data_manager,
    process_subgraph,
    DataManager,
)

@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "indexer": ["A", "B", "C"],
            "deployment_hash": ["hash1", "hash2", "hash3"],
            "score": [0.8, 0.6, 0.7],
        }
    )

@pytest.fixture
def mock_url_to_ip():
    return "0.0.0.0"  # Return a dummy IP for any URL

@pytest.fixture
def mock_bigquery_provider():
    mock = MagicMock()
    return mock

