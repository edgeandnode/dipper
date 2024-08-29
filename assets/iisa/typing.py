"""
IISA-specific type hints and Pandera schema field factories.
"""

from functools import partial
from typing import NewType

import pandera as pa
import pyarrow

HttpUrlStr = NewType("HttpUrlStr", str)

IataCodeField = partial(
    pa.Field,
    description="IATA 3-letter Location code",
    str_matches=r"^[A-Z]{3}$",
)

LatitudeField = partial(
    pa.Field,
    description="Latitude in decimal degrees",
    in_range={"min_value": -90.0, "max_value": 90.0},
)

LongitudeField = partial(
    pa.Field,
    description="Longitude in decimal degrees",
    in_range={"min_value": -180.0, "max_value": 180.0},
)

Iso3166CountryField = partial(
    pa.Field,
    description="ISO 3166-1 alpha-2 country code",
    str_matches=r"^[A-Z]{2}$",
)

ArrowDate32Field = partial(
    pa.Field,
    description="Date in pyarrow.date32 format",
    dtype_kwargs={"pyarrow_dtype": pyarrow.date32()},
)

EthAddressField = partial(
    pa.Field, description="Ethereum address string", str_matches=r"0x[a-fA-F0-9]{40}"
)

HttpUrlField = partial(
    pa.Field, description="HTTP URL string", str_matches=r"https?://[^\s]+"
)

IpfsHashField = partial(
    pa.Field, description="IPFS hash string", str_matches=r"Qm[a-zA-Z0-9]{44}"
)

QueryIdField = partial(
    pa.Field, description="Query ID string", str_matches=r"[a-f0-9]{16}-[A-Z]{3}"
)
