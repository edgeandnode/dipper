"""
IISA-specific type hints and Pandera schema field factories.
"""

from functools import partial
from typing import NewType

import pandera as pa

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
