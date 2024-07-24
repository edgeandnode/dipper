import os
import socket
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import airportsdata
import maxminddb
import pandas as pd
from pandas import Series


def _load_ip_addr_location_db() -> maxminddb.Reader:
    """
    Load the IP address location database.

    Use the GeoLite2-City database from MaxMind to get the location details of an IP address without depending on an
    external API.
    """
    db_path = Path(os.path.dirname(__file__)) / "__assets__" / "GeoLite2-City.mmdb"
    return maxminddb.open_database(db_path)


__GEOIP_ADDR_DB__ = _load_ip_addr_location_db()
__AIRPORTS_DATA_IATA__ = airportsdata.load('IATA')


def extract_location_and_details(url) -> Series:
    """
    This function extracts location and details from a URL by resolving it to an IP address.

    Parameters:
    url (str): The URL to be resolved.

    Returns:
    pd.Series: A pandas Series containing location details.
    """
    ip = _resolve_ip_address(url)
    return _get_location_and_details_from_ip(ip)


def get_location_and_details_from_iata(iata_code: str) -> Series:
    """
    Get location and other details from an IATA Code.

    Parameters:
    iata (str): The IATA code to get details for.

    Returns:
    pd.Series: A pandas Series containing latitude, longitude, and country.
    """
    if iata_code is None:
        return Series({"latitude": None, "longitude": None, "country": None})

    try:
        iata_info = __AIRPORTS_DATA_IATA__[iata_code]
    except KeyError:
        return Series({"latitude": None, "longitude": None, "country": None})

    return Series(
        {
            "latitude": float(iata_info.get("lat", None)),
            "longitude": float(iata_info.get("lon", None)),
            "country": iata_info.get("country", None),
        }
    )


def _get_location_and_details_from_ip(ip: Optional[str]) -> Series:
    """
    This function gets location and other details from an IP address.

    Parameters:
    ip (str): The IP address to be resolved to location details.

    Returns:
    dict: A dictionary containing location/other details.
    """
    if ip is None:
        return Series({"location": "Unknown", "loc": "Unknown", "ip": "Unknown"})

    # Get the location data for the IP address
    try:
        data = __GEOIP_ADDR_DB__.get(ip)
    except ValueError:
        return Series({"location": "Unknown", "loc": "Unknown", "ip": "Unknown"})

    # Extract the location details
    try:
        country = data["country"]["iso_code"]
    except ValueError:
        country = "Unknown"

    try:
        region = data["subdivisions"][0]["names"]["en"]
    except ValueError:
        region = "Unknown"

    try:
        location = f"{data['location']['latitude']},{data['location']['longitude']}"
    except ValueError:
        location = "Unknown"

    return Series({
        "location": f'{country}, {region}',
        "loc": location,
        "ip": ip,
    })


def _resolve_ip_address(url: Optional[str]) -> Optional[str]:
    """
    Resolve URL host IP address.

    If the URL is invalid or the IP address resolution fails, `None` is returned.
    """
    if pd.isna(url) or not isinstance(url, str):
        return None

    # Parse the URL to get the hostname
    try:
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
    except ValueError:
        return None

    if hostname is None:
        return None

    # Resolve the hostname to an IP address
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None
