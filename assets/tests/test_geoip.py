"""
Test suite covering the geoip module.
"""

from iisa.geoip import (
    _get_ipaddr_location_info,
    _get_url_host,
    _resolve_host_ipaddr,
    _IpAddressStr,
    _UrlHostStr,
    GeoipResolver,
)
from iisa.typing import HttpUrlStr


class TestResolveUrlHostIpaddr:
    def test_get_host_from_url(self):
        ## Given
        url = HttpUrlStr("https://thegraph.com")

        ## When
        result = _get_url_host(url)

        ## Then
        assert result == "thegraph.com"

    def test_get_host_from_ipaddr_url(self):
        ## Given
        host = HttpUrlStr("https://192.168.0.1:8080/index.html")

        ## When
        result = _get_url_host(host)

        ## Then
        assert result == "192.168.0.1"

    def test_get_hot_from_url_with_no_host(self):
        ## Given
        host = HttpUrlStr("https://")

        ## When
        result = _get_url_host(host)

        ## Then
        assert result is None

    def test_resolve_ipaddr_from_host_str(self):
        ## Given
        # DNS domain that resolves to localhost (127.0.0.1)
        host = _UrlHostStr("localtest.me")

        ## When
        result = _resolve_host_ipaddr(host)

        ## Then
        assert result == "127.0.0.1"

    def test_resolve_ipaddr_from_host_str_with_no_resolution(self):
        ## Given
        # Invalid hostname
        host = _UrlHostStr("invalid-hostname.local")

        ## When
        result = _resolve_host_ipaddr(host)

        ## Then
        assert result is None


class TestGetIpaddrLocationInfo:
    def test_get_geolocation_info_for_ipaddr(self):
        ## Given
        # Use one of the Google APIs US-east URLs to ensure it's a US-based IP address
        url = HttpUrlStr("https://storage.us-east1.rep.googleapis.com")

        # Resolve the hostname to an IP address
        host = _get_url_host(url)
        ipaddr = _resolve_host_ipaddr(host)

        ## When
        result = _get_ipaddr_location_info(ipaddr)

        ## Then
        assert result["ip_addr"] == ipaddr
        assert result["org"] == "AS19527 Google LLC"
        assert result["latitude"] != "Unknown"
        assert result["longitude"] != "Unknown"
        assert result["country"] == "US"

    def test_get_geolocation_info_for_private_address(self):
        ## Given
        # Private IP address
        ipaddr = _IpAddressStr("192.168.0.1")

        ## When
        result = _get_ipaddr_location_info(ipaddr)

        ## Then
        assert result["ip_addr"] == ipaddr
        assert result["org"] == "Unknown"
        assert result["latitude"] == "Unknown"
        assert result["longitude"] == "Unknown"
        assert result["country"] == "Unknown"


class TestGeoipResolver:
    def test_resolve_url_host_info(self):
        ## Given
        url = HttpUrlStr("https://dns.google")

        geoip = GeoipResolver()

        ## When
        result = geoip.resolve_url_host_info(url)

        ## Then
        # Assert the IP address and geolocation information is returned
        assert result["ip_addr"] == "8.8.4.4"
        assert result["org"] == "AS15169 Google LLC"
        assert result["latitude"] != "Unknown"
        assert result["longitude"] != "Unknown"
        assert result["country"] == "US"

        # Assert caches are no longer empty
        assert geoip._host_ipaddr_cache_entries() == 1
        assert geoip._ipinfo_cache_entries() == 1

    def test_resolve_url_host_info_with_no_host_url(self):
        ## Given
        url = HttpUrlStr("https://")

        geoip = GeoipResolver()

        ## When
        result = geoip.resolve_url_host_info(url)

        ## Then
        # Assert no information is returned
        assert result["ip_addr"] == "Unknown"
        assert result["org"] == "Unknown"
        assert result["latitude"] == "Unknown"
        assert result["longitude"] == "Unknown"
        assert result["country"] == "Unknown"

        # Assert cache is empty
        assert geoip._host_ipaddr_cache_entries() == 0
        assert geoip._ipinfo_cache_entries() == 0

    def test_resolve_url_host_info_with_non_resolvable_host(self):
        ## Given
        url = HttpUrlStr("https://invalid-hostname.local")

        geoip = GeoipResolver()

        ## When
        result = geoip.resolve_url_host_info(url)

        ## Then
        # Assert no information is returned
        assert result["ip_addr"] == "Unknown"
        assert result["org"] == "Unknown"
        assert result["latitude"] == "Unknown"
        assert result["longitude"] == "Unknown"
        assert result["country"] == "Unknown"

        # Assert cache is empty
        assert geoip._host_ipaddr_cache_entries() == 0
        assert geoip._ipinfo_cache_entries() == 0

    def test_resolve_url_host_info_with_private_ipaddr(self):
        ## Given
        # Resolve the hostname to the localhost IP address
        url = HttpUrlStr("https://localtest.me")

        geoip = GeoipResolver()

        ## When
        result = geoip.resolve_url_host_info(url)

        ## Then
        # Assert the localhost IP address is returned, but no geolocation information
        assert result["ip_addr"] == "127.0.0.1"
        assert result["org"] == "Unknown"
        assert result["latitude"] == "Unknown"
        assert result["longitude"] == "Unknown"
        assert result["country"] == "Unknown"

        # Assert cache is not empty
        assert geoip._host_ipaddr_cache_entries() == 1
        assert geoip._ipinfo_cache_entries() == 1
