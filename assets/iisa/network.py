"""
A module that provides classes to represent the graph network of indexers and their indexed subgraphs.

.. note::
    The classes in this module are meant to be used as data transfer objects (DTOs) to represent the graph network
    of indexers and their indexed subgraphs. The classes are not meant to be used as domain objects.
"""

from typing import Dict, Iterable, List, Optional, cast

import pandera as pa
from pandera.typing import DataFrame, Series

from .geoip import GeoipResolver
from .typing import (
    DeploymentId,
    EthAddressField,
    HttpUrlField,
    HttpUrlStr,
    IndexerId,
    IpV4AddressField,
    Iso3166CountryField,
    LatitudeField,
    LongitudeField,
    SubgraphId,
)


class IndexerSubgraphVersion:
    """A subgraph version associated with an indexer.

    Represents a tuple of a subgraph version number and the deployment Id of the indexed subgraph version.
    """

    def __init__(self, version: int, deployment_id: DeploymentId) -> None:
        """Initializes a new instance of the SubgraphVersion class.

        :param version: The subgraph version number.
        :param deployment_id: The deployment Id of the subgraph version.
        """
        self._version = version
        self._deployment_id = deployment_id

    @property
    def version(self) -> int:
        """
        The subgraph version number.

        Subgraph versions are monotonically increasing integers that are assigned to a subgraph deployment.

        :returns: The subgraph version number.
        :rtype: int
        """
        return self._version

    @property
    def deployment_id(self) -> DeploymentId:
        """
        The deployment ID of the subgraph version.

        The deployment ID is a unique identifier (a IPFS CID) that is assigned to a subgraph deployment.

        :return: The deployment ID of the subgraph version.
        """
        return self._deployment_id


class IndexerSubgraph:
    """
    A subgraph associated with an indexer.

    Represents a subgraph associated with an indexer and with a unique subgraph Id and a list of subgraph versions.
    """

    def __init__(
        self,
        subgraph_id: SubgraphId,
        versions: List[IndexerSubgraphVersion],
    ) -> None:
        """
        Initializes a new instance of the Subgraph class.

        .. warning::
            When instantiating a subgraph, the provided list of subgraph versions must be non-empty and ordered by
            version number in ascending order.

        :param subgraph_id: The unique subgraph ID.
        :param versions: The list of subgraph versions.
        """
        self._id = subgraph_id
        self._versions = versions

    @property
    def id(self) -> SubgraphId:
        """
        The unique subgraph ID.

        :returns: The subgraph ID.
        """
        return self._id

    @property
    def versions(self) -> List[IndexerSubgraphVersion]:
        """
        The list of subgraph versions.

        :returns: The list of subgraph versions.
        """
        return self._versions


class Indexer:
    """
    An indexer.

    Represents an indexer with a unique indexer Id, a URL, and a list of indexed subgraphs.
    """

    def __init__(
        self,
        indexer_id: IndexerId,
        url: HttpUrlStr,
        subgraphs: Dict[SubgraphId, IndexerSubgraph],
    ) -> None:
        """
        Initializes a new instance of the Indexer class.

        :param indexer_id: The unique indexer ID.
        :param url: The URL of the indexer.
        :param subgraphs: The list of subgraphs.
        """
        self._id = indexer_id
        self._url = url
        self._subgraphs = subgraphs

    @property
    def id(self) -> IndexerId:
        """
        The unique indexer ID.

        :returns: The indexer ID.
        """
        return self._id

    @property
    def url(self) -> HttpUrlStr:
        """
        The URL of the indexer.

        :returns: The URL of the indexer.
        """
        return self._url

    @property
    def subgraphs(self) -> Dict[SubgraphId, IndexerSubgraph]:
        """
        The list of subgraphs.

        :returns: The subgraphs list.
        """
        return self._subgraphs


class IndexersSchema(pa.DataFrameModel):
    """A schema for validating the indexers dataframe"""

    indexer: Series[str] = EthAddressField(
        description="The indexer's Ethereum address", unique=True
    )
    url: Series[str] = HttpUrlField(description="The indexer's URL")
    indexer_network: Series[str] = pa.Field(isin=["arbitrum"])

    # Resolved IP address and geolocation information
    ip_addr: Series[str] = IpV4AddressField(
        description="The indexer's IP address", nullable=True
    )
    org: Series[str] = pa.Field(
        description="The organization name of the indexer's IP address", nullable=True
    )
    country: Series[str] = Iso3166CountryField(
        description="The country code of the indexer's IP address geolocation",
        nullable=True,
    )
    latitude: Series[float] = LatitudeField(
        description="The latitude (decimal) of the indexer's IP address geolocation",
        nullable=True,
    )
    longitude: Series[float] = LongitudeField(
        description="The longitude (decimal) of the indexer's IP address geolocation",
        nullable=True,
    )


IndexersDataFrame = DataFrame[IndexersSchema]


class NetworkProvider:
    """
    The Graph network information provider.

    The network provider is responsible for holding the network information snapshot, which includes the list of
    indexers and their indexed subgraphs. If the network information snapshot is not available, the provider will raise
    a ValueError when attempting to access the network information.
    """

    def __init__(self, geoip: GeoipResolver) -> None:
        """Initializes a new instance of the NetworkProvider class."""
        self._geoip = geoip

        self._indexers: Optional[IndexersDataFrame] = None

    def set_snapshot(
        self,
        indexers: Iterable[Indexer],
    ) -> None:
        """
        Updates the network snapshot with the provided indexers and subgraphs.

        .. note::
            This method is meant to be called by the Rust host to update the network snapshot with the latest
            network subgraph retrieval results.

        :param indexers: The list of indexers.
        """
        self._indexers = _to_indexers_dataframe(indexers, geoip=self._geoip)

    def indexers(self) -> IndexersDataFrame:
        """
        The list of indexers.

        :returns: The indexers dataframe.
        """
        if self._indexers is None:
            raise ValueError("Network snapshot not available")

        return self._indexers


def _to_indexers_dataframe(
    indexers: Iterable[Indexer],
    *,
    geoip: GeoipResolver,
) -> IndexersDataFrame:
    """
    Converts a list of indexers to a Pandas DataFrame.

    :param indexers: The list of indexers.
    :param geoip: The GeoipResolver instance.
    :param day_partition: The date to set the "day-partition" column to. If None, the current date is used.
    :return: The indexers DataFrame.
    """
    indexers_df: DataFrame = DataFrame(
        {
            "indexer": [indexer.id for indexer in indexers],
            "url": [indexer.url for indexer in indexers],
        }
    )

    # Set network columns to 'arbitrum'
    indexers_df["indexer_network"] = "arbitrum"

    # Resolve the IP address and geolocation information for each indexer
    indexers_df[["ip_addr", "org", "country", "latitude", "longitude"]] = indexers_df[
        "url"
    ].apply(lambda url: Series(geoip.resolve_url_host_info(url)))

    return cast(IndexersDataFrame, indexers_df)
