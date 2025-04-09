"""
IISA-specific type hints
"""

from typing import NewType

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

EthAddressStr = NewType("EthAddressStr", str)
HttpUrlStr = NewType("HttpUrlStr", str)
IpfsHashStr = NewType("IpfsHashStr", str)
QueryIdStr = NewType("QueryIdStr", str)

IndexerId = EthAddressStr
DeploymentId = IpfsHashStr
SubgraphId = NewType("SubgraphId", str)
