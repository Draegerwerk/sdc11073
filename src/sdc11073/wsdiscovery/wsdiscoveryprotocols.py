"""WS discovery interface definition."""

from typing import Protocol

from sdc11073.xml_types.wsd_types import ScopesType


class WsDiscoveryProtocol(Protocol):
    """The WS discovery interface."""

    def publish_service(self, epr: str, types: list, scopes: ScopesType, x_addrs: list):  # noqa: D102
        ...

    @property
    def active_address(self) -> str:  # noqa: D102
        ...

    def clear_service(self, epr: str):  # noqa: D102
        ...
