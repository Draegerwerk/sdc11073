from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.xml_types import wsd_types

if TYPE_CHECKING:
    from lxml.etree import QName


class Service:
    """Service objects contain discovery relevant data of a service.

    They are used for publishing a service or as result (list) when searching for services in the network.
    """

    def __init__(self,
                 types: list[QName] | None,
                 scopes: wsd_types.ScopesType | None,
                 x_addrs: list[str] | None,
                 epr: str,
                 instance_id: str,
                 metadata_version=1):
        self.types = types
        if scopes is not None:
            assert isinstance(scopes, wsd_types.ScopesType)
        self.scopes = scopes
        self._x_addrs = x_addrs
        self.epr = epr
        self.instance_id = instance_id
        self.message_number = 0
        self.metadata_version = metadata_version

    def get_x_addrs(self) -> list[str]:
        """Get the addresses of the service."""
        return self._x_addrs or []

    def set_x_addrs(self, x_addrs: list[str]) -> None:
        """Set the addresses of the service."""
        self._x_addrs = x_addrs

    def increment_message_number(self) -> None:
        """Add one."""
        self.message_number += 1

    def __repr__(self) -> str:
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id} Xaddr={self._x_addrs} ' \
               f'scopes={scopes_str} types={types_str}'

    def __str__(self) -> str:
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id}\n' \
               f'   Xaddr={self._x_addrs}\n' \
               f'   scopes={scopes_str}\n' \
               f'   types={types_str}'
