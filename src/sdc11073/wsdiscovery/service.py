from __future__ import annotations

from typing import List, Optional
from urllib.parse import urlsplit

from lxml.etree import QName

from ..netconn import get_ipv4_addresses
from ..xml_types import wsd_types


class Service:
    def __init__(self, types: Optional[List[QName]],
                 scopes: Optional[wsd_types.ScopesType],
                 x_addrs: list[str],
                 epr: str,
                 instance_id: str,
                 metadata_version=1):
        self.types = types
        if scopes is not None:
            assert isinstance(scopes, wsd_types.ScopesType)
        self.scopes = scopes
        self._x_addrs = x_addrs or []
        self.epr = epr
        self.instance_id = instance_id
        self.message_number = 0
        self.metadata_version = metadata_version

    def get_x_addrs(self) -> list[str]:
        ret = []
        ip_addrs = None
        for x_addr in self._x_addrs:
            if '{ip}' in x_addr:
                if ip_addrs is None:
                    ip_addrs = get_ipv4_addresses()
                for ip_addr in ip_addrs:
                    ret.append(x_addr.format(ip=ip_addr))
            else:
                ret.append(x_addr)
        return ret

    def set_x_addrs(self, x_addrs):
        self._x_addrs = x_addrs

    def increment_message_number(self):
        self.message_number = self.message_number + 1

    def is_located_on(self, *ip_addresses):
        """
        :param ip_addresses: ip addresses, lists of strings or strings
        """
        my_addresses = []
        for ip_address in ip_addresses:
            if isinstance(ip_address, str):
                my_addresses.append(ip_address)
            else:
                my_addresses.extend(ip_address)
        for addr in self.get_x_addrs():
            parsed = urlsplit(addr)
            ip_addr = parsed.netloc.split(':')[0]
            if ip_addr in my_addresses:
                return True
        return False

    def __repr__(self):
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id} Xaddr={self._x_addrs} ' \
               f'scopes={scopes_str} types={types_str}'

    def __str__(self):
        scopes_str = 'None' if self.scopes is None else ', '.join([str(x) for x in self.scopes.text])
        types_str = 'None' if self.types is None else ', '.join([str(x) for x in self.types])
        return f'Service epr={self.epr}, instanceId={self.instance_id}\n' \
               f'   Xaddr={self._x_addrs}\n' \
               f'   scopes={scopes_str}\n' \
               f'   types={types_str}'
