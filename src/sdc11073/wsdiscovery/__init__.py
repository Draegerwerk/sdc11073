# noqa: D104
from sdc11073.xml_types.wsd_types import ScopesType  # make ScopesType visible in wsdiscovery for convenience

from .wsdimpl import (
    WSDiscovery,
    WSDiscoverySingleAdapter,
)

__all__ = ['WSDiscovery', 'WSDiscoverySingleAdapter', 'ScopesType']
