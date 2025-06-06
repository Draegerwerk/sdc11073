from sdc11073.xml_types.wsd_types import ScopesType  # make ScopesType visible in wsdiscovery for convenience

from .wsdimpl import (
    WSDiscovery,
    WSDiscoverySingleAdapter,
)
from sdc11073.wsdiscovery.service import Service

__all__ = ['WSDiscovery', 'WSDiscoverySingleAdapter', 'ScopesType', 'Service']
