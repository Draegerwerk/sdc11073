from sdc11073.wsdiscovery.service import Service
from sdc11073.xml_types.wsd_types import ScopesType  # make ScopesType visible in wsdiscovery for convenience

from .wsdimpl import (
    WSDiscovery,
    WSDiscoverySingleAdapter,
)

__all__ = ['ScopesType', 'Service', 'WSDiscovery', 'WSDiscoverySingleAdapter']
