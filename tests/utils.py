from __future__ import annotations

import platform
import random
import string
import uuid

import lxml.etree

from sdc11073 import location, network
from sdc11073.xml_types import wsd_types

RFC3986 = string.ascii_letters + string.digits + '-_.~'


def get_random_RFC3986_string_of_length(length_of_string: int,  # noqa: N802
                                        characters_to_exclude: str | None = None) -> str:
    """Create a random string containing characters of the "unreserved" RFC3986 set.

    @param length_of_string: length of the generated string
    @param characters_to_exclude: string of characters to be excluded from selection
    @return: return a random string which has the given length
    """
    rfc3986_strings = set(RFC3986) - set(characters_to_exclude or [])
    return ''.join(random.choices(list(rfc3986_strings), k=length_of_string))


def random_location() -> location.SdcLocation:
    """Create a random location."""
    return location.SdcLocation(fac=get_random_RFC3986_string_of_length(7),
                                poc=get_random_RFC3986_string_of_length(7),
                                bed=get_random_RFC3986_string_of_length(7),
                                bldng=get_random_RFC3986_string_of_length(7),
                                flr=get_random_RFC3986_string_of_length(7),
                                rm=get_random_RFC3986_string_of_length(7))


def random_qname() -> lxml.etree.QName:
    """Create random qname."""
    return lxml.etree.QName(f'{"".join(random.choices(list(string.ascii_letters), k=1))}{uuid.uuid4().hex}',
                            f'{"".join(random.choices(list(string.ascii_letters), k=1))}{uuid.uuid4().hex}')


def random_scope() -> wsd_types.ScopesType:
    """Create random scope."""
    return wsd_types.ScopesType(random_location().scope_string)


def get_network_adapter_for_testing() -> network.NetworkAdapter:
    """Get a network adapter for testing."""
    adapters = network.get_adapters()
    if platform.system() == 'Darwin':
        # macOS restricts sending multicast traffic from a loopback interface
        return [adapter for adapter in adapters if not adapter.is_loopback][0]
    else:
        return [adapter for adapter in adapters if adapter.is_loopback][0]
