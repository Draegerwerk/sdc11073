"""Test utilities."""

from __future__ import annotations

import random
import string
import uuid

from lxml import etree

from sdc11073 import location
from sdc11073.xml_types import wsd_types

RFC3986 = string.ascii_letters + string.digits + '-_.~'


def get_random_rfc3986_string_of_length(
    length_of_string: int,
    characters_to_exclude: str | None = None,
) -> str:
    """Create a random string containing characters of the "unreserved" RFC3986 set.

    @param length_of_string: length of the generated string
    @param characters_to_exclude: string of characters to be excluded from selection
    @return: return a random string which has the given length
    """
    rfc3986_strings = set(RFC3986) - set(characters_to_exclude or [])
    return ''.join(random.choices(list(rfc3986_strings), k=length_of_string))


def random_location() -> location.SdcLocation:
    """Create a random location."""
    return location.SdcLocation(
        fac=get_random_rfc3986_string_of_length(7),
        poc=get_random_rfc3986_string_of_length(7),
        bed=get_random_rfc3986_string_of_length(7),
        bldng=get_random_rfc3986_string_of_length(7),
        flr=get_random_rfc3986_string_of_length(7),
        rm=get_random_rfc3986_string_of_length(7),
    )


def random_qname_part() -> str:
    """Create random qname part."""
    return f'{"".join(random.choices(list(string.ascii_letters), k=1))}{uuid.uuid4().hex}'


def random_qname(*, localname: str | None = None, namespace: str | None = None) -> etree.QName:
    """Create random qname."""
    return etree.QName(localname or random_qname_part(), namespace or random_qname_part())


def random_scope() -> wsd_types.ScopesType:
    """Create random scope."""
    return wsd_types.ScopesType(random_location().scope_string)
