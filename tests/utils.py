"""Test utilities."""

from __future__ import annotations

import math
import random
import string
import uuid
from typing import TYPE_CHECKING

from lxml import etree

from sdc11073 import location
from sdc11073.xml_types import wsd_types

if TYPE_CHECKING:
    from sdc11073.mdib.containerbase import ContainerBase

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


def random_qname(*, namespace: str | None = None, localname: str | None = None) -> etree.QName:
    """Create random qname."""
    return etree.QName(namespace or random_qname_part(), localname or random_qname_part())


def random_scope() -> wsd_types.ScopesType:
    """Create random scope."""
    return wsd_types.ScopesType(random_location().scope_string)


def container_diff(
    first: ContainerBase,
    second: ContainerBase,
    max_float_diff: float = 1e-15,
) -> None | list[str]:
    """Compare all properties (except to be ignored ones).

    @param first: the first object to compare
    @param second: the second object to compare
    @param max_float_diff: parameter for math.isclose() if float values are incorporated.
                            1e-15 corresponds to 15 digits max. accuracy (see sys.float_info.dig)
    @return: textual representation of differences or None if equal
    """
    ret = []
    first_properties = first.sorted_container_properties()

    first_property_names = {p[0] for p in first_properties}
    second_property_names = {p[0] for p in second.sorted_container_properties()}
    surplus_names = first_property_names.symmetric_difference(second_property_names)
    if surplus_names:
        ret.append(f'objects differ by their properties: {surplus_names}')
    if ret:
        return ret

    for name, _ in first_properties:
        first_value = getattr(first, name)
        second_value = getattr(second, name)
        if first_value != second_value:
            if isinstance(first_value, float) or isinstance(second_value, float):
                if not math.isclose(first_value, second_value, rel_tol=max_float_diff, abs_tol=max_float_diff):
                    ret.append(f'{name}={first_value}, second={second_value}')
            else:
                ret.append(f'{name}={first_value}, second={second_value}')

    return None if len(ret) == 0 else ret
