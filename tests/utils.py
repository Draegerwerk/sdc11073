"""Test utilities."""

from __future__ import annotations

import math
import os
import random
import string
import time
import uuid
from typing import TYPE_CHECKING

from lxml import etree

from sdc11073 import location
from sdc11073.xml_types import wsd_types

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sdc11073.mdib.containerbase import ContainerBase

RFC3986 = string.ascii_letters + string.digits + '-_.~'

# below the ephemeral port ranges of Linux (32768+) and Windows (49152+), so it never collides with an outbound socket
WSD_BASE_PORT = 23702


def wsd_port() -> int:
    """Return the WS-Discovery multicast port for the current pytest-xdist worker.

    Each worker gets its own port, so that discovery traffic of parallel workers does not interfere.
    Without xdist the port of worker 'gw0' is used, so tests never bind the default port 3702.
    """
    worker = os.environ.get('PYTEST_XDIST_WORKER', 'gw0')
    return WSD_BASE_PORT + int(worker.removeprefix('gw'))


def wait_for(condition: Callable[[], bool], timeout: float, interval: float = 0.1) -> bool:
    """Poll condition until it is true or timeout expires.

    :return: True if condition became true, False on timeout
    """
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(interval)
    return True


def get_random_rfc3986_string_of_length(
    length_of_string: int,
    characters_to_exclude: str | None = None,
) -> str:
    """Create a random string containing characters of the "unreserved" RFC3986 set.

    :param length_of_string: length of the generated string
    :param characters_to_exclude: string of characters to be excluded from selection
    :return: return a random string which has the given length
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
) -> None | Sequence[str]:
    """Compare all properties (except to be ignored ones).

    :param first: the first object to compare
    :param second: the second object to compare
    :param max_float_diff: parameter for math.isclose() if float values are incorporated.
                            1e-15 corresponds to 15 digits max. accuracy (see sys.float_info.dig)
    :return: textual representation of differences or None if equal
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
                    ret.append(
                        f'Comparison with tolerance failed! '
                        f'{name}={first_value}, '
                        f'second={second_value}, '
                        f'rel_tol={max_float_diff}, '
                        f'abs_tol={max_float_diff}'
                    )
            else:
                ret.append(f'Direct comparison failed! {name}={first_value}, second={second_value}')

    return None if len(ret) == 0 else ret
