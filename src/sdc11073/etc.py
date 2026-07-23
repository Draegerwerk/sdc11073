"""Small helper functions shared across the sdc11073 library."""

from collections.abc import Callable, Iterable, Sequence
from typing import Any, TypeVar

R = TypeVar('R')


def apply_map(function: Callable[..., R], *iterable: Iterable[Any]) -> Sequence[R]:
    """Call function for all elements of iterable(s).

    apply_map uses builtin map internally.
    """
    return list(map(function, *iterable))
