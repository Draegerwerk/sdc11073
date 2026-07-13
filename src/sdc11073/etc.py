"""Small helper functions shared across the sdc11073 library."""

from collections.abc import Callable, Iterable, Sequence
from typing import TypeVar

P = TypeVar('P')
R = TypeVar('R')

def apply_map(function: Callable[P, R], *iterable: P) -> Sequence[R]:
    """Call function for all elements of iterable(s).

    apply_map uses builtin map internally.
    """
    return list(map(function, *iterable))


def _short_action_string(action: str) -> str:
    """Return only the last 2 elements of the action."""
    elements = action.split('/')
    return '/'.join(elements[-2:])


def short_filter_string(actions: Iterable[str]) -> str:
    """Make shorter action strings for logging.

    :param actions: list of strings
    :return: a comma separated string of shortened names
    """
    return ', '.join([_short_action_string(a) for a in actions])
