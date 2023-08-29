"""Module containing utilities and helper methods regarding xml."""

import copy
import sys
from typing import Callable

from lxml.etree import _Element

if sys.version_info >= (3, 10):
    from typing import TypeAlias

    LxmlElement: TypeAlias = _Element
else:
    from typing_extensions import TypeAlias

    LxmlElement = _Element


def copy_element(node: LxmlElement,
                 method: Callable[[LxmlElement], LxmlElement] = copy.deepcopy) -> LxmlElement:
    """Copy and preserve complete namespace.

    :param node: node to be copied
    :param method: method that creates a duplication of the root node
    :return: new node
    """
    # walk from target to root
    current = node
    ns_map_list: list[dict[str, str]] = []  # saves all namespaces
    while current is not None:
        ns_map_list.append({k: v for k, v in current.nsmap.items() if k})  # filter for default namespace
        current = current.getparent()  # type: ignore[assignment]

    # create new instance
    root_tree = node.getroottree()
    current = method(root_tree.getroot())
    x_path_steps = root_tree.getpath(node).split('/')[1:]
    assert len(x_path_steps) == len(ns_map_list)

    # walk from root to target
    ns_map_list.reverse()
    for i, step in enumerate(x_path_steps):
        current = current.xpath(f'/{step}' if i == 0 else step, namespaces=ns_map_list[i])[0]
    return current
