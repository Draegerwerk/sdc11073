"""Base classes for declaring XML data types used throughout the library."""

from __future__ import annotations

import enum
import inspect
import traceback
import typing
from collections.abc import Sequence
from math import isclose
from typing import TYPE_CHECKING

from lxml import etree

from .xml_structure import NodeStringProperty, NodeTextListProperty

if TYPE_CHECKING:
    from sdc11073 import xml_utils


class StringEnum(str, enum.Enum):
    """Enum whose members are strings and stringify to their value."""

    def __str__(self) -> str:
        return str(self.value)


class XMLTypeBase:
    """Base class used to declare XML data types, supporting nesting of data and inheritance.

    It uses xml_structure elements to declare the members.
    Because order matters in XML, the _props member is needed that lists all members that
    represent XML data in the correct order.
    Usage:
    - object creation: All derived classes have a constructor without arguments.
    - initializing from XML: class method 'from_node'
    """

    # This class defines __eq__ but its instances are mutable, so they remain unhashable
    # (matching Python's default behaviour of setting __hash__ to None when __eq__ is defined).
    __hash__ = None

    def __init__(self):
        for _, prop in self.sorted_container_properties():
            prop.init_instance_data(self)

    def as_etree_node(
        self,
        q_name: etree.QName,
        ns_map: dict,
        parent_node: etree.Element | None = None,
    ) -> xml_utils.LxmlElement:
        """Serialize this object into an lxml element and return it.

        :param q_name: the qualified name of the element to create
        :param ns_map: the namespace map for the created element
        :param parent_node: optional parent to which the created element is appended
        :return: the created lxml element
        """
        if parent_node is not None:
            node = etree.SubElement(parent_node, q_name, nsmap=ns_map)
        else:
            node = etree.Element(q_name, nsmap=ns_map)
        self.update_node(node)
        return node

    def update_node(self, node: xml_utils.LxmlElement) -> None:
        """Write the values of all container properties into the given node.

        :param node: the lxml element to update
        """
        for prop_name, prop in self.sorted_container_properties():
            try:
                prop.update_xml_value(self, node)
            except Exception as ex:  # noqa: PERF203
                # re-raise with some information about the data
                msg = f'In {self.__class__.__name__}.{prop_name}, {prop!s} could not update: {traceback.format_exc()}'
                raise ValueError(msg) from ex

    def update_from_node(self, node: xml_utils.LxmlElement) -> None:
        """Read the values of all container properties from the given node.

        :param node: the lxml element to read from
        """
        for _, prop in self.sorted_container_properties():
            prop.update_from_node(self, node)

    def sorted_container_properties(self) -> list[tuple[str, typing.Any]]:
        """Return a list of (name, object) tuples of all GenericProperties (and subclasses).

        The list is created based on the _props lists of the classes.

        :return: a list of (name, property object) tuples in XML order
        """
        ret = []
        classes = inspect.getmro(self.__class__)
        for cls in reversed(classes):
            try:
                names = cls.__dict__['_props']  # this checks only current class, not parent
            except (AttributeError, KeyError):
                continue
            for name in names:
                obj = getattr(cls, name)
                if obj is not None:
                    ret.append((name, obj))
        return ret

    def __eq__(self, other: XMLTypeBase):
        """Compare all properties."""
        try:
            for name, _ in self.sorted_container_properties():
                my_value = getattr(self, name)
                other_value = getattr(other, name)
                if my_value == other_value:
                    continue
                if (isinstance(my_value, float) or isinstance(other_value, float)) and isclose(my_value, other_value):
                    continue  # float compare (almost equal)
                return False
            return True  # noqa: TRY300
        except (TypeError, AttributeError):
            return False

    def __ne__(self, other: XMLTypeBase):
        return not self == other

    def __repr__(self):
        return f'{self.__class__.__name__}({self.sorted_container_properties()})'

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement) -> XMLTypeBase:
        """Default from_node Constructor that provides no arguments for class __init__."""
        obj = cls()
        obj.update_from_node(node)
        return obj

    @classmethod
    def value_class_from_node(cls, _) -> type[typing.Self]:  # noqa: ANN001
        """Return the concrete value class for deserialization from an XML node."""
        return cls


class ElementWithText(XMLTypeBase):
    """An Element with text.

    It is different form NodeTextProperty in two aspects:
    - access to text via "text" member, it is not the property value itself.
    - It can be extended with Attributes
    """

    NODETYPE = None
    text: str | None = NodeStringProperty()  # this is the text of the node. Here attribute is lower case!
    _props = ('text',)

    def __init__(self, text: str | None = None):
        super().__init__()
        self.text = text


class ElementWithTextList(XMLTypeBase):
    """An Element with text, which is alist of words(string without whitespace)."""

    # this is the text list of the node. Here attribute is lower case!
    text: Sequence[str] = NodeTextListProperty(sub_element_name=None, value_class=str)
    _props = ('text',)


class MessageType(XMLTypeBase):
    """Base for all classes that are used as the body of a soap envelope.

    All derived classes must set these values.
    NODETYPE defines the qualified name of the Element, action is used for the action element
    in the soap header.
    """

    NODETYPE = None
    action = None
    additional_namespaces = ()  # derived class list namespaces other than PM and MSG
