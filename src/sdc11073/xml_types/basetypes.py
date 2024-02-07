from __future__ import annotations

import enum
import inspect
import traceback
from math import isclose
from typing import TYPE_CHECKING

from lxml import etree as etree_

from .xml_structure import NodeStringProperty, NodeTextListProperty

if TYPE_CHECKING:
    from sdc11073 import xml_utils


class StringEnum(str, enum.Enum):

    def __str__(self):
        return str(self.value)


class XMLTypeBase:
    """ Base class that it used to declare XML data types. It supports nesting of data and inheritance.
    It uses xml_structure elements to declare the members.
    Because order matters in XML, the _props member is needed that lists all members that
    represent XML data in the correct order.
    Usage:
    - object creation: All derived classes have a constructor without arguments.
    - initializing from XML: class method 'from_node'
    -
    """

    def __init__(self):
        for _, prop in self.sorted_container_properties():
            prop.init_instance_data(self)

    def as_etree_node(self, q_name: etree_.QName, ns_map: dict, parent_node: etree_.Element | None = None):
        if parent_node is not None:
            node = etree_.SubElement(parent_node, q_name, nsmap=ns_map)
        else:
            node = etree_.Element(q_name, nsmap=ns_map)
        self.update_node(node)
        return node

    def update_node(self, node: xml_utils.LxmlElement):
        for prop_name, prop in self.sorted_container_properties():
            try:
                prop.update_xml_value(self, node)
            except Exception as ex:
                # re-raise with some information about the data
                raise ValueError(
                    f'In {self.__class__.__name__}.{prop_name}, {prop!s} could not update: {traceback.format_exc()}') from ex

    def update_from_node(self, node: xml_utils.LxmlElement):
        for dummy, prop in self.sorted_container_properties():
            prop.update_from_node(self, node)

    def sorted_container_properties(self):
        """
        @return: a list of (name, object) tuples of all GenericProperties ( and subclasses)
        list is created based on _props lists of classes
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

    def __eq__(self, other):
        """ compares all properties"""
        try:
            for name, dummy in self.sorted_container_properties():
                my_value = getattr(self, name)
                other_value = getattr(other, name)
                if my_value == other_value:
                    continue
                if (isinstance(my_value, float) or isinstance(other_value, float)) and isclose(my_value, other_value):
                    continue  # float compare (almost equal)
                return False
            return True
        except (TypeError, AttributeError):
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return f'{self.__class__.__name__}({self.sorted_container_properties()})'

    @classmethod
    def from_node(cls, node):
        """ default from_node Constructor that provides no arguments for class __init__"""
        obj = cls()
        obj.update_from_node(node)
        return obj

    @classmethod
    def value_class_from_node(cls, _):
        return cls


class ElementWithText(XMLTypeBase):
    """An Element with text. It is different form NodeTextProperty in two aspects:
    - access to text via "text" member, it is not the property value itself.
    - It can be extended with Attributes
    """
    NODETYPE = None
    text: str = NodeStringProperty()  # this is the text of the node. Here attribute is lower case!
    _props = ('text',)

    def __init__(self, text=None):
        super().__init__()
        self.text = text


class ElementWithTextList(XMLTypeBase):
    """An Element with text, which is alist of words(string without whitespace).
    """
    # this is the text list of the node. Here attribute is lower case!
    text: list[str] = NodeTextListProperty(sub_element_name=None,
                                           value_class=str)
    _props = ('text',)


class MessageType(XMLTypeBase):
    """This is the base for all classes that are used as the body of a soap envelope.
    All derived classes must set these values.
    NODETYPE defines the qualified name of the Element, action is used for the action element
    in the soap header."""
    NODETYPE = None
    action = None
    additional_namespaces = ()  # derived class list namespaces other than PM and MSG
