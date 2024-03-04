from __future__ import annotations

import copy
import inspect
from typing import Any
import math
from lxml.etree import Element, SubElement, QName

from sdc11073 import observableproperties as properties
from sdc11073.namespaces import QN_TYPE, NamespaceHelper
from sdc11073 import xml_utils


class ContainerBase:
    """Common base class for descriptors and states."""

    NODETYPE: QName = None  # overwrite in derived classes! This is the BICEPS Type.
    node = properties.ObservableProperty()
    is_state_container = False
    is_descriptor_container = False

    # every class with container properties must provide a list of property names.
    # this list is needed to create sub elements in a certain order.
    # rule is : elements are sorted from this root class to derived class. Last derived class comes last.
    # Initialization is from left to right
    # This is according to the inheritance in BICEPS xml schema

    def __init__(self):
        self.node = None  # set in update_from_node
        for _, cprop in self.sorted_container_properties():
            cprop.init_instance_data(self)

    def get_actual_value(self, attr_name: str) -> Any:
        """Ignore default value and implied value, e.g. return None if value is not present in xml."""
        return getattr(self.__class__, attr_name).get_actual_value(self)

    def mk_node(self,
                tag: QName,
                ns_helper: NamespaceHelper,
                parent_node: xml_utils.LxmlElement | None = None,
                set_xsi_type: bool = False) -> xml_utils.LxmlElement:
        """Create an etree node from instance data.

        :param tag: tag of the newly created node
        :param ns_helper: namespaces.NamespaceHelper instance
        :param parent_node: optional parent node
        :param set_xsi_type: if True, adds Type attribute to node
        :return: etree node
        """
        ns_map = ns_helper.partial_map(ns_helper.PM,
                                       ns_helper.MSG,
                                       ns_helper.XSI)
        if parent_node is not None:
            node = SubElement(parent_node, tag, nsmap=ns_map)
        else:
            node = Element(tag, nsmap=ns_map)

        self.update_node(node, ns_helper, set_xsi_type)
        return node

    def update_node(self, node: xml_utils.LxmlElement,
                    ns_helper: NamespaceHelper,
                    set_xsi_type: bool = False) -> xml_utils.LxmlElement:
        """Update node with own data.

        :param node: node to be updated
        :param ns_helper: namespaces.NamespaceHelper instance
        :param set_xsi_type:if True, adds Type attribute to node
        :return: etree node
        """
        if set_xsi_type and self.NODETYPE is not None:
            node.set(QN_TYPE, ns_helper.doc_name_from_qname(self.NODETYPE))
        for _, prop in self.sorted_container_properties():
            prop.update_xml_value(self, node)
        return node

    def update_from_node(self, node: xml_utils.LxmlElement):
        """Update members from node."""
        for _, cprop in self.sorted_container_properties():
            cprop.update_from_node(self, node)
        self.node = node

    def _update_from_other(self, other_container: ContainerBase, skipped_properties: list[str] | None):
        """Update all ContainerProperties."""
        if skipped_properties is None:
            skipped_properties = []
        for prop_name, _ in self.sorted_container_properties():
            if prop_name not in skipped_properties:
                new_value = getattr(other_container, prop_name)
                setattr(self, prop_name, copy.copy(new_value))

    def mk_copy(self, copy_node: bool = False) -> ContainerBase:
        """Make a copy of self."""
        copied = copy.copy(self)
        if copy_node and self.node is not None:
            copied.node = xml_utils.copy_element(self.node)
        return copied

    def sorted_container_properties(self) -> list:
        """Return a list of (name, object) tuples of all GenericProperties ( and subclasses).

        Base class properties are first.
        """
        ret = []
        all_classes = inspect.getmro(self.__class__)
        for cls in reversed(all_classes):
            try:
                names = cls.__dict__['_props']
            except KeyError:
                continue
            for name in names:
                obj = getattr(cls, name)
                if obj is not None:
                    ret.append((name, obj))
        return ret

    def diff(self, other: ContainerBase,
             ignore_property_names: list[str] | None = None,
             max_float_diff = 1e-15) -> None | list[str]:
        """Compare all properties (except to be ignored ones).

        :param other: the object to compare with
        :param ignore_property_names: list of properties that shall be excluded from diff calculation
        :param max_float_diff: parameter for math.isclose() if float values are incorporated.
                                1e-15 corresponds to 15 digits max. accuracy (see sys.float_info.dig)
        :return: textual representation of differences or None if equal
        """
        ret = []
        ignore_list = ignore_property_names or []
        my_properties = self.sorted_container_properties()
        for name, _ in my_properties:
            if name in ignore_list:
                continue
            my_value = getattr(self, name)
            try:
                other_value = getattr(other, name)
            except AttributeError:
                ret.append(f'{name}={my_value}, other does not have this attribute')
            else:
                if isinstance(my_value, float) or isinstance(other_value, float):
                    if not math.isclose(my_value, other_value, rel_tol=max_float_diff, abs_tol=max_float_diff):
                        ret.append(f'{name}={my_value}, other={other_value}')
                elif my_value != other_value:
                    ret.append(f'{name}={my_value}, other={other_value}')
        # check also if other has a different list of properties
        my_property_names = {p[0] for p in my_properties}  # set comprehension
        other_property_names = {p[0] for p in other.sorted_container_properties()}
        surplus_names = other_property_names - my_property_names
        if surplus_names:
            ret.append(f'other has more data elements:{surplus_names}')
        return None if len(ret) == 0 else ret

    def is_equal(self, other: ContainerBase) -> bool:
        """Compare all properties."""
        return len(self.diff(other)) == 0
