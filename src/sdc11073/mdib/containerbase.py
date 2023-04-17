import copy
import inspect
from typing import Optional, List

from lxml import etree as etree_

from .. import observableproperties as properties
from ..namespaces import QN_TYPE, NamespaceHelper


class ContainerBase:
    NODETYPE = None  # overwrite in derived classes! determines the value of xsi:Type attribute, must be an etree_.QName object
    node = properties.ObservableProperty()
    is_state_container = False
    is_descriptor_container = False
    # every class with container properties must provide a list of property names.
    # this list is needed to create sub elements in a certain order.
    # rule is : elements are sorted from this root class to derived class. Last derived class comes last.
    # Initialization is from left to right
    # This is according to the inheritance in BICEPS xml schema

    def __init__(self):
        self.node = None
        for dummy_name, cprop in self.sorted_container_properties():
            cprop.init_instance_data(self)

    def get_actual_value(self, attr_name):
        """ ignores default value and implied value, e.g. returns None if value is not present in xml"""
        return getattr(self.__class__, attr_name).get_actual_value(self)

    def mk_node(self, tag:etree_.QName, ns_helper: NamespaceHelper, set_xsi_type: bool=False):
        """
        create an etree node from instance data
        :param tag: tag of the newly created node
        :param ns_helper: namespaces.NamespaceHelper instance
        :param set_xsi_type: if True, adds Type attribute to node
        :return: etree node
        """
        ns_map = ns_helper.partial_map(ns_helper.PM,
                                       ns_helper.MSG,
                                       ns_helper.XSI,)
        node = etree_.Element(tag, nsmap=ns_map)
        self.update_node(node, ns_helper, set_xsi_type)
        return node

    def update_node(self, node: etree_.Element, ns_helper: NamespaceHelper, set_xsi_type: bool = False):
        """
        update node with own data
        :param node: node to be updated
        :param ns_helper: namespaces.NamespaceHelper instance
        :param set_xsi_type:if True, adds Type attribute to node
        :return: etree node
        """
        if set_xsi_type and self.NODETYPE is not None:
            node.set(QN_TYPE, ns_helper.doc_name_from_qname(self.NODETYPE))
        for dummy_name, prop in self.sorted_container_properties():
            prop.update_xml_value(self, node)
        return node

    def update_from_node(self, node):
        """ update members.
        :param node: node to be updated
        """
        for dummy_name, cprop in self.sorted_container_properties():
            cprop.update_from_node(self, node)

    def _update_from_other(self, other_container, skipped_properties):
        # update all ContainerProperties
        if skipped_properties is None:
            skipped_properties = []
        for prop_name, _ in self.sorted_container_properties():
            if prop_name not in skipped_properties:
                new_value = getattr(other_container, prop_name)
                setattr(self, prop_name, copy.copy(new_value))

    def mk_copy(self, copy_node=True):
        copied = copy.copy(self)
        if copy_node:
            copied.node = copy.deepcopy(self.node)
        return copied

    def sorted_container_properties(self):
        """
        :return: a list of (name, object) tuples of all GenericProperties ( and subclasses), base class properties first.
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

    def diff(self, other, ignore_property_names: Optional[List[str]]=None):
        """ compares all properties (except to be ignored ones).
        :param other: the object to compare with
        :param ignore_property_names: list of properties that shall be excluded from diff calculation
        :return: textual representation of differences or None if equal
        """
        ret = []
        ignore_list = ignore_property_names or []
        my_properties = self.sorted_container_properties()
        for name, dummy in my_properties:
            if name in ignore_list:
                continue
            my_value = getattr(self, name)
            try:
                other_value = getattr(other, name)
            except AttributeError:
                ret.append(f'{name}={my_value}, other does not have this attribute')
            else:
                if isinstance(my_value, float) or isinstance(other_value, float):
                    # cast both to float, if one is a Decimal Exception might be thrown
                    try:
                        if abs((float(my_value) - float(other_value)) / float(my_value)) > 1e-6:  # 1e-6 is good enough
                            ret.append(f'{name}={my_value}, other={other_value}')
                    except ZeroDivisionError:
                        if abs((float(my_value) - float(other_value))) > 1e-6:  # 1e-6 is good enough
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

    def is_equal(self, other):
        return len(self.diff(other)) == 0
