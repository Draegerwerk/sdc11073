""" Classes in this module are used to declare the place where a xml value is located inside a document.
They also provide a mapping between XML data types (which are always stings in specific formats) and
python types. By doing so these classes completely hide the XML nature of data.
The basic offered types are Element, list of elements, attribute, and list of attributes.
They are the buildings blocks that are needed to declare XML data types.
Container properties represent values in xml nodes.
"""

from __future__ import annotations
import copy
import time
from collections import OrderedDict
from datetime import datetime, date
from typing import Union
from lxml import etree as etree_

from . import isoduration
from .dataconverters import DataConverterProtocol
from .dataconverters import DurationConverter, ClassCheckConverter, ListConverter, EnumConverter
from .dataconverters import StringConverter, NullConverter
from .dataconverters import TimestampConverter, DecimalConverter, IntegerConverter, BooleanConverter
from ..exceptions import ApiUsageError
from ..namespaces import QN_TYPE, docname_from_qname, text_to_qname


STRICT_TYPES = True  # if True, only the expected types are excepted.
MANDATORY_VALUE_CHECKING = True  # checks if mandatory values are present when xml is generated


class ElementNotFoundException(Exception):
    pass

class _NumberStack:
    # uses as a part of _local_var_name in _XmlStructureBaseProperty.
    # This makes duplicate names impossible
    _value = 0
    @classmethod
    def unique_number(cls) -> str:
        cls._value += 1
        return str(cls._value)


class _XmlStructureBaseProperty:
    """ This defines a python property that converts between Python Data Types and XML data types.
    It has knowledge about two things:
    - how to covert data from xml to python type and vice versa
    - name/ location of the xml data in a node.

    All derived Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    get_py_value_from_node: reads the value from XML data and converts it to Python data type.
    update_xml_value: convert the Python data type to XML type and write it to XML node.
     """

    def __init__(self, local_var_name: str, value_converter: DataConverterProtocol,
                 default_py_value=None, implied_py_value=None, is_optional=False):
        """

        :param local_var_name: a member with this same is added to instance
        :param value_converter: DataConverterProtocol
        :param default_py_value: initial value when initialized
                                 (should be set for mandatory elements, otherwise created xml might violate schema)
                                 and if the xml element does not exist.
        :param implied_py_value: for optional elements, this is the value that shall be implied if
                                 xml element does not exist.
                                 This value is for information only! Access only via class possible.
        :param is_optional: reflects if this element is optional in schema
        """
        if implied_py_value is not None and default_py_value is not None:
            raise ValueError('set only one of default_py_value and implied_py_value')
        if not is_optional and implied_py_value is not None:
            raise ValueError('is_optional == False and implied_py_value != None is not allowed ')
        if not hasattr(value_converter, 'check_valid'):
            raise TypeError
        self._converter = value_converter
        if STRICT_TYPES:
            if default_py_value is not None:
                self._converter.check_valid(default_py_value)
            if implied_py_value is not None:
                self._converter.check_valid(implied_py_value)
        self._default_py_value = None
        self._implied_py_value = None
        if default_py_value is not None:
            self._default_py_value = default_py_value
        if implied_py_value is not None:
            self._implied_py_value = implied_py_value
        self._is_optional = is_optional
        self._local_var_name = local_var_name
        self._is_default_value_set = False

    @property
    def is_optional(self):
        return self._is_optional

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = self._implied_py_value
        return value

    def get_actual_value(self, instance):
        """ Returns the actual value without considering default value and implied value,
        e.g. returns None if no value in xml exists.
        :param instance: the instance that has the property as member"""
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            return None

    def __set__(self, instance, py_value):
        """value is the representation on the program side, e.g a float. """
        if STRICT_TYPES:
            self._converter.check_valid(py_value)
        setattr(instance, self._local_var_name, py_value)

    def init_instance_data(self, instance):
        """
        Sets initial values to default_py_value.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :return: None
        """
        if self._default_py_value is not None:
            setattr(instance, self._local_var_name, copy.deepcopy(self._default_py_value))

    def update_xml_value(self, instance, node: etree_.Element):
        """
        Updates node with current data from instance.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that shall be updated
        :return: None
        """
        # to be defined in derived classes
        raise NotImplementedError

    def get_py_value_from_node(self, instance, node: etree_.Element):
        """
        Reads data from node.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that provides the value
        :return: value
        """
        # to be defined in derived classes
        raise NotImplementedError

    def update_from_node(self, instance, node: etree_.Element):
        """
        Updates instance data with data from node.
        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        value = self.get_py_value_from_node(instance, node)
        setattr(instance, self._local_var_name, value)


class _AttributeBase(_XmlStructureBaseProperty):
    """ Base class that represents an XML Attribute.
    XML Representation is a string, Python representation is determined by value_converter."""

    def __init__(self, attribute_name: str, value_converter=None,
                 default_py_value=None, implied_py_value=None, is_optional=True):
        """
        Represents an attribute in xml.
        :param attribute_name: name of the attribute in xml node
        :param value_converter: converter between xml value and python value
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        if isinstance(attribute_name, etree_.QName):
            local_var_name = f'_a_{attribute_name.localname}_{_NumberStack.unique_number()}'
        else:
            local_var_name = f'_a_{attribute_name.lower()}_{_NumberStack.unique_number()}'
        super().__init__(local_var_name, value_converter, default_py_value, implied_py_value, is_optional)
        self._attribute_name = attribute_name

    def get_py_value_from_node(self, instance, node):
        value = self._default_py_value
        try:
            xml_value = node.attrib.get(self._attribute_name)
            if xml_value is not None:
                value = self._converter.to_py(xml_value)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node: etree_.Element):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # this can only happen if there is no default value defined and __set__ has never been called
            py_value = None
        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional:
                raise ValueError(f'mandatory value {self._attribute_name} missing')
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xml_value = self._converter.to_xml(py_value)
            node.set(self._attribute_name, xml_value)

    def __str__(self):
        return f'{self.__class__.__name__} attribute {self._attribute_name}'


class _ElementBase(_XmlStructureBaseProperty):
    """ Base class that represents an XML Element."""

    def __init__(self, sub_element_name: Union[etree_.QName, None], value_converter, default_py_value=None, implied_py_value=None,
                 is_optional=False):
        """
        Represents a (sub) element in xml.
        :param sub_element_name: a QName or None
                                if None, the property represents the node itself, otherwise the sub node with given name.
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        if sub_element_name is None:
            local_var_name = f'_e_{_NumberStack.unique_number()}'
        else:
            local_var_name = f'_e_{sub_element_name.localname.lower()}_{_NumberStack.unique_number()}'
        super().__init__(local_var_name, value_converter, default_py_value, implied_py_value, is_optional)
        self._sub_element_name = sub_element_name

    @staticmethod
    def _get_element_by_child_name(node, sub_element_name, create_missing_nodes: bool):
        if sub_element_name is None:
            return node
        sub_node = node.find(sub_element_name)
        if sub_node is None:
            if not create_missing_nodes:
                raise ElementNotFoundException(f'Element {sub_element_name} not found in {node.tag}')
            sub_node = etree_.SubElement(node, sub_element_name)  # create this node
        return sub_node

    def remove_sub_element(self, node):
        if self._sub_element_name is None:
            return
        sub_node = node.find(self._sub_element_name)
        if sub_node is not None:
            node.remove(sub_node)

    def __str__(self):
        return f'{self.__class__.__name__} in sub element {self._sub_element_name}'


class StringAttributeProperty(_AttributeBase):
    """Python representation is a string."""

    def __init__(self, attribute_name: str, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, StringConverter, default_py_value, implied_py_value, is_optional)


class AnyURIAttributeProperty(StringAttributeProperty):
    pass


class CodeIdentifierAttributeProperty(StringAttributeProperty):
    pass


class HandleAttributeProperty(StringAttributeProperty):
    pass


class HandleRefAttributeProperty(StringAttributeProperty):
    pass


class SymbolicCodeNameAttributeProperty(StringAttributeProperty):
    pass


class ExtensionAttributeProperty(StringAttributeProperty):
    pass


class LocalizedTextRefAttributeProperty(StringAttributeProperty):
    pass


class TimeZoneAttributeProperty(StringAttributeProperty):
    pass


class EnumAttributeProperty(_AttributeBase):
    """ Python representation is an Enum."""

    def __init__(self, attribute_name: str, enum_cls,
                 default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, EnumConverter(enum_cls), default_py_value, implied_py_value, is_optional)


class TimestampAttributeProperty(_AttributeBase):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name: str, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class CurrentTimestampAttributeProperty(_AttributeBase):
    """ used for ClockState, it always writes current time to node.
    Setting value from python is possible, but makes no sense.
    """

    def __init__(self, attribute_name, is_optional=True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=None, is_optional=is_optional)

    def update_xml_value(self, instance, node):
        setattr(instance, self._local_var_name, time.time())
        super().update_xml_value(instance, node)


class DecimalAttributeProperty(_AttributeBase):
    """Python representation is a Decimal. """

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=DecimalConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QualityIndicatorAttributeProperty(DecimalAttributeProperty):
    """BICEPS: A value between 0 and 1 """


class DurationAttributeProperty(_AttributeBase):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=DurationConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class IntegerAttributeProperty(_AttributeBase):
    """ XML notation is an integer, python is an integer."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=IntegerConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class UnsignedIntAttributeProperty(IntegerAttributeProperty):
    """Python has no unsigned int, therefore this is the same as IntegerAttributeProperty. """
    pass


class VersionCounterAttributeProperty(UnsignedIntAttributeProperty):
    """VersionCounter in BICEPS is unsigned long.
    Python has no unsigned int, therefore this is the same as IntegerAttributeProperty. """
    pass


class ReferencedVersionAttributeProperty(VersionCounterAttributeProperty):
    pass


class BooleanAttributeProperty(_AttributeBase):
    """ XML notation is 'true' or 'false'.
    Python is a boolean."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=BooleanConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QNameAttributeProperty(_AttributeBase):
    """ XML Representation is a prefix:name string, Python representation is a QName."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=ClassCheckConverter(etree_.QName),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        """
        :return: None or a QName
        """
        value = self._default_py_value
        try:
            xml_value = node.attrib.get(self._attribute_name)
            if xml_value is not None:
                value = text_to_qname(xml_value, node.nsmap)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None
            py_value = None
        if py_value is None:
            py_value = self._default_py_value
        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional:
                raise ValueError(f'mandatory value {self._attribute_name} missing')
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xml_value = docname_from_qname(py_value, node.nsmap)
            node.set(self._attribute_name, xml_value)


class _AttributeListBase(_AttributeBase):
    """ XML Representation is a string which is a space separated list.
    Python representation is a list of strings if value_converter is None,
    else a list of converted values."""

    def __init__(self, attribute_name, value_converter, is_optional=True):
        super().__init__(attribute_name, value_converter, is_optional=is_optional)

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def init_instance_data(self, instance):
        setattr(instance, self._local_var_name, [])

    def get_py_value_from_node(self, instance, node):
        values = self._default_py_value
        try:
            xml_value = node.attrib.get(self._attribute_name)
            if xml_value is not None:
                split_result = xml_value.split(' ')
                values = [self._converter.elem_to_py(val) for val in split_result if val]
        except ElementNotFoundException:
            pass
        return values

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if not py_value and self.is_optional:  # is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            if py_value is None:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._attribute_name} missing')
                xml_value = ''
            else:
                xml_value = ' '.join([self._converter.elem_to_xml(v) for v in py_value])
            node.set(self._attribute_name, xml_value)


class _StringAttributeListBase(_AttributeListBase):
    def __init__(self, attribute_name, value_converter=None):
        converter = value_converter or ListConverter(ClassCheckConverter(str))
        super().__init__(attribute_name, converter)


class HandleRefListAttributeProperty(_StringAttributeListBase):
    pass


class EntryRefListAttributeProperty(_StringAttributeListBase):
    pass


class OperationRefListAttributeProperty(_StringAttributeListBase):
    pass


class AlertConditionRefListAttributeProperty(_StringAttributeListBase):
    pass


class DecimalListAttributeProperty(_AttributeListBase):
    """ XML representation: an attribute string that represents 0...n decimals, separated with spaces.
        Python representation: List of Decimal if attribute is set (can be an empty list!), otherwise None.
        """

    def __init__(self, attribute_name):
        super().__init__(attribute_name, ListConverter(DecimalConverter))


class NodeTextProperty(_ElementBase):
    """ The handled data is the text of an element.
    Python representation is a string."""

    def __init__(self, sub_element_name, value_converter, default_py_value=None, implied_py_value=None,
                 is_optional=False, min_length=0):
        super().__init__(sub_element_name, value_converter, default_py_value, implied_py_value, is_optional)
        self._min_length = min_length

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            return self._converter.to_py(sub_node.text)
        except ElementNotFoundException:
            return self._default_py_value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional and self._min_length:
                raise ValueError(f'mandatory value {self._sub_element_name} missing')

            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            else:
                if self.is_optional:
                    sub_node = node.find(self._sub_element_name)
                    if sub_node is not None:
                        node.remove(sub_node)
                else:
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            sub_node.text = self._converter.to_xml(py_value)

    def __repr__(self):
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class NodeStringProperty(NodeTextProperty):
    """Python representation is a string."""

    def __init__(self, sub_element_name=None, default_py_value=None, implied_py_value=None, is_optional=False,
                 min_length=0):
        super().__init__(sub_element_name, StringConverter, default_py_value, implied_py_value, is_optional, min_length)


class AnyUriTextElement(NodeStringProperty):
    # for now the same as NodeStringProperty ,but later it could be handy to add uri type checking
    pass


class LocalizedTextContentProperty(NodeStringProperty):
    pass


class NodeEnumTextProperty(NodeTextProperty):
    """Python representation is an Enum."""

    def __init__(self, sub_element_name, enum_cls, default_py_value=None, implied_py_value=None, is_optional=False):
        super().__init__(sub_element_name, EnumConverter(enum_cls), default_py_value, implied_py_value,
                         is_optional, min_length=1)
        self.enum_cls = enum_cls


class NodeIntProperty(NodeTextProperty):
    """Python representation is a string."""

    def __init__(self, sub_element_name=None, default_py_value=None, implied_py_value=None, is_optional=False,
                 min_length=0):
        super().__init__(sub_element_name, IntegerConverter, default_py_value, implied_py_value, is_optional,
                         min_length)


class NodeTextQNameProperty(_ElementBase):
    """ The handled data is a single qualified name in the text of an element
    in the form prefix:localname"""

    def __init__(self, sub_element_name, default_py_value=None, is_optional=False):
        super().__init__(sub_element_name, ClassCheckConverter(etree_.QName), default_py_value,
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            xml_value = sub_node.text
            if xml_value is not None:
                value = text_to_qname(xml_value, sub_node.nsmap)
                return value
        except ElementNotFoundException:
            pass
        return self._default_py_value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            else:
                if self.is_optional:
                    sub_node = node.find(self._sub_element_name)
                    if sub_node is not None:
                        node.remove(sub_node)
                else:
                    if MANDATORY_VALUE_CHECKING and not self.is_optional:
                        raise ValueError(f'mandatory value {self._sub_element_name} missing')
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            value = docname_from_qname(py_value, sub_node.nsmap)
            sub_node.text = value


class _ExtensionLocalValue:
    def __init__(self, value):
        self.value = value or OrderedDict()

    def __eq__(self, other):
        if other is None:
            return len(self.value) == 0
        return self.value == other.value


class ExtensionNodeProperty(_ElementBase):
    """ Represents an ext:Extension Element that contains xml tree of any kind."""

    def __init__(self, sub_element_name, default_py_value=None):
        super().__init__(sub_element_name, ClassCheckConverter(_ExtensionLocalValue), default_py_value,
                         is_optional=True)

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = _ExtensionLocalValue(None)
            setattr(instance, self._local_var_name, value)
        return value

    def get_py_value_from_node(self, instance, node):
        try:
            extension_nodes = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundException:
            return None
        values = OrderedDict()
        for extension_node in extension_nodes:
            try:
                cls = instance.extension_class_lookup.get(extension_node.tag)
            except AttributeError:
                cls = None
            if cls:
                values[extension_node.tag] = cls.from_node(extension_node)
            else:
                values[extension_node.tag] = extension_node
        return _ExtensionLocalValue(values)

    def update_xml_value(self, instance, node):
        try:
            extension_local_value = getattr(instance, self._local_var_name)
        except AttributeError:
            extension_local_value = None
        if extension_local_value is None:
            sub_node = node.find(self._sub_element_name)
            if sub_node is not None:
                node.remove(sub_node)
        else:
            if not extension_local_value.value:
                return
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)

            del sub_node[:]  # delete all children first

            for tag, val in extension_local_value.value.items():
                if val is None:
                    continue
                if hasattr(val, 'as_etree_node'):
                    _node = val.as_etree_node(tag, node.nsmap)
                else:
                    _node = val
                sub_node.append(copy.copy(_node))

class AnyEtreeNodeProperty(_ElementBase):
    """ Represents an Element that contains xml tree of any kind."""

    def __init__(self, sub_element_name, is_optional=False):
        super().__init__(sub_element_name, NullConverter, default_py_value=None,
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundException:
            return None
        return sub_node[:]  # all children

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            else:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if isinstance(py_value, etree_._Element):
                sub_node.append(py_value)
            else:
                sub_node.extend(py_value)


class SubElementProperty(_ElementBase):
    """ uses a value that has an "as_etree_node" method"""

    def __init__(self, sub_element_name, value_class, default_py_value=None,
                 implied_py_value=None, is_optional=False):
        super().__init__(sub_element_name, ClassCheckConverter(value_class), default_py_value, implied_py_value,
                         is_optional)
        self.value_class = value_class

    def get_py_value_from_node(self, instance, node):
        value = self._default_py_value
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            value_class = self.value_class.value_class_from_node(sub_node)
            value = value_class.from_node(sub_node)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None:
            if not self.is_optional:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                etree_.SubElement(node, self._sub_element_name, nsmap=node.nsmap)
        else:
            sub_node = py_value.as_etree_node(self._sub_element_name, node.nsmap)
            if hasattr(py_value, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                    and py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
            node.append(sub_node)


class ContainerProperty(_ElementBase):
    """ a value that has "mk_node" and cls.from_node methods"""

    def __init__(self, sub_element_name, value_class, cls_getter, ns_helper, is_optional=False):
        super().__init__(sub_element_name, ClassCheckConverter(value_class), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def get_py_value_from_node(self, instance, node):
        value = self._default_py_value
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            node_type_str = sub_node.get(QN_TYPE)
            if node_type_str is not None:
                node_type = text_to_qname(node_type_str, node.nsmap)
                value_class = self._cls_getter(node_type)
            else:
                value_class = self.value_class
            value = value_class.from_node(sub_node)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None:
            if not self.is_optional:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                etree_.SubElement(node, self._sub_element_name, nsmap=node.nsmap)
        else:
            self.remove_sub_element(node)
            sub_node = py_value.mk_node(self._sub_element_name, self._ns_helper)
            if py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
            node.append(sub_node)


class _ElementListProperty(_ElementBase):
    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def init_instance_data(self, instance):
        setattr(instance, self._local_var_name, [])

    def get_py_value_from_node(self, instance, node):
        # still not implemented here, to be defined in derived classes
        raise NotImplementedError

    def update_xml_value(self, instance, node):
        # still not implemented here, to be defined in derived classes
        raise NotImplementedError


class SubElementListProperty(_ElementListProperty):
    """ a list of values that have an "as_etree_node" method. Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel"""

    def __init__(self, sub_element_name, value_class, is_optional=True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class

    def get_py_value_from_node(self, instance, node):
        """ get from node"""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for _node in nodes:
                value_class = self.value_class.value_class_from_node(_node)
                value = value_class.from_node(_node)
                objects.append(value)
            return objects
        except ElementNotFoundException:
            return objects

    def update_xml_value(self, instance, node):
        """ value is a list of objects with "as_etree_node" method"""
        # remove all existing nodes
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is not None:
            for val in py_value:
                #name = self._sub_element_name or val.NODETYPE
                sub_node = val.as_etree_node(self._sub_element_name, node.nsmap)
                if hasattr(val, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                        and val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))
                node.append(sub_node)

    def __repr__(self):
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class ContainerListProperty(_ElementListProperty):
    """ a list of values that have "mk_node" and cls.from_node methods.
    Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel"""

    def __init__(self, sub_element_name, value_class, cls_getter, ns_helper, is_optional=True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def get_py_value_from_node(self, instance, node):
        """ get from node"""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for _node in nodes:
                node_type_str = _node.get(QN_TYPE)
                if node_type_str is not None:
                    node_type = text_to_qname(node_type_str, _node.nsmap)
                    value_class = self._cls_getter(node_type)
                else:
                    value_class = self.value_class
                value = value_class.from_node(_node)
                objects.append(value)
            return objects
        except ElementNotFoundException:
            return objects

    def update_xml_value(self, instance, node):
        """ value is a list of objects with "mk_node" method"""
        # remove all existing nodes
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        nodes = node.findall(self._sub_element_name)
        for _node in nodes:
            node.remove(_node)
        # ... and create new ones
        if py_value is not None:
            for val in py_value:
                sub_node = val.mk_node(self._sub_element_name, self._ns_helper)
                if val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))
                node.append(sub_node)

    def __repr__(self):
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class SubElementTextListProperty(_ElementListProperty):
    """ represents a list of strings. on xml side every string is a text of a sub element"""

    def __init__(self, sub_element_name, value_class, is_optional=True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        """ get from node"""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for _node in nodes:
                objects.append(_node.text)
            return objects
        except ElementNotFoundException:
            return objects

    def update_xml_value(self, instance, node):
        """ value is a list of strings"""
        # remove all existing nodes
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None or len(py_value) == 0:
            return

        nodes = node.findall(self._sub_element_name)
        for _node in nodes:
            node.remove(_node)
        # ... and create new ones
        for val in py_value:
            child = etree_.SubElement(node, self._sub_element_name)
            try:
                child.text = val
            except TypeError as ex:
                # re-raise with better info about data
                raise TypeError(f'{ex} in {self}') from ex

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class SubElementStringListProperty(SubElementTextListProperty):
    """ represents a list of strings. on xml side every string is a text of a sub element"""

    def __init__(self, sub_element_name, is_optional=True):
        super().__init__(sub_element_name, str, is_optional=is_optional)


class SubElementHandleRefListProperty(SubElementStringListProperty):
    """ List of Handles"""


class SubElementWithSubElementListProperty(SubElementProperty):
    """This Represents an Element that is optional and only present if its value class is not empty.
    value_class must have an is_empty method
    """

    def __init__(self, sub_element_name, default_py_value, value_class):
        assert hasattr(value_class, 'is_empty')
        super().__init__(sub_element_name,
                         default_py_value=default_py_value,
                         value_class=value_class)

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None or py_value.is_empty():
            return
        self.remove_sub_element(node)
        node.append(py_value.as_etree_node(self._sub_element_name, node.nsmap))

    def __set__(self, instance, py_value):
        if isinstance(py_value, self.value_class):
            super().__set__(instance, py_value)
        else:
            raise ApiUsageError(f'do not set {self._sub_element_name} directly, use child member!')


class AnyEtreeNodeListProperty(_ElementListProperty):
    """ Node < sub_element_name> has etree Element children."""

    def __init__(self, sub_element_name, is_optional=True):
        value_class = etree_._Element
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        """ get from node"""
        objects = []
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is None:
                return []
            return sub_node[:]
        except ElementNotFoundException:
            return objects

    def update_xml_value(self, instance, node):
        """ value is a list of etree nodes"""
        # remove all existing nodes
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = None

        if py_value is None or len(py_value) == 0:
            if self.is_optional:
                self.remove_sub_element(node)
            return

        sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        sub_node.extend(py_value)

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class NodeTextListProperty(_ElementListProperty):
    """The handled data is a list of words (string without whitespace). The xml text is the joined list of words.
    """

    def __init__(self, sub_element_name, value_class, is_optional=False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)),
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node.text is not None:
                return sub_node.text.split()
        except ElementNotFoundException:
            pass
        return self._default_py_value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            else:
                if self.is_optional:
                    sub_node = node.find(self._sub_element_name)
                    if sub_node is not None:
                        node.remove(sub_node)
                else:
                    if MANDATORY_VALUE_CHECKING and not self.is_optional:
                        raise ValueError(f'mandatory value {self._sub_element_name} missing')
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            sub_node.text = ' '.join(py_value)


class NodeTextQNameListProperty(_ElementListProperty):
    """ The handled data is a list of qualified names. The xml text is the joined list of qnames in the
    form prefix:localname"""

    def __init__(self, sub_element_name, is_optional=False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(etree_.QName)),
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance, node):
        result = []
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is None:
                return None
            if sub_node.text is not None:
                for q_name_string in sub_node.text.split():
                    result.append(text_to_qname(q_name_string, sub_node.nsmap))
                return result
        except ElementNotFoundException:
            pass
        return self._default_py_value or result

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            else:
                if self.is_optional:
                    sub_node = node.find(self._sub_element_name)
                    if sub_node is not None:
                        node.remove(sub_node)
                else:
                    if MANDATORY_VALUE_CHECKING and not self.is_optional:
                        raise ValueError(f'mandatory value {self._sub_element_name} missing')
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            tmp = []
            for q_name in py_value:
                # by setting each qname as text, namespace prefixes are generated automatically
                sub_node.text = q_name
                tmp.append(sub_node.text)
            sub_node.text = ' '.join(tmp)


class DateOfBirthProperty(_ElementBase):
    """ this represents the DateOfBirth type of BICEPS xml schema draft 10:
        <xsd:simpleType>
            <xsd:union memberTypes="xsd:dateTime xsd:date xsd:gYearMonth xsd:gYear"/>
        </xsd:simpleType>
    xsd:dateTime is YYYY-MM-DDThh:mm:ss.sss
    xsd:date is YYYY-MM-DD format. All components are required
    xsd:gYearMonth is YYYY-MM
    xsd:gYear is YYYY
    If the timepoint of birth matters, the value SHALL be populated with a time zone.

    Time zone info can be provided:
       UTC can be specified by appending a Z character, e.g. 2002-09-24Z
       other timezones by adding a positive or negative time behind the date, e.g. 2002.09-24-06:00, 2002-09-24+06:00
    xsd:time is hh:mm:ss format, e.g. 9:30:10, 9:30:10.5. All components are required.
    Time zone handling is identical to date type

    The corresponding Python types are datetime.Date (=> not time point available) or datetime.Datetime (with time point attribute)
    """

    def __init__(self, sub_element_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(sub_element_name, ClassCheckConverter(datetime, date),
                         default_py_value, implied_py_value, is_optional)

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is not None:
                date_string = sub_node.text
                return isoduration.parse_date_time(date_string)
        except ElementNotFoundException:
            pass
        return None

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is None:
            self.remove_sub_element(node)
        else:
            if isinstance(py_value, str):
                datestring = py_value  # use strings as they are
            else:
                datestring = self._mk_datestring(py_value)
            sub_element = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            sub_element.text = datestring

    @staticmethod
    def mk_value_object(date_string):
        return isoduration.parse_date_time(date_string)

    @staticmethod
    def _mk_datestring(date_object):
        return isoduration.date_time_string(date_object)
