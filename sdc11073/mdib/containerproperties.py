# -*- coding: utf-8 -*-
"""Container properties represent values in xml nodes.
These values can be node attributes, node texts or a complete Elements with optional sub nodes.
The properties completely hide the XML nature of data. To serve this purpose, they can convert between XML data types and Python data types.
"""
import copy
import time
from collections import OrderedDict
from decimal import Decimal

from lxml import etree as etree_

from .. import isoduration
from .. import namespaces
from ..dataconverters import TimestampConverter, DecimalConverter, IntegerConverter, BooleanConverter, \
    DurationConverter, NullConverter
from ..namespaces import QN_TYPE, docname_from_qname, text_to_qname

# The STRICT_ENUM_ATTRIBUTE constant allows to change the global behavior regarding enumeration types:
# if STRICT_ENUM_ATTRIBUTE is True, EnumAttributeProperty instances will only accept enum values of correct type
# ( Or None if allowed). Otherwise every value is accepted.
STRICT_ENUM_ATTRIBUTE = True

# The STRICT_DECIMAL_ATTRIBUTE constant allows to change the global behavior regarding Decimal types:
# if STRICT_DECIMAL_ATTRIBUTE is True, DecimalAttributeProperty and DecimalListAttributeProperty instances will only
# accept decimal.Decimal values or None. Otherwise no type checking in __set__ is performed
# ( Or None if allowed). Otherwise every value is accepted.
STRICT_DECIMAL_ATTRIBUTE = True


class ElementNotFoundException(Exception):
    pass


class _PropertyBase:
    """ Navigates to sub element and handles storage of value in instance.

    All Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    get_py_value_from_node: reads the value from XML data and converts it to Python data type.
    update_xml_value: convert the Python data type to XML type and write it to XML node.
     """

    def __init__(self, default_py_value=None, implied_py_value=None, is_optional=False):
        """
        :param default_py_value: initial value when initialized (should be set for mandatory elements, otherwise created xml might violate schema)
                                 and if the xml element does not exist.
        :param implied_py_value: for optional elements, this is the value that shall be implied if xml element does not exist
                                 this value is for information only! Access only via class possible.
        :param is_optional: reflects if this element is optional in schema
        """
        self._default_py_value = default_py_value
        self._implied_py_value = implied_py_value
        self._is_optional = is_optional
        self._local_var_name = None

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
        setattr(instance, self._local_var_name, py_value)

    def init_instance_data(self, instance):
        """
        Sets initial values to default_py_value.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :return: None
        """
        setattr(instance, self._local_var_name, copy.copy(self._default_py_value))

    def update_xml_value(self, instance, node):
        """
        Updates node with current data from instance.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that shall be updated
        :return: None
        """
        # to be defined in derived classes
        raise NotImplementedError

    def get_py_value_from_node(self, instance, node):
        """
        Reads data from node.
        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that provides the value
        :return: value
        """
        # to be defined in derived classes
        raise NotImplementedError

    def update_from_node(self, instance, node):
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


class _AttributePropertyBase(_PropertyBase):
    """ Base class that represents an XML Attribute.
    XML Representation is a string, Python representation is determined by value_converter."""

    def __init__(self, attribute_name, value_converter=None,
                 default_py_value=None, implied_py_value=None, is_optional=True):
        """
        Represents an attribute in xml.
        :param attribute_name: name of the attribute in xml node
        :param value_converter: converter between xml value and python value
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        super().__init__(default_py_value, implied_py_value, is_optional)
        self._attribute_name = attribute_name
        if isinstance(attribute_name, etree_.QName):
            self._local_var_name = '_a_' + attribute_name.localname
        else:
            self._local_var_name = '_a_' + attribute_name.lower()
        self._converter = value_converter if value_converter is not None else NullConverter

    def get_py_value_from_node(self, instance, node):
        value = self._default_py_value
        try:
            xml_value = node.attrib.get(self._attribute_name)
            if xml_value is not None:
                value = self._converter.to_py(xml_value)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if py_value is None:
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


class _NodeProperty(_PropertyBase):
    """ Base class that represents an XML Element."""
    def __init__(self, sub_element_name, default_py_value=None, implied_py_value=None, is_optional=False,
                 local_var_prefix=''):
        """
        Represents a (sub) element in xml.
        :param sub_element_name: a QName or None
                                if None, the property represents the node itself, otherwise the sub node with given name.
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        super().__init__(default_py_value, implied_py_value, is_optional)
        self._sub_element_name = sub_element_name
        local_var_name = f'_none{local_var_prefix}' if self._sub_element_name is None \
            else f'_{local_var_prefix}{self._sub_element_name.localname.lower()}'
        self._local_var_name = local_var_name

    @staticmethod
    def _get_element_by_child_name(node, sub_element_name, create_missing_nodes):
        if sub_element_name is None:
            return node
        sub_node = node.find(sub_element_name)
        if sub_node is None:
            if not create_missing_nodes:
                raise ElementNotFoundException(f'Element {sub_element_name} not found in {node.tag}')
            sub_node = etree_.SubElement(node, sub_element_name)  # create this node
        return sub_node

    def remove_sub_element(self, node):
        sub_node = node.find(self._sub_element_name)
        if sub_node is not None:
            node.remove(sub_node)

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class StringAttributeProperty(_AttributePropertyBase):
    """Python representation is a string."""
    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, None, default_py_value, implied_py_value, is_optional)


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


class EnumAttributeProperty(_AttributePropertyBase):
    """ Python representation is an Enum."""

    def __init__(self, attribute_name, enum_cls, default_py_value=None,
                 implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, None, default_py_value, implied_py_value, is_optional)
        self.enum_cls = enum_cls

    def __set__(self, instance, py_value):
        """value is the representation on the program side, e.g a float. """
        if STRICT_ENUM_ATTRIBUTE:
            if not self.is_optional and py_value is None and self._default_py_value is None:
                raise ValueError(
                    f'None value is not allowed in {instance.__class__.__name__}.{self._attribute_name}, only {self.enum_cls}')
            if py_value is not None and not isinstance(py_value, self.enum_cls):
                raise ValueError(
                    f'in {instance.__class__.__name__}.{self._attribute_name}: value {py_value} is not of type {self.enum_cls}')
        super().__set__(instance, py_value)

    def get_py_value_from_node(self, instance, node):
        value = self._default_py_value
        try:
            xml_value = node.attrib.get(self._attribute_name)
            if xml_value is not None:
                value = self.enum_cls(xml_value)
        except ElementNotFoundException:
            pass
        return value

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if py_value is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            if hasattr(py_value, 'value'):
                xml_value = py_value.value
            else:
                xml_value = py_value
            node.set(self._attribute_name, xml_value)


class TimestampAttributeProperty(_AttributePropertyBase):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class CurrentTimestampAttributeProperty(_AttributePropertyBase):
    """ used for ClockState, it always writes current time to node.
    Setting value from python is possible, but makes no sense.
    """

    def __init__(self, attribute_name, is_optional=True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=None, is_optional=is_optional)

    def update_xml_value(self, instance, node):
        setattr(instance, self._local_var_name, time.time())
        super().update_xml_value(instance, node)


class DecimalAttributeProperty(_AttributePropertyBase):
    """Python representation is a Decimal. """
    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=DecimalConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)

    def __set__(self, instance, py_value):
        if py_value is not None and STRICT_DECIMAL_ATTRIBUTE is True:
            if not isinstance(py_value, Decimal):
                raise ValueError(f'only Decimal type allowed for {self._attribute_name}')
            xml_value = str(py_value)
            if 'E' in xml_value or 'e' in xml_value:
                raise ValueError(f'Decimal {xml_value} has exp notation, this is not valid for xml Decimal!')
        setattr(instance, self._local_var_name, py_value)


class QualityIndicatorAttributeProperty(DecimalAttributeProperty):
    """BICEPS: A value between 0 and 1 """


class DurationAttributeProperty(_AttributePropertyBase):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=DurationConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class IntegerAttributeProperty(_AttributePropertyBase):
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


class BooleanAttributeProperty(_AttributePropertyBase):
    """ XML notation is 'true' or 'false'.
    Python is a boolean."""

    def __init__(self, attribute_name, default_py_value=None, implied_py_value=None, is_optional=True):
        super().__init__(attribute_name, value_converter=BooleanConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QNameAttributeProperty(_AttributePropertyBase):
    """ XML Representation is a prefix:name string, Python representation is a QName."""

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
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xml_value = namespaces.docname_from_qname(py_value, node.nsmap)
            node.set(self._attribute_name, xml_value)


class _NodeAttributeListPropertyBase(_AttributePropertyBase):
    """ XML Representation is a string which is a space separated list.
    Python representation is a list of strings if value_converter is None,
    else a list of converted values."""

    def __init__(self, attribute_name, value_converter=None):
        super().__init__(attribute_name, value_converter)

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
                splitted = xml_value.split(' ')
                values = [self._converter.to_py(val) for val in splitted if val]
                # value = [h for h in self._converter.to_py(xml_value.split(' ')) if h]
        except ElementNotFoundException:
            pass
        return values

    def update_xml_value(self, instance, node):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if py_value is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            # xml_value = ' '.join(self._converter.to_xml(py_value))
            xml_value = ' '.join([self._converter.to_xml(v) for v in py_value])
            node.set(self._attribute_name, xml_value)


class HandleRefListAttributeProperty(_NodeAttributeListPropertyBase):
    pass


class EntryRefListAttributeProperty(_NodeAttributeListPropertyBase):
    pass


class OperationRefListAttributeProperty(_NodeAttributeListPropertyBase):
    pass


class AlertConditionRefListAttributeProperty(_NodeAttributeListPropertyBase):
    pass


class DecimalListAttributeProperty(_NodeAttributeListPropertyBase):
    """ XML representation: an attribute string that represents 0..n decimals, separated with spaces.
        Python representation: List of Decimal if attribute is set (can be an empty list!), otherwise None.
        """

    def __init__(self, attribute_name):
        super().__init__(attribute_name, DecimalConverter)

    def __set__(self, instance, py_value):
        if STRICT_DECIMAL_ATTRIBUTE is True:
            for value in py_value:
                if value is not None:
                    if not isinstance(value, Decimal):
                        raise ValueError(f'only Decimal type allowed for {self._attribute_name}')
                    xml_value = str(value)
                    if 'E' in xml_value or 'e' in xml_value:
                        raise ValueError(f'Decimal {xml_value} has exp notation, this is not valid for xml Decimal!')
        setattr(instance, self._local_var_name, py_value)


class NodeTextProperty(_NodeProperty):
    """ The handled data is the text of an element.
    Python representation is a string."""

    def __init__(self, sub_element_name=None, default_py_value=None, implied_py_value=None, is_optional=False):
        super().__init__(sub_element_name, default_py_value, implied_py_value, is_optional)
        self._converter = NullConverter

    def get_py_value_from_node(self, instance, node):
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            return sub_node.text
        except ElementNotFoundException:
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
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            sub_node.text = py_value

    def __repr__(self):
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class NodeEnumTextProperty(NodeTextProperty):
    """Python representation is an Enum."""
    def __init__(self, enum_cls, sub_element_name, default_py_value=None, implied_py_value=None, is_optional=False):
        super().__init__(sub_element_name, default_py_value, implied_py_value, is_optional)
        self.enum_cls = enum_cls


class NodeTextQNameProperty(_NodeProperty):
    """ The handled data is a qualified name as in the text of an element"""

    def __init__(self, sub_element_name, default_py_value=None, is_optional=False):
        super().__init__(sub_element_name, default_py_value, is_optional=is_optional)

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
                    sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                    sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            value = namespaces.docname_from_qname(py_value, sub_node.nsmap)
            sub_node.text = value


class LocalizedTextContentProperty(NodeTextProperty):
    pass


class _ExtensionLocalValue:
    def __init__(self, value):
        self.value = value or OrderedDict()

    def __eq__(self, other):
        if other is None:
            return len(self.value) == 0
        return self.value == other.value


class ExtensionNodeProperty(_NodeProperty):
    """ Represents an ext:Extension Element that contains xml tree of any kind."""

    def __init__(self, sub_element_name=None, default_py_value=None):
        if sub_element_name is None:
            sub_element_name = namespaces.extTag('Extension')
        super().__init__(sub_element_name, default_py_value, is_optional=True, local_var_prefix='ext')

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
                if isinstance(val, etree_._Element):  # pylint: disable=protected-access
                    _node = val
                else:
                    _node = val.as_etree_node(tag, node.nsmap)
                sub_node.append(copy.copy(_node))


class SubElementProperty(_NodeProperty):
    """ uses a value that has an "as_etree_node" method"""

    def __init__(self, sub_element_name, value_class, default_py_value=None, implied_py_value=None, is_optional=False):
        super().__init__(sub_element_name, default_py_value, implied_py_value, is_optional)
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

        if py_value is not None:
            self.remove_sub_element(node)
            sub_node = py_value.as_etree_node(self._sub_element_name, node.nsmap)
            if py_value.__class__ != self.value_class:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
            node.append(sub_node)


class _ElementListProperty(_NodeProperty):
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

    def __init__(self, sub_element_name, value_class):
        super().__init__(sub_element_name)
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

        nodes = node.findall(self._sub_element_name)
        for _node in nodes:
            node.remove(_node)
        # ... and create new ones
        if py_value is not None:
            for val in py_value:
                sub_node = val.as_etree_node(self._sub_element_name, node.nsmap)
                if val.__class__ != self.value_class:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
                node.append(sub_node)

    def __repr__(self):
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class SubElementTextListProperty(_ElementListProperty):
    """ represents a list of strings."""

    def __init__(self, sub_element_name):
        super().__init__(sub_element_name)

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


class SubElementHandleRefListProperty(SubElementTextListProperty):
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
            raise RuntimeError(f'do not set {self._sub_element_name} directly, use child member!')


class DateOfBirthProperty(_NodeProperty):
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
        super().__init__(sub_element_name, default_py_value, implied_py_value, is_optional)

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
