"""Classes in this module are used to declare the place where a xml value is located inside a document.

They also provide a mapping between XML data types (which are always stings in specific formats) and
python types. By doing so these classes completely hide the XML nature of data.
The basic offered types are Element, list of elements, attribute, and list of attributes.
They are the buildings blocks that are needed to declare XML data types.
Container properties represent values in xml nodes.
"""
from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Callable

from lxml import etree as etree_

from sdc11073.exceptions import ApiUsageError
from sdc11073.namespaces import QN_TYPE, docname_from_qname, text_to_qname

from . import isoduration
from .dataconverters import (
    BooleanConverter,
    ClassCheckConverter,
    DecimalConverter,
    DurationConverter,
    EnumConverter,
    IntegerConverter,
    ListConverter,
    NullConverter,
    StringConverter,
    TimestampConverter,
)
from sdc11073 import xml_utils

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from decimal import Decimal
    from sdc11073.namespaces import NamespaceHelper
    from sdc11073.xml_types.basetypes import XMLTypeBase
    from sdc11073.mdib.containerbase import ContainerBase
    from .dataconverters import DataConverterProtocol
    from .isoduration import DurationType

STRICT_TYPES = True  # if True, only the expected types are excepted.
MANDATORY_VALUE_CHECKING = True  # checks if mandatory values are present when xml is generated


class ElementNotFoundError(Exception):  # noqa: D101
    pass


class _NumberStack:
    # uses as a part of _local_var_name in _XmlStructureBaseProperty.
    # This makes duplicate names impossible
    _value = 0

    @classmethod
    def unique_number(cls) -> str:
        cls._value += 1
        return str(cls._value)


class _XmlStructureBaseProperty(ABC):
    """_XmlStructureBaseProperty defines a python property that converts between Python Data Types and XML data types.

    It has knowledge about two things:
    - how to covert data from xml to python type and vice versa
    - name/ location of the xml data in a node.

    All derived Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    get_py_value_from_node: reads the value from XML data and converts it to Python data type.
    update_xml_value: convert the Python data type to XML type and write it to XML node.
    """

    def __init__(self, local_var_name: str,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        """Construct an instance.

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
    def is_optional(self) -> bool:
        return self._is_optional

    def __get__(self, instance, owner) -> Any:  # noqa: ANN001
        """Return a python value, use the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = self._implied_py_value
        return value

    def get_actual_value(self, instance: Any) -> Any | None:
        """Return the actual value without considering default value and implied value.

        E.g. return None if no value in xml exists.
        :param instance: the instance that has the property as member
        """
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            return None

    def __set__(self, instance, py_value):  # noqa: ANN001
        """Value is the representation on the program side, e.g a float."""
        if STRICT_TYPES:
            self._converter.check_valid(py_value)
        setattr(instance, self._local_var_name, py_value)

    def init_instance_data(self, instance: Any):
        """Set initial values to default_py_value.

        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :return: None
        """
        if self._default_py_value is not None:
            setattr(instance, self._local_var_name, copy.deepcopy(self._default_py_value))

    @abstractmethod
    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Update node with current data from instance.

        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that shall be updated
        :return: None
        """

    @abstractmethod
    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Read data from node.

        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :param node: the etree node that provides the value
        :return: value
        """

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        value = self.get_py_value_from_node(instance, node)
        setattr(instance, self._local_var_name, value)


class _AttributeBase(_XmlStructureBaseProperty):
    """Base class that represents an XML Attribute.

    The XML Representation is a string.
    The python representation is determined by value_converter.
    """

    def __init__(self, attribute_name: str,  # noqa: PLR0913
                 value_converter: DataConverterProtocol | None = None,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        """Construct an instance.

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

    def get_py_value_from_node(self, instance: Any,  # noqa: ARG002
                               node: xml_utils.LxmlElement | None) -> Any:
        xml_value = None if node is None else node.attrib.get(self._attribute_name)
        return None if xml_value is None else self._converter.to_py(xml_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # this can only happen if there is no default value defined and __set__ has never been called
            py_value = None
        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional:
                raise ValueError(f'mandatory value {self._attribute_name} missing')
            try:
                if self._attribute_name in node.attrib:
                    del node.attrib[self._attribute_name]
            except ElementNotFoundError:
                return
        else:
            xml_value = self._converter.to_xml(py_value)
            node.set(self._attribute_name, xml_value)

    def __str__(self) -> str:
        return f'{self.__class__.__name__} attribute {self._attribute_name}'


class _ElementBase(_XmlStructureBaseProperty, ABC):
    """_ElementBase represents an XML Element."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = False):
        """Construct the representation of a (sub) element in xml.

        :param sub_element_name: a QName or None. If None, the property represents the node itself,
                                 otherwise the sub node with given name.
        :param value_converter: see base class doc.
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
    def _get_element_by_child_name(node: xml_utils.LxmlElement,
                                   sub_element_name: etree_.QName | None,
                                   create_missing_nodes: bool) -> xml_utils.LxmlElement:
        if sub_element_name is None:
            return node
        sub_node = node.find(sub_element_name)
        if sub_node is None:
            if not create_missing_nodes:
                raise ElementNotFoundError(f'Element {sub_element_name} not found in {node.tag}')
            sub_node = etree_.SubElement(node, sub_element_name)  # create this node
        return sub_node

    def remove_sub_element(self, node: xml_utils.LxmlElement):
        if self._sub_element_name is None:
            return
        sub_node = node.find(self._sub_element_name)
        if sub_node is not None:
            node.remove(sub_node)

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in sub element {self._sub_element_name}'


class StringAttributeProperty(_AttributeBase):
    """Python representation is a string."""

    def __init__(self, attribute_name: str,
                 default_py_value: Any = None,
                 implied_py_value: Any = None, is_optional: bool = True):
        super().__init__(attribute_name, StringConverter, default_py_value, implied_py_value, is_optional)


class AnyURIAttributeProperty(StringAttributeProperty):
    """Represents an AnyURIAttribute."""


class CodeIdentifierAttributeProperty(StringAttributeProperty):
    """Represents a CodeIdentifier attribute."""


class HandleAttributeProperty(StringAttributeProperty):
    """Represents a Handle attribute."""


class HandleRefAttributeProperty(StringAttributeProperty):
    """Represents a HandleRef attribute."""


class SymbolicCodeNameAttributeProperty(StringAttributeProperty):
    """Represents a SymbolicCodeName attribute."""


class ExtensionAttributeProperty(StringAttributeProperty):
    """Represents an Extension attribute."""


class LocalizedTextRefAttributeProperty(StringAttributeProperty):
    """Represents a LocalizedTextRef attribute."""


class TimeZoneAttributeProperty(StringAttributeProperty):
    """Represents a TimeZone attribute."""


class EnumAttributeProperty(_AttributeBase):
    """Base class for enum attributes."""

    def __init__(self, attribute_name: str,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, EnumConverter(enum_cls), default_py_value, implied_py_value, is_optional)


class TimestampAttributeProperty(_AttributeBase):
    """Represents a Timestamp attribute.

    XML notation is integer in milliseconds.
    Python is a float in seconds.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class CurrentTimestampAttributeProperty(_AttributeBase):
    """Represents a special Timestamp attribute used for ClockState, it always writes current time to node.

    Setting the value from python is possible, but makes no sense.
    """

    def __init__(self, attribute_name: str,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=TimestampConverter,
                         default_py_value=None, is_optional=is_optional)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        setattr(instance, self._local_var_name, time.time())
        super().update_xml_value(instance, node)


class DecimalAttributeProperty(_AttributeBase):
    """Represents a Decimal attribute."""

    def __init__(self, attribute_name: str,
                 default_py_value: Decimal | None = None,
                 implied_py_value: Decimal | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=DecimalConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QualityIndicatorAttributeProperty(DecimalAttributeProperty):
    """Represents a QualityIndicator attribute, a value between 0 and 1."""


class DurationAttributeProperty(_AttributeBase):
    """Represents a Duration attribute.

    XML notation is integer in milliseconds.
    Python is a float in seconds.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: DurationType | None = None,
                 implied_py_value: DurationType | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=DurationConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class IntegerAttributeProperty(_AttributeBase):
    """Represents an Integer attribute.

    XML notation is an integer, python is an integer.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: int | None = None,
                 implied_py_value: int | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=IntegerConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class UnsignedIntAttributeProperty(IntegerAttributeProperty):
    """Represents an UnsignedInt attribute.

    Python has no unsigned int, therefore this is the same as IntegerAttributeProperty.
    """


class VersionCounterAttributeProperty(UnsignedIntAttributeProperty):
    """Represents a VersionCounter attribute.

    VersionCounter in BICEPS is unsigned long.
    Python has no unsigned long, therefore this is the same as IntegerAttributeProperty.
    """


class ReferencedVersionAttributeProperty(VersionCounterAttributeProperty):
    """Represents an ReferencedVersion attribute."""


class BooleanAttributeProperty(_AttributeBase):
    """Represents a Boolean attribute.

    XML notation is 'true' or 'false'.
    Python is a bool.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: bool | None = None,
                 implied_py_value: bool | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=BooleanConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QNameAttributeProperty(_AttributeBase):
    """Represents a qualified name attribute.

    XML Representation is a prefix:name string, Python representation is a QName.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: etree_.QName | None = None,
                 implied_py_value: etree_.QName | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=ClassCheckConverter(etree_.QName),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any,  # noqa: ARG002
                               node: xml_utils.LxmlElement | None) -> Any:
        xml_value = None if node is None else node.attrib.get(self._attribute_name)
        return None if xml_value is None else text_to_qname(xml_value, node.nsmap)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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
                if self._attribute_name in node.attrib:
                    del node.attrib[self._attribute_name]
            except ElementNotFoundError:
                return
        else:
            xml_value = docname_from_qname(py_value, node.nsmap)
            node.set(self._attribute_name, xml_value)


class _AttributeListBase(_AttributeBase):
    """Base class for a list of values as attribute.

    XML Representation is a string which is a space separated list.
    Python representation is a list of Any (type depends on ListConverter),
    else a list of converted values.
    """

    _converter: ListConverter

    def __init__(self, attribute_name: str,
                 value_converter: ListConverter,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter, is_optional=is_optional)

    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, use the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def init_instance_data(self, instance: Any):
        setattr(instance, self._local_var_name, [])

    def get_py_value_from_node(self, instance: Any,  # noqa: ARG002
                               node: xml_utils.LxmlElement | None) -> list[Any]:
        xml_value = None if node is None else node.attrib.get(self._attribute_name)
        if xml_value is not None:
            split_result = xml_value.split(' ')
            return [self._converter.elem_to_py(val) for val in split_result if val]
        return []

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if not py_value and self.is_optional:  # is None:
            try:
                if self._attribute_name in node.attrib:
                    del node.attrib[self._attribute_name]
            except ElementNotFoundError:
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
    """Base class for a list of strings as attribute.

    XML Representation is a string which is a space separated list.
    Python representation is a list of strings.
    """

    def __init__(self, attribute_name: str, value_converter: DataConverterProtocol | None = None):
        converter = value_converter or ListConverter(ClassCheckConverter(str))
        super().__init__(attribute_name, converter)


class HandleRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of HandleRef attribute."""


class EntryRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of EntryRef attribute."""


class OperationRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of OperationRef attribute."""


class AlertConditionRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of AlertConditionRef attribute."""


class DecimalListAttributeProperty(_AttributeListBase):
    """Represents a list of Decimal attribute.

    XML representation: an attribute string that represents 0...n decimals, separated with spaces.
    Python representation: List of Decimal if attribute is set (can be an empty list!), otherwise None.
    """

    def __init__(self, attribute_name: str):
        super().__init__(attribute_name, ListConverter(DecimalConverter))


class NodeTextProperty(_ElementBase):
    """Represents the text of an XML Element.

    Python representation depends on value converter.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, value_converter,
                         default_py_value,
                         implied_py_value,
                         is_optional)
        self._min_length = min_length

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node.

        :return: None if the element was not found, else result of converter.
        """
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundError:
            return None  # element was not found, return None
        return self._converter.to_py(sub_node.text)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional and self._min_length:
                raise ValueError(f'mandatory value {self._sub_element_name} missing')

            if not self._sub_element_name:
                # update text of this element
                node.text = None
            elif self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            else:
                sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            sub_node.text = self._converter.to_xml(py_value)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class NodeStringProperty(NodeTextProperty):
    """Represents the text of an XML Element.

    Python representation is a string.
    libxml sets text of element to None, if text in xml is empty. In this case the python value is an empty string.
    if the xml element that should contain the text does not exist, the python value is None.
    """

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: str | None = None,
                 implied_py_value: str | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, StringConverter, default_py_value, implied_py_value,
                         is_optional, min_length)


class AnyUriTextElement(NodeStringProperty):
    """For now the same as NodeStringProperty ,but later it could be handy to add uri type checking."""


# class LocalizedTextContentProperty(NodeStringProperty):
#     pass


class NodeEnumTextProperty(NodeTextProperty):
    """Represents the text of an XML Element.

    Python representation is an enum.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, EnumConverter(enum_cls), default_py_value, implied_py_value,
                         is_optional, min_length=1)
        self.enum_cls = enum_cls


class NodeEnumQNameProperty(NodeTextProperty):
    """Represents a qualified name as text of an XML Element.

    Python representation is an Enum of QName, XML is prefix:localname.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, EnumConverter(enum_cls), default_py_value, implied_py_value,
                         is_optional, min_length=1)
        self.enum_cls = enum_cls

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            prefix, localname = sub_node.text.split(':')
            namespace = node.nsmap[prefix]
            q_name = etree_.QName(namespace, localname)
            return self._converter.to_py(q_name)
        except ElementNotFoundError:
            return self._default_py_value

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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
            elif self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            else:
                sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            for prefix, namespace in sub_node.nsmap.items():
                if namespace == py_value.value.namespace:
                    value = f'{prefix}:{py_value.value.localname}'
                    sub_node.text = self._converter.to_xml(value)
                    return
            raise ValueError(f'no prefix for namespace "{py_value.value.namespace}"')


class NodeIntProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: int | None = None,
                 implied_py_value: int | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, IntegerConverter, default_py_value, implied_py_value,
                         is_optional, min_length)

class NodeDecimalProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: Decimal | None = None,
                 implied_py_value: Decimal | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, DecimalConverter, default_py_value, implied_py_value,
                         is_optional, min_length)

class NodeDurationProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: isoduration.DurationType | None = None,
                 implied_py_value: isoduration.DurationType | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, DurationConverter, default_py_value, implied_py_value,
                         is_optional, min_length)


class NodeTextQNameProperty(_ElementBase):
    """The handled data is a single qualified name in the text of an element in the form prefix:localname."""

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: etree_.QName | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ClassCheckConverter(etree_.QName), default_py_value,
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            xml_value = sub_node.text
            if xml_value is not None:
                value = text_to_qname(xml_value, sub_node.nsmap)
                return value
        except ElementNotFoundError:
            pass
        return self._default_py_value

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            elif self.is_optional:
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
            sub_node.text = py_value  # this adds the namesoace to sube_node.nsmap


def _compare_extension(left: xml_utils.LxmlElement, right: xml_utils.LxmlElement) -> bool:
    # SDPi R0019 and R0020 forbid the usage of XML mixed content or xml schema qname type in extensions
    try:
        if left.tag != right.tag:  # compare expanded names
            return False
        if dict(left.attrib) != dict(right.attrib):  # unclear how lxml _Attrib compares
            return False
    except AttributeError:  # right side is not an Element type because expected attributes are missing
        return False

    # ignore comments
    left_children = [child for child in left if not isinstance(child, etree_._Comment)]
    right_children = [child for child in right if not isinstance(child, etree_._Comment)]

    if len(left_children) != len(right_children):  # compare children count
        return False
    if len(left_children) == 0 and len(right_children) == 0:
        if left.text != right.text:  # mixed content is not allowed. only compare text if there are no children
            return False
    return all(map(_compare_extension, left_children, right_children))  # compare children but keep order


class ExtensionLocalValue(list[xml_utils.LxmlElement]):

    compare_method: Callable[[xml_utils.LxmlElement, xml_utils.LxmlElement], bool] = _compare_extension
    """may be overwritten by user if a custom comparison behaviour is required"""

    def __eq__(self, other: Sequence) -> bool:
        try:
            if len(self) != len(other):
                return False
        except TypeError: # len of other cannot be determined
            return False
        return all(self.__class__.compare_method(left, right) for left, right in zip(self, other))

    def __ne__(self, other):
        return not self == other


class ExtensionNodeProperty(_ElementBase):
    """Represents an ext:Extension Element that contains 0...n child elements of any kind.

    The python representation is an ExtensionLocalValue with list of elements.
    """

    def __init__(self, sub_element_name: etree_.QName | None, default_py_value: Any | None = None):
        super().__init__(sub_element_name, ClassCheckConverter(ExtensionLocalValue), default_py_value,
                         is_optional=True)

    def __set__(self, instance: Any, value: Iterable):
        if not isinstance(value, ExtensionLocalValue):
            value = ExtensionLocalValue(value)
        super().__set__(instance, value)

    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, uses the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = ExtensionLocalValue()
            setattr(instance, self._local_var_name, value)
        return value

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:
        """Read value from node."""
        try:
            extension_nodes = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundError:
            return ExtensionLocalValue()
        return ExtensionLocalValue(extension_nodes[:])

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node.

        The Extension Element is only added if there is at least one element available in local list.
        """
        try:
            extension_local_value = getattr(instance, self._local_var_name)
        except AttributeError:
            return  # nothing to add
        if extension_local_value is None or len(extension_local_value) == 0:
            return  # nothing to add
        sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        sub_node.extend(xml_utils.copy_node_wo_parent(x) for x in extension_local_value)


class AnyEtreeNodeProperty(_ElementBase):
    """Represents an Element that contains xml tree of any kind."""

    def __init__(self, sub_element_name: etree_.QName | None, is_optional: bool = False):
        super().__init__(sub_element_name, NullConverter, default_py_value=None,
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundError:
            return None
        return sub_node[:]  # all children

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            elif MANDATORY_VALUE_CHECKING:
                raise ValueError(f'mandatory value {self._sub_element_name} missing')
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if isinstance(py_value, etree_._Element):  # noqa: SLF001
                sub_node.append(py_value)
            else:
                sub_node.extend(py_value)


class SubElementProperty(_ElementBase):
    """Uses a value that has an "as_etree_node" method."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[XMLTypeBase],
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ClassCheckConverter(value_class), default_py_value, implied_py_value,
                         is_optional)
        self.value_class = value_class

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        value = self._default_py_value
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            value_class = self.value_class.value_class_from_node(sub_node)
            value = value_class.from_node(sub_node)
        except ElementNotFoundError:
            pass
        return value

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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
            sub_node = py_value.as_etree_node(self._sub_element_name, node.nsmap, node)
            if hasattr(py_value, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                    and py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))


class ContainerProperty(_ElementBase):
    """ContainerProperty supports xsi:type information from xml and instantiates value accordingly."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[ContainerBase],
                 cls_getter: Callable[[etree_.QName], type],
                 ns_helper: NamespaceHelper,
                 is_optional: bool = False):
        """Construct a ContainerProperty.

        :param sub_element_name: see doc of base class
        :param value_class: Default value class if no xsi:type is found
        :param cls_getter: function that returns a class for xsi:type QName
        :param ns_helper: name space helper that knows current prefixes
        :param is_optional: see doc of base class
        """
        super().__init__(sub_element_name, ClassCheckConverter(value_class), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
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
        except ElementNotFoundError:
            pass
        return value

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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
            sub_node = py_value.mk_node(self._sub_element_name, self._ns_helper, node)
            if py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))


class _ElementListProperty(_ElementBase, ABC):
    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, uses the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def __set__(self, instance, py_value):
        if isinstance(py_value, tuple):
            py_value = list(py_value)
        super().__set__(instance, py_value)

    def init_instance_data(self, instance: Any):
        setattr(instance, self._local_var_name, [])

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return:
        """
        value: list | None = self.get_py_value_from_node(instance, node)
        if value is not None:
            setattr(instance, self._local_var_name, value)


class SubElementListProperty(_ElementListProperty):
    """SubElementListProperty is  a list of values that have an "as_etree_node" method.

    Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: type[XMLTypeBase],
                 is_optional: bool = True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for _node in nodes:
                value_class = self.value_class.value_class_from_node(_node)
                value = value_class.from_node(_node)
                objects.append(value)
            return objects
        except ElementNotFoundError:
            return objects

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is not None:
            for val in py_value:
                sub_node = val.as_etree_node(self._sub_element_name, node.nsmap, node)
                if hasattr(val, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                        and val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class ContainerListProperty(_ElementListProperty):
    """ContainerListProperty is a property with a list of elements, each supports xsi:type information.

    Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[ContainerBase],
                 cls_getter: Callable[[etree_.QName], type],
                 ns_helper: NamespaceHelper,
                 is_optional: bool = True):
        """Construct a list of Containers.

        :param sub_element_name: see doc of base class
        :param value_class: Default value class if no xsi:type is found
        :param cls_getter: function that returns a class for xsi:type QName
        :param ns_helper: name space helper that knows current prefixes
        :param is_optional: see doc of base class
        """
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
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
        except ElementNotFoundError:
            return objects

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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
                sub_node = val.mk_node(self._sub_element_name, self._ns_helper, node)
                if val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class SubElementTextListProperty(_ElementListProperty):
    """SubElementTextListProperty represents a list of strings.

    On xml side every string is a text of a sub element.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: Any,
                 is_optional: bool = True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for _node in nodes:
                objects.append(_node.text)
            return objects
        except ElementNotFoundError:
            return objects

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class SubElementStringListProperty(SubElementTextListProperty):
    """SubElementStringListProperty represents a list of strings.

    On xml side every string is a text of a sub element.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 is_optional: bool = True):
        super().__init__(sub_element_name, str, is_optional=is_optional)


class SubElementHandleRefListProperty(SubElementStringListProperty):
    """Represents a list of Handles."""


class SubElementWithSubElementListProperty(SubElementProperty):
    """Class represents an optional Element that is only present if its value class is not empty.

    value_class must have an is_empty method.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: Any,
                 value_class: type[XMLTypeBase]):
        assert hasattr(value_class, 'is_empty')
        super().__init__(sub_element_name,
                         default_py_value=default_py_value,
                         value_class=value_class)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None or py_value.is_empty():
            return
        self.remove_sub_element(node)
        py_value.as_etree_node(self._sub_element_name, node.nsmap, node)  # creates a sub-node

    def __set__(self, instance: Any, py_value: Any):
        if isinstance(py_value, self.value_class):
            super().__set__(instance, py_value)
        else:
            raise ApiUsageError(f'do not set {self._sub_element_name} directly, use child member!')


class AnyEtreeNodeListProperty(_ElementListProperty):
    """class represents a list of lxml elements."""

    def __init__(self, sub_element_name: etree_.QName | None, is_optional: bool = True):
        super().__init__(sub_element_name,
                         ListConverter(ClassCheckConverter(xml_utils.LxmlElement)),
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        objects = []
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is None:
                return []
            return sub_node[:]
        except ElementNotFoundError:
            return objects

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
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

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class NodeTextListProperty(_ElementListProperty):
    """The handled data is a list of words (string without whitespace). The xml text is the joined list of words."""

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: Any,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)),
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> list[Any] | None:  # noqa: ARG002
        """Read value from node.

        If the expected node does not exist, return _default_py_value (usually None).
        If the node exists, but the text is None, return an empty list.
        """
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node.text is not None:
                return sub_node.text.split()
            return []
        except ElementNotFoundError:
            return self._default_py_value

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            elif self.is_optional:
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
    """The handled data is a list of qualified names.

    The xml text is the joined list of qnames in the form prefix:localname.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(etree_.QName)),
                         is_optional=is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        result = []
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is None:
                return None
            if sub_node.text is not None:
                for q_name_string in sub_node.text.split():
                    result.append(text_to_qname(q_name_string, sub_node.nsmap))
                return result
        except ElementNotFoundError:
            pass
        return self._default_py_value or result

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            elif self.is_optional:
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
    """DateOfBirthProperty represents the DateOfBirth type of BICEPS.

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

    The corresponding Python types are datetime.Date (=> not time point available)
    or datetime.Datetime (with time point attribute).
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(sub_element_name, ClassCheckConverter(datetime, date),
                         default_py_value, implied_py_value, is_optional)

    def get_py_value_from_node(self, instance: Any, node: xml_utils.LxmlElement) -> Any:  # noqa: ARG002
        """Read value from node."""
        try:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
            if sub_node is not None:
                date_string = sub_node.text
                return isoduration.parse_date_time(date_string)
        except ElementNotFoundError:
            pass
        return None

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is None:
            self.remove_sub_element(node)
            return

        date_string = py_value if isinstance(py_value, str) else self._mk_datestring(py_value)
        sub_element = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        sub_element.text = date_string

    @staticmethod
    def mk_value_object(date_string: str) -> isoduration.DateTypeUnion | None:
        """Parse isoduration string."""
        return isoduration.parse_date_time(date_string)

    @staticmethod
    def _mk_datestring(date_object: date | datetime | isoduration.GYear | isoduration.GYearMonth | None) -> str:
        """Create isoduration string."""
        return isoduration.date_time_string(date_object)
