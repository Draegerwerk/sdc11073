# -*- coding: utf-8 -*-
"""Container properties represent values in xml nodes.
These values can be node attributes, node texts or a complete Elements with optional sub nodes.
The properties completely hide the XML nature of data. To serve this purpose, they can convert between XML data types and Python data types.
"""
import re
import datetime
import time
import copy
from collections import OrderedDict
from lxml import etree as etree_
from sdc11073.dataconverters import TimestampConverter, DecimalConverter, IntegerConverter, BooleanConverter, \
    DurationConverter, NullConverter
import sdc11073.namespaces as namespaces
from sdc11073 import isoduration

# if STRICT_ENUM_ATTRIBUTE is True, EnumAttributeProperty instances will only accept enum values of correct type
# ( Or None if allowed). Otherwise every value is accepted.
STRICT_ENUM_ATTRIBUTE = False

class ElementNotFoundException(Exception):
    pass


class _PropertyBase(object):
    """ Navigates to sub element and handles storage of value in instance.

    All Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    getPyValueFromNode: reads the value from XML data and converts it to Python data type.
    updateXMLValue: convert the Python data type to XML type and write it to XML node.
     """

    def __init__(self, defaultPyValue=None, impliedPyValue=None, isOptional=False):
        """
        :param defaultPyValue: initial value when initialized (should be set for mandatory elements, otherwise created xml might violate schema)
                               and if the xml element does not exist.
        :param impliedPyValue: for optional elements, this is the value that shall be implied if xml element does not exist
                                this value is for information only! Access only via class possible.
        :param isOptional: reflects of this element is optional in schema
        """
        self._defaultPyValue = defaultPyValue
        self.impliedPyValue = impliedPyValue
        self._is_optional = isOptional

    @property
    def isOptional(self):
        return self._is_optional

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._localVarName)
        except AttributeError:
            value = None
        if value is None:
            value = self.impliedPyValue
        return value

    def getActualValue(self, instance):
        """ Returns the actual value without considering default value and implied value, e.g. returns None if no value in xml exists."""
        try:
            return getattr(instance, self._localVarName)
        except AttributeError:
            return None

    def __set__(self, instance, pyValue):
        """value is the representation on the program side, e.g a float. """
        setattr(instance, self._localVarName, pyValue)

    def initInstanceData(self, instance):
        setattr(instance, self._localVarName, copy.copy(self._defaultPyValue))

    def updateXMLValue(self, instance, node):
        # to be defined in derived classes
        raise NotImplementedError

    def getPyValueFromNode(self, instance, node):
        # to be defined in derived classes
        raise NotImplementedError

    def updateFromNode(self, instance, node):
        value = self.getPyValueFromNode(instance, node)
        setattr(instance, self._localVarName, value)




class AttributeProperty(_PropertyBase):
    """ XML Representation is a string, Python representation is determined by valueConverter."""

    def __init__(self, attribute_name, valueConverter=None,
                 defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(defaultPyValue, impliedPyValue, isOptional)
        self._attribute_name = attribute_name
        if isinstance(attribute_name, etree_.QName):
            localVarName = '_attr_' + attribute_name.localname
        else:
            localVarName = '_attr_' + attribute_name.lower()
        self._localVarName = localVarName
        self._converter = valueConverter if valueConverter is not None else NullConverter

    def getPyValueFromNode(self, instance, node):
        value = self._defaultPyValue
        try:
            xmlValue = node.attrib.get(self._attribute_name)
            if xmlValue is not None:
                value = self._converter.toPy(xmlValue)
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xmlValue = self._converter.toXML(pyValue)
            node.set(self._attribute_name, xmlValue)

    def __str__(self):
        return f'{self.__class__.__name__} attribute {self._attribute_name}'


class _NodeProperty(_PropertyBase):
    def __init__(self, subElementName, defaultPyValue=None, impliedPyValue=None, isOptional=False, local_var_prefix=''):
        """
        :param subElementName: a QName or None
        :param defaultPyValue: initial value when initialized (should be set for mandatory elements, otherwise created xml might violate schema)
                               and if the xml element does not exist.
        :param impliedPyValue: for optional elements, this is the value that shall be implied if xml element does not exist
                                this value is for information only! Access only via class possible.
        """
        super().__init__(defaultPyValue, impliedPyValue, isOptional)
        if isinstance(subElementName, (list, tuple)):
            raise RuntimeError('subElementNames must not be a list')
        self._sub_element_name = subElementName
        localVarName = f'_none{local_var_prefix}' if self._sub_element_name is None \
            else f'_{local_var_prefix}{self._sub_element_name.localname.lower()}'
        self._localVarName = localVarName

    @staticmethod
    def _get_element_by_child_name(node, sub_element_name, createMissingNodes):
        if sub_element_name is None:
            return node
        subNode = node.find(sub_element_name)
        if subNode is None:
            if not createMissingNodes:
                raise ElementNotFoundException(f'Element {sub_element_name} not found in {node.tag}')
            subNode = etree_.SubElement(node, sub_element_name)  # create this node
        return subNode

    def rmLastSubElement(self, node):
        subNode = node.find(self._sub_element_name)
        if subNode is not None:
            node.remove(subNode)

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class NotImplementedProperty(AttributeProperty):
    """ For place holders """

    def __get__(self, instance, owner):
        return None

    def __set__(self, instance, value):
        raise NotImplementedError


class StringAttributeProperty(AttributeProperty):
    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, None, defaultPyValue, impliedPyValue, isOptional)


class EnumAttributeProperty(AttributeProperty):
    """ XML Representation is a string, Python representation is a enum."""
    def __init__(self, attribute_name, enum_cls=None, defaultPyValue=None,
                 impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, None, defaultPyValue, impliedPyValue, isOptional)
        self.enum_cls = enum_cls

    def __set__(self, instance, pyValue):
        """value is the representation on the program side, e.g a float. """
        if STRICT_ENUM_ATTRIBUTE:
            if not self.isOptional and pyValue is None and self._defaultPyValue is None:
                raise ValueError(f'None value is not allowed, only {self.enum_cls}')
            elif pyValue is not None and not isinstance(pyValue, self.enum_cls):
                raise ValueError(f'value {pyValue} is not of type {self.enum_cls}')
        super().__set__(instance, pyValue)

    def getPyValueFromNode(self, instance, node):
        value = self._defaultPyValue
        try:
            xmlValue = node.attrib.get(self._attribute_name)
            if xmlValue is not None:
                value = self.enum_cls(xmlValue)
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            if hasattr(pyValue, 'value'):
                xmlValue = pyValue.value
            else:
                xmlValue = pyValue
            node.set(self._attribute_name, xmlValue)


class TimestampAttributeProperty(AttributeProperty):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, valueConverter=TimestampConverter,
                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue, isOptional=isOptional)


class CurrentTimestampAttributeProperty(AttributeProperty):
    """ used for ClockState, it always writes current time to node. Setting value from python is possible, but makes no sense.
    """
    def __init__(self, attribute_name, isOptional=True):
        super().__init__(attribute_name, valueConverter=TimestampConverter,
                         defaultPyValue=None, isOptional=isOptional)

    def updateXMLValue(self, instance, node):
        setattr(instance, self._localVarName, time.time())
        super().updateXMLValue(instance, node)


class DecimalAttributeProperty(AttributeProperty):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""
    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, valueConverter=DecimalConverter,
                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue, isOptional=isOptional)


class DurationAttributeProperty(AttributeProperty):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""
    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, valueConverter=DurationConverter,
                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue, isOptional=isOptional)


class IntegerAttributeProperty(AttributeProperty):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, valueConverter=IntegerConverter,
                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue, isOptional=isOptional)


class BooleanAttributeProperty(AttributeProperty):
    """ XML notation is integer in milliseconds.
    Python is a float in seconds."""

    def __init__(self, attribute_name, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(attribute_name, valueConverter=BooleanConverter,
                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue, isOptional=isOptional)


class XsiTypeAttributeProperty(AttributeProperty):
    """ XML Representation is a namespace:name string, Python representation is a QName."""

    def getPyValueFromNode(self, instance, node):
        """
        @param node: the etree node as input
        @return: None or a QName
        """
        value = self._defaultPyValue
        try:
            xmlValue = node.attrib.get(self._attribute_name)
            if xmlValue is not None:
                value = namespaces.txt2QName(xmlValue, node.nsmap)
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xmlValue = namespaces.docNameFromQName(pyValue, node.nsmap)
            node.set(self._attribute_name, xmlValue)


class NodeAttributeListProperty(AttributeProperty):
    """ XML Representation is a string which is a space separated list"""

    def __init__(self, attribute_name, valueConverter=None):
        super().__init__(attribute_name, valueConverter)

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._localVarName)
        except AttributeError:
            setattr(instance, self._localVarName, [])
            return getattr(instance, self._localVarName)

    def initInstanceData(self, instance):
        setattr(instance, self._localVarName, [])

    def getPyValueFromNode(self, instance, node):
        value = self._defaultPyValue
        try:
            xmlValue = node.attrib.get(self._attribute_name)
            if xmlValue is not None:
                value = [h for h in self._converter.toPy(xmlValue).split(' ') if h]
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
        else:
            xmlValue = ' '.join(self._converter.toXML(pyValue))
            node.set(self._attribute_name, xmlValue)


class HandleRefListAttributeProperty(NodeAttributeListProperty):
    pass


class OperationRefListAttributeProperty(NodeAttributeListProperty):
    pass


class AlertConditionRefListAttributeProperty(NodeAttributeListProperty):
    pass


class DecimalListAttributeProperty(NodeAttributeListProperty):
    """ XML representation: an attribute string that represents 1..n decimals, separated with spaces.
        Python representation: a list of integers and/or floats.
        """

    def __init__(self, attribute_name):
        super().__init__(attribute_name, NullConverter)

    def getPyValueFromNode(self, instance, node):
        value = self._defaultPyValue
        try:
            xmlValue = node.attrib.get(self._attribute_name)
            if xmlValue is not None:
                xmlValues = xmlValue.split()
                values = [DecimalConverter.toPy(v) for v in xmlValues]
                return values
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:
            pyValue = None
        # value is a list of integer/float or None
        if pyValue is None:
            try:
                if self._attribute_name in node.attrib.keys():
                    del node.attrib[self._attribute_name]
            except ElementNotFoundException:
                return
            return
        else:
            attrValue = ' '.join([DecimalConverter.toXML(v) for v in pyValue])
            node.set(self._attribute_name, attrValue)


class NodeTextProperty(_NodeProperty):
    """ The handled data is the text of an element."""

    def __init__(self, subElementName=None, defaultPyValue=None, impliedPyValue=None, isOptional=False):
        super().__init__(subElementName, defaultPyValue, impliedPyValue, isOptional)
        self._converter = NullConverter

    def getPyValueFromNode(self, instance, node):
        try:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=False)
            return subNode.text
        except ElementNotFoundException:
            return self._defaultPyValue

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            else:
                if self.isOptional:
                    subNode = node.find(self._sub_element_name)
                    if subNode is not None:
                        node.remove(subNode)
                else:
                    subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=True)
                    subNode.text = None
        else:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=True)
            subNode.text = pyValue

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class NodeEnumTextProperty(NodeTextProperty):
    def __init__(self, enum_cls, subElementName, defaultPyValue=None, impliedPyValue=None, isOptional=False):
        super().__init__(subElementName, defaultPyValue, impliedPyValue, isOptional)
        self.enum_cls = enum_cls


class NodeTextQNameProperty(_NodeProperty):
    """ The handled data is a qualified name as in the text of an element"""

    def __init__(self, subElementName, defaultPyValue=None, isOptional=False):
        super().__init__(subElementName, defaultPyValue, isOptional=isOptional)

    def getPyValueFromNode(self, instance, node):
        try:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=False)
            xmlValue = subNode.text
            if xmlValue is not None:
                value = namespaces.txt2QName(xmlValue, subNode.nsmap)
                return value
        except ElementNotFoundException:
            pass
        return self._defaultPyValue

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None

        if pyValue is None:
            try:
                parentNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1],
                                                              createMissingNodes=False)
            except ElementNotFoundException:
                return
            subNode = parentNode.find(self._subElementNames[-1])
            if subNode is not None:
                parentNode.remove(subNode)
        else:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=True)
            value = namespaces.docNameFromQName(pyValue, subNode.nsmap)
            subNode.text = value


class _ExtensionLocalValue:
    def __init__(self, value):
        self.value = value or OrderedDict()

    def __eq__(self, other):
        if other is None:
            return len(self.value) == 0
        return self.value == other.value


class ExtensionNodeProperty(_NodeProperty):
    """ Represents an ext:Extension Element that contains xml tree of any kind."""

    def __init__(self, subElementName=None, defaultPyValue=None):
        if subElementName is None:
            subElementName = namespaces.extTag('Extension')
        super().__init__(subElementName, defaultPyValue, isOptional=True, local_var_prefix='ext')
        self._converter = None

    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._localVarName)
        except AttributeError:
            value = None
        if value is None:
            value =_ExtensionLocalValue(None)
            setattr(instance, self._localVarName, value)
        return value

    def getPyValueFromNode(self, instance, node):
        try:
            extension_node = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=False)
        except ElementNotFoundException:
            return None
        values = OrderedDict()
        for n in extension_node:
            try:
                cls = instance.extension_class_lookup.get(n.tag)
            except AttributeError:
                cls = None
            if cls:
                values[n.tag] = cls.fromNode(n)
            else:
                values[n.tag] = n
        return  _ExtensionLocalValue(values)

    def updateXMLValue(self, instance, node):
        try:
            extension_local_value = getattr(instance, self._localVarName)
        except AttributeError:
            extension_local_value = None
        if extension_local_value is None:
            subNode = node.find(self._sub_element_name)
            if subNode is not None:
                node.remove(subNode)
        else:
            if not  extension_local_value.value:
                return
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=True)

            del subNode[:]  # delete all children first

            for tag, val in extension_local_value.value.items():
                if isinstance(val, etree_._Element):
                    _node = val # .extend([copy.copy(n) for n in val])
                else:
                    _node = val.asEtreeNode(tag, node.nsmap)
                subNode.append(copy.copy(_node))


class SubElementProperty(_NodeProperty):
    """ uses a value that has an "asEtreeNode" method"""

    def __init__(self, subElementName, valueClass, defaultPyValue=None, impliedPyValue=None, isOptional=False):
        super().__init__(subElementName, defaultPyValue, impliedPyValue, isOptional)
        self.valueClass = valueClass

    def getPyValueFromNode(self, instance, node):
        value = self._defaultPyValue
        try:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=False)
            value = self.valueClass.fromNode(subNode)
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is not None:
            self.rmLastSubElement(node)
            node.append(pyValue.asEtreeNode(self._sub_element_name, node.nsmap))


class _ElementListProperty(_NodeProperty):
    def __get__(self, instance, owner):
        """ returns a python value, uses the locally stored value"""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._localVarName)
        except AttributeError:
            setattr(instance, self._localVarName, [])
            return getattr(instance, self._localVarName)

    def initInstanceData(self, instance):
        setattr(instance, self._localVarName, [])


class SubElementListProperty(_ElementListProperty):
    """ a list of values that have an "asEtreeNode" method. Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel"""

    def __init__(self, subElementName, valueClass):
        super().__init__(subElementName)
        self.valueClass = valueClass

    def getPyValueFromNode(self, instance, node):
        """ get from node"""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for n in nodes:
                objects.append(self.valueClass.fromNode(n))
            return objects
        except ElementNotFoundException:
            return objects

    def updateXMLValue(self, instance, node):
        """ value is a list of objects with "asEtreeNode" method"""
        # remove all existing nodes
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        nodes = node.findall(self._sub_element_name)
        for n in nodes:
            node.remove(n)
        # ... and create new ones
        if pyValue is not None:
            for v in pyValue:
                node.append(v.asEtreeNode(self._sub_element_name, node.nsmap))

    def __repr__(self):
        return f'{self.__class__.__name__} datatype { self.valueClass.__name__} in subelement {self._sub_element_name}'


class SubElementTextListProperty(_ElementListProperty):
    """ represents a list of strings."""

    def __init__(self, subElementName, noEmptySubNode=True):
        """

        :param subElementNames: path to the text elements
        :param noEmptySubNode: if true, the sub elements are not created if pyValue is None or an empty list.
                Otherwise the subelements except the last one are created.
        """
        self._noEmptySubNode = noEmptySubNode
        super().__init__(subElementName)

    def getPyValueFromNode(self, instance, node):
        """ get from node"""
        objects = []
        try:
            nodes = node.findall(self._sub_element_name)
            for n in nodes:
                objects.append(n.text)
            return objects
        except ElementNotFoundException:
            return objects

    def updateXMLValue(self, instance, node):
        """ value is a list of strings"""
        # remove all existing nodes
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is None:
            return
        elif len(pyValue) == 0 and self._noEmptySubNode:
            return

        nodes = node.findall(self._sub_element_name)
        for n in nodes:
            node.remove(n)
        # ... and create new ones
        for v in pyValue:
            child = etree_.SubElement(node, self._sub_element_name)
            try:
                child.text = v
            except TypeError as ex:
                # re-raise with better info about data
                raise TypeError(f'{ex} in {self}')

    def __str__(self):
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class SubElementWithSubElementListProperty(SubElementProperty):
    """This Represents an Element that is optional and only present if its value class is not empty.
    valueClass must have an is_empty method
    """
    def __init__(self, subElementName, defaultPyValue, valueClass):
        assert hasattr(valueClass, 'is_empty')
        super().__init__(subElementName,
                         defaultPyValue=defaultPyValue,
                         valueClass=valueClass)

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:
            pyValue = self._defaultPyValue

        if pyValue is None or pyValue.is_empty():
            return
        else:
            self.rmLastSubElement(node)
            node.append(pyValue.asEtreeNode(self._sub_element_name, node.nsmap))

    def __set__(self, instance, pyValue):
        _pyValue = getattr(instance, self._localVarName)
        if isinstance(pyValue, self.valueClass):
            super().__set__(instance, pyValue)
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

    def __init__(self, subElementName, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        super().__init__(subElementName, defaultPyValue, impliedPyValue, isOptional)

    def getPyValueFromNode(self, instance, node):
        try:
            subNode = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=False)
            if subNode is not None:
                date_string = subNode.text
                return isoduration.parse_date_time(date_string)
        except ElementNotFoundException:
            return None

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is None:
            self.rmLastSubElement(node)
        else:
            if isinstance(pyValue, str):
                datestring = pyValue  # use strings as they are
            else:
                datestring = self._mk_datestring(pyValue)
            subElement = self._get_element_by_child_name(node, self._sub_element_name, createMissingNodes=True)
            subElement.text = datestring

    @staticmethod
    def mk_value_object(date_string):
        return isoduration.parse_date_time(date_string)

    @staticmethod
    def _mk_datestring(date_object):
        return isoduration.date_time_string(date_object)


ZERO = datetime.timedelta(0)


class UTC(datetime.tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset_minutes, tzname=None):
        self._offset = datetime.timedelta(minutes=offset_minutes)
        self._tzname = tzname

    def utcoffset(self, dt):  # pylint:disable=unused-argument
        return self._offset

    def tzname(self, dt):  # pylint:disable=unused-argument
        return self._tzname

    def dst(self, dt):  # pylint:disable=unused-argument
        return ZERO
