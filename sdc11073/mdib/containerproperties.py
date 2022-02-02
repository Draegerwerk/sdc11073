# -*- coding: utf-8 -*-
'''Container properties represent values in xml nodes. 
These values can be node attributes, node texts or a complete Elements with optional sub nodes.
The properties completely hide the XML nature of data. To serve this purpose, they can convert between XML data types and Python data types.
'''
import datetime
import time
import copy
from lxml import etree as etree_
from sdc11073.dataconverters import TimestampConverter, DecimalConverter, IntegerConverter, BooleanConverter, DurationConverter, NullConverter
import sdc11073.namespaces as namespaces
from sdc11073 import isoduration


class ElementNotFoundException(Exception):
    pass



class _PropertyBase(object):
    ''' Navigates to sub element and handles storage of value in instance.
    
    All Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    getPyValueFromNode: reads the value from XML data and converts it to Python data type.
    updateXMLValue: convert the Python data type to XML type and write it to XML node.
     '''
    def __init__(self, attrname, subElementNames, defaultPyValue, impliedPyValue=None):
        '''

        :param attrname: name of the attribute that an instance represents
        :param subElementNames: a list of element names that define the path to the attribute in a node (can be an empty list)
        :param defaultPyValue: initial value when initialized (should be set for mandatory elements, otherwise created xml might violate schema)
                               and if the xml element does not exist.
        :param impliedPyValue: for optional elements, this is the value that shall be implied if xml element does not exist
                                this value is for information only! Access only via class possible.
        '''
        self._attrname = attrname
        if subElementNames is None:
            self._subElementNames = []
        else:
            self._subElementNames = subElementNames
        if subElementNames is not None:
            localVarName = '_' + '_'.join([s.localname.lower() for s in subElementNames])
        else:
            localVarName = ''
        if attrname is not None: # add attrname
            if isinstance(attrname, etree_.QName):
                localVarName = localVarName + '_'+ attrname.localname
            else:
                localVarName = localVarName + '_'+ attrname.lower()
        self._localVarName = localVarName
        self._defaultPyValue = defaultPyValue
        self.impliedPyValue = impliedPyValue


    def __get__(self, instance, owner):
        ''' returns a python value, uses the locally stored value'''
        if instance is None: # if called via class
            return self
        try:
            value = getattr(instance, self._localVarName)
        except AttributeError:
            value = None
        if value is None:
            if callable(self.impliedPyValue):
                value = self.impliedPyValue()
            else:
                value = self.impliedPyValue
        return value


    def getActualValue(self, instance):
        ''' Returns the actual value without considering default value and implied value, e.g. returns None if no value in xml exists.'''
        try:
            return getattr(instance, self._localVarName)
        except AttributeError:
            return None


    def __set__(self, instance, pyValue):
        '''value is the representation on the program side, e.g a float. '''
        setattr(instance, self._localVarName, pyValue)


    def initInstanceData(self, instance):
        setattr(instance, self._localVarName, copy.copy(self._defaultPyValue))


    def updateXMLValue(self, instance, node):
        # to be defined in derived classes
        raise NotImplementedError


    def getPyValueFromNode(self, node):
        # to be defined in derived classes
        raise NotImplementedError
    
    
    @staticmethod
    def _getElementbyChildNamesList(node, subElementNames, createMissingNodes):
        for n in subElementNames:   
            subNode = node.find(n)
            if subNode is None:
                if not createMissingNodes:
                    raise ElementNotFoundException('Element {} not found in {}, path={}'.format(n, node.tag, subElementNames))
                subNode = etree_.SubElement(node, n) # create this node
            node = subNode
        return node


    def rmLastSubElement(self, node):
        try:
            pNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=False)
        except ElementNotFoundException:
            return
        subNode = pNode.find(self._subElementNames[-1])
        if subNode is not None:
            pNode.remove(subNode)


    def updateFromNode(self, instance, node):
        value = self.getPyValueFromNode(node)
        setattr(instance, self._localVarName, value)

    def __str__(self):
        if self._subElementNames:
            path_string = ', '.join([str(x) for x in self._subElementNames])
            return '{} attribute {} in subelement {}'.format(self.__class__.__name__, self._attrname, path_string)
        else:
            return '{} attribute {}'.format(self.__class__.__name__, self._attrname )



class NotImplementedProperty(_PropertyBase):
    ''' For place holders '''
    
    def __init__(self, attrname, subElementNames=None):
        super(NotImplementedProperty, self).__init__(attrname, subElementNames, None)
    
    def __get__(self, instance, owner):
        return None

    def __set__(self, instance, value):
        raise NotImplementedError



class _ListPropertyBase(_PropertyBase):
    ''' Base class for all classes that have an empty list as default value.
    These classes do not use an implied value'''

    def __init__(self, attrname, subElementNames):
        super(_ListPropertyBase, self).__init__(attrname, subElementNames, None)

    def __get__(self, instance, owner):
        ''' returns a python value, uses the locally stored value'''
        if instance is None: # if called via class
            return self
        try:
            return getattr(instance, self._localVarName)
        except AttributeError:
            setattr(instance, self._localVarName, [])
            return getattr(instance, self._localVarName)

    def initInstanceData(self, instance):
        setattr(instance, self._localVarName, [])


class NodeAttributeProperty(_PropertyBase):
    ''' XML Representation is a string, Python representation is determined by valueConverter.'''
    def __init__(self, attrname, subElementNames=None, valueConverter=None, defaultPyValue=None, impliedPyValue=None):
        super(NodeAttributeProperty, self).__init__(attrname, subElementNames, defaultPyValue, impliedPyValue)
        self._converter = valueConverter if valueConverter is not None else NullConverter


    def getPyValueFromNode(self, node):
        value = self._defaultPyValue
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            xmlValue = subNode.attrib.get(self._attrname)
            if xmlValue is not None:
                value = self._converter.toPy(xmlValue)
        except ElementNotFoundException:
            pass
        return value


    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
                if subNode is None:
                    return
                if self._attrname in subNode.attrib.keys():
                    del subNode.attrib[self._attrname]
            except ElementNotFoundException:
                return
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            xmlValue = self._converter.toXML(pyValue)
            subNode.set(self._attrname, xmlValue)


class NodeAttributeListProperty(_ListPropertyBase):
    ''' XML Representation is a string which is a space separated list'''

    def __init__(self, attrname, subElementNames=None, valueConverter=None):
        super(NodeAttributeListProperty, self).__init__(attrname, subElementNames)
        self._converter = valueConverter if valueConverter is not None else NullConverter

    def getPyValueFromNode(self, node):
        value = self._defaultPyValue
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            xmlValue = subNode.attrib.get(self._attrname)
            if xmlValue is not None:
                value = [h for h in self._converter.toPy(xmlValue).split(' ') if h]
        except ElementNotFoundException:
            pass
        return value

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
                if subNode is None:
                    return
                if self._attrname in subNode.attrib.keys():
                    del subNode.attrib[self._attrname]
            except ElementNotFoundException:
                return
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            xmlValue = ' '.join(self._converter.toXML(pyValue))
            subNode.set(self._attrname, xmlValue)


class XsiTypeAttributeProperty(_PropertyBase):
    ''' XML Representation is a namespace:name string, Python representation is a QName.'''
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(XsiTypeAttributeProperty, self).__init__(attrname, subElementNames, defaultPyValue, impliedPyValue)


    def getPyValueFromNode(self, node):
        '''
        @param node: the etree node as input
        @return: None or a QName
        '''
        value = self._defaultPyValue
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            xmlValue = subNode.attrib.get(self._attrname)
            if xmlValue is not None:
                value = namespaces.txt2QName(xmlValue, node.nsmap)
        except ElementNotFoundException:
            pass
        return value


    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
                if self._attrname in subNode.attrib.keys():
                    del subNode.attrib[self._attrname]
            except ElementNotFoundException:
                return
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            xmlValue = namespaces.docNameFromQName(pyValue, node.nsmap)            
            subNode.set(self._attrname, xmlValue)



class DecimalListAttributeProperty(_ListPropertyBase):
    ''' XML representation: an attribute string that represents 1..n decimals, separated with spaces.
        Python representation: a list of integers and/or floats.
        '''
    def __init__(self, attrname, subElementNames=None):
        super(DecimalListAttributeProperty, self).__init__(attrname, subElementNames)

    def getPyValueFromNode(self, node):
        value = self._defaultPyValue
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            xmlValue = subNode.attrib.get(self._attrname)
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
                subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
                if self._attrname in subNode.attrib.keys():
                    del subNode.attrib[self._attrname]
            except ElementNotFoundException:
                return
            return
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            attrValue = ' '.join([DecimalConverter.toXML(v) for v in pyValue])
            subNode.set(self._attrname, attrValue)



class NodeTextProperty(_PropertyBase):
    ''' The handled data is the text of an element.'''
    def __init__(self, subElementNames=None, valueConverter=None, defaultPyValue=None, impliedPyValue=None, isOptional=True):
        attrname = '_text'
        super(NodeTextProperty, self).__init__(attrname, subElementNames, defaultPyValue, impliedPyValue)
        self._converter = valueConverter if valueConverter is not None else NullConverter
        self._isOptional = isOptional


    def getPyValueFromNode(self, node):
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            xmlValue = subNode.text
            if xmlValue is not None:
                return self._converter.toPy(xmlValue)
        except ElementNotFoundException:
            pass
        return self._defaultPyValue


    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None
        if pyValue is None:
            try:
                parentNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=False)
            except ElementNotFoundException:
                return
            if not self._subElementNames:
                # update text of this element
                node.text = ''
            else:
                if self._isOptional:
                    subNode = parentNode.find(self._subElementNames[-1])
                    if subNode is not None:
                        parentNode.remove(subNode)
                else:
                    subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
                    subNode.text=None
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            value = self._converter.toXML(pyValue)
            subNode.text = value


    def __str__(self):
        if self._subElementNames:
            path_string = ', '.join([str(x) for x in self._subElementNames])
            return '{} in subelement {}'.format(self.__class__.__name__, path_string)
        else:
            return '{} '.format(self.__class__.__name__, )



class NodeTextQNameProperty(_PropertyBase):
    ''' The handled data is a qualified name as in the text of an element'''
    def __init__(self, subElementNames, defaultPyValue=None):
        attrname = None
        super(NodeTextQNameProperty, self).__init__(attrname, subElementNames, defaultPyValue)


    def getPyValueFromNode(self, node):
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
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
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = None

        if pyValue is None:
            try:
                parentNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=False)
            except ElementNotFoundException:
                return
            subNode = parentNode.find(self._subElementNames[-1])
            if subNode is not None:
                parentNode.remove(subNode)
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            value = namespaces.docNameFromQName(pyValue, subNode.nsmap)
            subNode.text = value


class ExtensionNodeProperty(_PropertyBase):
    ''' Represents an ext:Extension Element that contains xml tree of any kind.'''
    def __init__(self, subElementNames=None, defaultPyValue=None):
        if subElementNames is None:
            subElementNames = [namespaces.extTag('Extension')]
        else:
            subElementNames.append(namespaces.extTag('Extension'))
        attrname =  '_ext_ext'
        super(ExtensionNodeProperty, self).__init__(attrname, subElementNames, defaultPyValue)
        self._converter = None


    def getPyValueFromNode(self, node):
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            return subNode # this is the ext:Extension node
        except ElementNotFoundException:
            return None


    def updateXMLValue(self, instance, node):
        try:
            extensionNode = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            extensionNode = None
        if extensionNode is None:
            try:
                parentNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=False)
            except ElementNotFoundException:
                return
            subNode = parentNode.find(self._subElementNames[-1])
            if subNode is not None:
                parentNode.remove(subNode)
        else:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)

            del subNode[:]# delete all children first
            subNode.extend([copy.copy(n) for n in extensionNode])



class SubElementProperty(_PropertyBase):
    ''' uses a value that has an "asEtreeNode" method'''
    def __init__(self, subElementNames, valueClass, defaultPyValue=None, impliedPyValue=None):
        attrname = None
        super(SubElementProperty, self).__init__(attrname, subElementNames, defaultPyValue, impliedPyValue)
        self.valueClass = valueClass

    
    def getPyValueFromNode(self, node):
        value = self._defaultPyValue
        try:
            subNode = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            value = self.valueClass.fromNode(subNode)
        except ElementNotFoundException:
            pass
        return value
        

    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is not None:
            parentElement = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=True)
            self.rmLastSubElement(node)
            parentElement.append(pyValue.asEtreeNode(self._subElementNames[-1], parentElement.nsmap))



class SubElementListProperty(_ListPropertyBase):
    ''' a list of values that have an "asEtreeNode" method. Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel'''
    def __init__(self, subElementNames, cls):
        attrname = None
        super(SubElementListProperty, self).__init__(attrname, subElementNames)
        self._cls = cls


    def getPyValueFromNode(self, node):
        ''' get from node'''
        objects = []
        try:
            pNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=False) # get parent Node
            nodes = pNode.findall(self._subElementNames[-1])
            for n in nodes:
                objects.append(self._cls.fromNode(n))
            return objects
        except ElementNotFoundException:
            return objects

        
    def updateXMLValue(self, instance, node):
        ''' value is a list of objects with "asEtreeNode" method'''
        # remove all existing nodes
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue
            
        pNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1], createMissingNodes=True) # get parent Node
        nodes = pNode.findall(self._subElementNames[-1])
        for n in nodes:
            pNode.remove(n)
        # ... and create new ones
        if pyValue is not None:
            for v in pyValue:
                pNode.append(v.asEtreeNode(self._subElementNames[-1], pNode.nsmap))
        
    def __str__(self):
        if self._subElementNames:
            path_string = ', '.join([str(x) for x in self._subElementNames])
            return '{} datatype {} in subelement {}'.format(self.__class__.__name__, self._cls.__name__, path_string)
        else:
            return '{} datatype {}'.format(self.__class__.__name__, self._cls.__name__ )


class SubElementTextListProperty(_PropertyBase):
    ''' represents a list of strings.'''

    def __init__(self, subElementNames, noEmptySubNode=True):
        '''

        :param subElementNames: path to the text elements
        :param noEmptySubNode: if true, the sub elements are not created if pyValue is None or an empty list.
                Otherwise the subelements except the last one are created.
        '''
        attrname = None
        self._noEmptySubNode = noEmptySubNode
        super(SubElementTextListProperty, self).__init__(attrname, subElementNames, defaultPyValue=None)
        self._defaultPyValue = []

    def getPyValueFromNode(self, node):
        ''' get from node'''
        objects = []
        try:
            pNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1],
                                                     createMissingNodes=False)  # get parent Node
            nodes = pNode.findall(self._subElementNames[-1])
            for n in nodes:
                objects.append(n.text)
            return objects
        except ElementNotFoundException:
            return objects

    def updateXMLValue(self, instance, node):
        ''' value is a list of strings'''
        # remove all existing nodes
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is None:
            return
        elif len(pyValue) == 0 and self._noEmptySubNode:
            return

        pNode = self._getElementbyChildNamesList(node, self._subElementNames[:-1],
                                                 createMissingNodes=True)  # get parent Node
        nodes = pNode.findall(self._subElementNames[-1])
        for n in nodes:
            pNode.remove(n)
        # ... and create new ones
        for v in pyValue:
            child = etree_.SubElement(pNode, self._subElementNames[-1])
            child.text = v

    def __str__(self):
        if self._subElementNames:
            path_string = ', '.join([str(x) for x in self._subElementNames])
            return '{} datatype {} in subelement {}'.format(self.__class__.__name__, self._cls.__name__, path_string)
        else:
            return '{} datatype {}'.format(self.__class__.__name__, self._cls.__name__)

class DateOfBirthProperty(_PropertyBase):
    ''' this represents the DateOfBirth type of BICEPS xml schema draft 10:
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
    '''

    def __init__(self, subElementNames, defaultPyValue=None, impliedPyValue=None):
        attrname = None #subElementNames[-1].localname
        super().__init__(attrname, subElementNames, defaultPyValue, impliedPyValue)

    def getPyValueFromNode(self, node):
        try:
            node = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=False)
            if node is not None:
                date_string = node.text
                return isoduration.parse_date_time(date_string)
        except ElementNotFoundException:
            return None


    def updateXMLValue(self, instance, node):
        try:
            pyValue = getattr(instance, self._localVarName)
        except AttributeError: # set to None (it is in the responsibility of the called method to do the right thing)
            pyValue = self._defaultPyValue

        if pyValue is None:
            self.rmLastSubElement(node)
        else:
            if isinstance(pyValue, str):
                datestring = pyValue # use strings as they are
            else:
                datestring = self._mk_datestring(pyValue)
            subElement = self._getElementbyChildNamesList(node, self._subElementNames, createMissingNodes=True)
            subElement.text = datestring

    @staticmethod
    def mk_value_object(date_string):
        return isoduration.parse_date_time(date_string)

    @staticmethod
    def _mk_datestring(date_object):
        return isoduration.date_time_string(date_object)


class TimestampAttributeProperty(NodeAttributeProperty):
    ''' XML notation is integer in milliseconds.
    Python is a float in seconds.'''
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(TimestampAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=TimestampConverter,
                                                         defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue)


class CurrentTimestampAttributeProperty(NodeAttributeProperty):
    ''' used for ClockState, it always writes current time to node. Setting value from python is possible, but makes no sense.
    '''
    def __init__(self, attrname, subElementNames=None):
        super(CurrentTimestampAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=TimestampConverter, defaultPyValue=None)

    def updateXMLValue(self, instance, node):
        setattr(instance, self._localVarName, time.time())
        super(CurrentTimestampAttributeProperty, self).updateXMLValue(instance, node)


class DecimalAttributeProperty(NodeAttributeProperty):
    ''' XML notation is integer in milliseconds.
    Python is a float in seconds.'''
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(DecimalAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=DecimalConverter,
                                                       defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue)



class DurationAttributeProperty(NodeAttributeProperty):
    ''' XML notation is integer in milliseconds.
    Python is a float in seconds.'''
    
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(DurationAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=DurationConverter,
                                                        defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue)



class IntegerAttributeProperty(NodeAttributeProperty):
    ''' XML notation is integer in milliseconds.
    Python is a float in seconds.'''
    
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(IntegerAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=IntegerConverter,
                                                       defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue)



class BooleanAttributeProperty(NodeAttributeProperty):
    ''' XML notation is integer in milliseconds.
    Python is a float in seconds.'''
    
    def __init__(self, attrname, subElementNames=None, defaultPyValue=None, impliedPyValue=None):
        super(BooleanAttributeProperty, self).__init__(attrname, subElementNames, valueConverter=BooleanConverter,
                                                       defaultPyValue=defaultPyValue, impliedPyValue=impliedPyValue)



ZERO = datetime.timedelta(0)
class UTC(datetime.tzinfo):
    """Fixed offset in minutes east from UTC."""
    def __init__(self, offset_minutes, tzname=None):
        self._offset = datetime.timedelta(minutes=offset_minutes)
        self._tzname = tzname

    def utcoffset(self, dt): #pylint:disable=unused-argument
        return self._offset

    def tzname(self, dt): #pylint:disable=unused-argument
        return self._tzname

    def dst(self, dt): #pylint:disable=unused-argument
        return ZERO
