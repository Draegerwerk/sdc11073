""" Implementation of some data types used in Participant Model"""
import decimal
from collections import namedtuple
import traceback
import inspect
import itertools
import enum
import warnings
from typing import Union
from lxml import etree as etree_
from sdc11073 import namespaces
from .mdib import containerproperties  as cp
from math import isclose
'''
Interface of  pmTypes:

fromEtreeNode (class method) is a constructor that is used to create a type object from XML node

asEtreeNode: returns an etree node that represents the object
'''


class PropertyBasedPMType(object):
    """ Base class that assumes all data is defined as containerproperties and _props lists all property names."""

    def asEtreeNode(self, qname, nsmap):
        node = etree_.Element(qname, nsmap=nsmap)
        self._updateNode(node)
        return node


    def _updateNode(self, node):
        for prop_name, prop in self._sortedContainerProperties():
            try:
                prop.updateXMLValue(self, node)
            except Exception as ex:
                raise RuntimeError('In {}.{}, {} could not update: {}'.format(self.__class__.__name__, prop_name, str(prop), traceback.format_exc()))


    def updateFromNode(self, node):
        for dummy, prop in self._sortedContainerProperties():
            prop.updateFromNode(self, node)


    def _sortedContainerProperties(self):
        """
        @return: a list of (name, object) tuples of all GenericProperties ( and subclasses)
        list is created based on _props lists of classes
        """
        ret = []
        classes = inspect.getmro(self.__class__)
        for cls in reversed(classes):
            try:
                names = cls._props  # pylint: disable=protected-access
            except:
                continue
            for name in names:
                obj = getattr(cls, name)
                if obj is not None:
                    ret.append((name, obj))
        return ret


    def __eq__(self, other):
        """ compares all properties"""
        try:
            for name, dummy in self._sortedContainerProperties():
                my_value = getattr(self, name)
                other_value = getattr(other, name)
                if my_value == other_value:
                    continue
                elif (isinstance(my_value,  float) or isinstance(other_value,  float)) and isclose(my_value, other_value):
                    continue # float compare (almost equal)
                else:
                    return False
            return True
        except (TypeError, AttributeError):
            return False

    def __ne__(self, other):
        return not self == other

    @classmethod
    def fromNode(cls, node):
        """ default fromNode Constructor that provides no arguments for class __init__"""
        obj = cls()
        obj.updateFromNode(node)
        return obj


class ElementWithTextOnly(PropertyBasedPMType):
    text = cp.NodeTextProperty()  # this is the text of the node. Here attribute is lower case!
    _props = ['text']
    '''An Element that has no attributes, only a text.'''
    def __init__(self, text):
        self.text = text


    @classmethod
    def fromNode(cls, node):
        text = node.text
        return cls(text)    



class LocalizedText(PropertyBasedPMType):
    text = cp.NodeTextProperty()  # this is the text of the node. Here attribute is lower case!
    Ref = cp.NodeAttributeProperty('Ref')
    Lang = cp.NodeAttributeProperty('Lang')
    Version = cp.IntegerAttributeProperty('Version')
    TextWidth = cp.NodeAttributeProperty('TextWidth') # one of xs, s, m, l, xl, xxl
    _props = ['text', 'Ref', 'Lang', 'Version', 'TextWidth']
    ''' Represents a LocalizedText type in the Participant Model. '''
    def __init__(self, text, lang=None, ref=None, version=None, textWidth=None):
        """
        @param text: a string
        @param lang: a string or None
        @param ref: a string or None
        @param version: an int or None
        @param textWidth: xs, s, m, l, xl, xxl or None
        """
        self.text = text
        self.Lang = lang
        self.Ref = ref
        self.Version = version
        self.TextWidth = textWidth

    def __repr__(self):
        repr_string = 'LocalizedText("{}"'.format(self.text)
        if self.Lang is not None:
            repr_string += ', lang="{}"'.format(self.Lang)
        if self.Ref is not None:
            repr_string += ', ref="{}"'.format(self.Ref)
        if self.Version is not None:
            repr_string += ', version="{}"'.format(self.Version)
        if self.TextWidth is not None:
            repr_string += ', textWidth="{}"'.format(self.TextWidth)
        repr_string += ')'
        return repr_string

    @classmethod
    def fromNode(cls, node):
        text = node.text
        lang = node.get('Lang')
        ref = node.get('Ref')
        version = node.get('Version')
        if version is not None:
            version = int(version)
        textWidth = node.get('TextWidth')
        return cls(text, lang, ref, version, textWidth)


DefaultCodingSystem = 'urn:oid:1.2.840.10004.1.1.1.0.0.1' # ISO/IEC 11073-10101

def _get_default_coding_system():
    """Helper method that allows handling of DefaultCodingSystem modification at runtime.
    By using it implied coding system always is the current value of DefaultCodingSystem."""
    return DefaultCodingSystem


class NotCompareableVersionError(Exception):
    """ This exception says that two coded values cannot be compared, because one has a coding system version, the other one not.
    In that case it is not possible to decide if they are equal."""
    pass


_CodingBase = namedtuple('_CodingBase', 'code codingSystem codingSystemVersion')

class Coding(_CodingBase):
    """ Immutable representation of a coding. Can be used as key in dictionaries"""


    def __new__(cls, code, codingSystem=None, codingSystemVersion=None):
        return super(Coding, cls).__new__(cls,
                                          str(code),
                                          codingSystem or DefaultCodingSystem,
                                          codingSystemVersion)

    def equals(self, other, raiseNotComparableException=False):
        """ different compare method to __eq__, overwriting this one makes Coding unhashable!
         other can be an int, a string, or a Coding.
         Simple comparison with int or string only works if self.codingSystem == DefaultCodingSystem
         and self.codingSystemVersion is None"""
        if isinstance(other, int):
            other = str(other)
        if isinstance(other, str):
            # compare to 11073 coding system where codes are strings
            if self.codingSystem != DefaultCodingSystem:
                return False
            elif self.code != other:
                return False
            elif self.codingSystemVersion is None:
                return True
            else:
                if raiseNotComparableException:
                    raise NotCompareableVersionError('no simple compare, self.codingSystemVersion == {}'.format(self.codingSystemVersion))
                else:
                    return False
        else:
            try:
                if self.code != other.code or self.codingSystem != other.codingSystem:
                    return False
                elif self.codingSystemVersion == other.codingSystemVersion:
                    return True
                elif self.codingSystemVersion is None or other.codingSystemVersion is None:
                    if raiseNotComparableException:
                        raise NotCompareableVersionError('my codingSystem = "{}", other = "{}"'.format(self.codingSystemVersion, other.codingSystemVersion))
                    else:
                        return False
                else:
                    return False
            except AttributeError:
                return False

    @classmethod
    def fromNode(cls, node):
        """ Read Code and CodingSystem attributes of a node (CodedValue). """
        code = node.get('Code')
        codingSystem = node.get('CodingSystem', DefaultCodingSystem)
        codingSystemVersion = node.get('CodingSystemVersion')
        return cls(code, codingSystem, codingSystemVersion)


def mkCoding(code, codingSystem=None, codingSystemVersion=None):
    return Coding(code, codingSystem or DefaultCodingSystem, codingSystemVersion)


class T_Translation(PropertyBasedPMType):
    """
    Translation is part of CodedValue in BICEPS FINAL
    """
    ext_Extension = cp.ExtensionNodeProperty()
    Code = cp.NodeAttributeProperty('Code')
    CodingSystem = cp.NodeAttributeProperty('CodingSystem', impliedPyValue=_get_default_coding_system)
    CodingSystemVersion = cp.NodeAttributeProperty('CodingSystemVersion')

    _props = ['ext_Extension', 'Code', 'CodingSystem', 'CodingSystemVersion']

    def __init__(self, code=None, codingsystem=None, codingSystemVersion=None):
        """

        @param code: a string or an int
        @param codingsystem: anyURI or None, defaults to ISO/IEC 11073-10101 if None
        @param codingSystemVersion: a string, min. length = 1
        """
        self.Code = str(code)
        self.CodingSystem = codingsystem
        self.CodingSystemVersion = codingSystemVersion
        self.coding = None # oberwritten by self._mkCoding()
        self.mkCoding()

    def __repr__(self):
        if self.CodingSystem is None:
            return 'T_Translation("{}")'.format(self.Code)
        elif self.CodingSystemVersion is None:
            return 'T_Translation("{}", codingsystem="{}")'.format(self.Code, self.CodingSystem)
        else:
            return 'T_Translation("{}", codingsystem="{}", codingsystemversion="{}")'\
                .format(self.Code, self.CodingSystem, self.CodingSystemVersion)

    def mkCoding(self):
        if self.Code is not None:
            self.coding = Coding(self.Code, self.CodingSystem, self.CodingSystemVersion)
        else:
            self.coding = None

    def __eq__(self, other):
        """ other can be an int, a string, a CodedValue like object (has "coding" member) or a Coding"""
        if hasattr(other, 'coding'):
            return self.coding.equals(other.coding)
        else:
            return self.coding.equals(other)

    @classmethod
    def fromNode(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        obj.mkCoding()
        return obj


class _CodedValueBase(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    CodingSystemName = cp.SubElementListProperty([namespaces.domTag('CodingSystemName')], cls=LocalizedText)
    ConceptDescription = cp.SubElementListProperty([namespaces.domTag('ConceptDescription')], cls=LocalizedText)
    Code = cp.NodeAttributeProperty('Code')
    CodingSystem = cp.NodeAttributeProperty('CodingSystem', impliedPyValue=_get_default_coding_system)
    CodingSystemVersion = cp.NodeAttributeProperty('CodingSystemVersion')
    SymbolicCodeName = cp.NodeAttributeProperty('SymbolicCodeName')
    Type = cp.XsiTypeAttributeProperty(namespaces.QN_TYPE)
    _props = ['ext_Extension', 'CodingSystemName', 'ConceptDescription',
              'Code', 'CodingSystem', 'CodingSystemVersion', 'SymbolicCodeName', 'Type']
    # aliases for backward compatibility
    codingSystemNames = CodingSystemName
    conceptDescriptions = ConceptDescription
    codingSystem = CodingSystem

    def __init__(self, code, codingsystem=None, codingSystemVersion=None, codingSystemNames=None, conceptDescriptions=None, symbolicCodeName=None):
        """
        @param code: a string or an int
        @param codingsystem: anyURI or None, defaults to ISO/IEC 11073-10101 if None
        @param codingSystemVersion: a string, min. length = 1
        @param codingSystemNames: a list of LocalizedText objects or None
        @param conceptDescriptions: a list of LocalizedText objects or None
        @param symbolicCodeName: a string, min. length = 1 or None
        """
        self.Code = str(code)
        self.CodingSystem = codingsystem
        self.CodingSystemVersion = codingSystemVersion
        self.CodingSystemName = [] if codingSystemNames is None else codingSystemNames
        self.ConceptDescription = [] if conceptDescriptions is None else conceptDescriptions
        self.SymbolicCodeName = symbolicCodeName
        self.coding = None # oberwritten by self._mkCoding()
        self.mkCoding()

    def mkCoding(self):
        if self.Code is not None:
            self.coding = Coding(self.Code, self.CodingSystem, self.CodingSystemVersion)
        else:
            self.coding = None

    def __repr__(self):
        if self.CodingSystem is None:
            return 'CodedValue("{}")'.format(self.Code)
        elif self.CodingSystemVersion is None:
            return 'CodedValue("{}", codingsystem="{}")'.format(self.Code, self.CodingSystem)
        else:
            return 'CodedValue("{}", codingsystem="{}", codingsystemversion="{}")'\
                .format(self.Code, self.CodingSystem, self.CodingSystemVersion)

    @classmethod
    def fromNode(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        obj.mkCoding()
        return obj


class CodedWithTranslations(_CodedValueBase):
    Translation = cp.SubElementListProperty([namespaces.domTag('Translation')], cls=_CodedValueBase)
    _props = ['Translation']

    def __eq__(self, other):
        """ other can be an int, a string, a CodedValue like object (has "coding" member) or a Coding"""
        if hasattr(other, 'coding'):
            return self.coding == other.coding
        else:
            return self.coding == other


class CodedValue(_CodedValueBase):
    Translation = cp.SubElementListProperty([namespaces.domTag('Translation')], cls=T_Translation)
    _props = ['Translation']

    def __eq__(self, other):
        """ This operator handles not comparable versions as different versions

        :param other: int, str or another CodedValue
        """
        if isinstance(other, (int, str)):
            return have_matching_codes(self, Coding(other))
        return have_matching_codes(self, other)

    def equals(self, other, raiseNotComparableException=True):
        """
        Compare this CodedValue with another one.
        A simplified compare with an int or string is also possible, it assumes DefaultCodingSystem and no CodingSystemVersion
        :param other: int, str or another CodedValue
        :param raiseNotComparableException: if False, not comparable versions are handled as different versions
                        if True, a NotCompareableVersionError is thrown if any of the Codings are not comparable
        :return: boolean
        """
        warnings.warn('equals is deprecated, use function have_matching_codes', DeprecationWarning)
        if isinstance(other, self.__class__):
                # Two CodedValue objects C1 and C2 are equivalent, if there exists a CodedValue object T1 in C1/pm:Translation
                # and a CodedValue object T2 in C2/pm:Translation such that T1 and T2 are equivalent, C1 and T2 are equivalent, or C2 and T1 are equivalent.
                foundMatch = False
                not_comparables = []
                my_codes = [self]  # C1
                my_codes.extend(self.Translation)  # all T1
                other_codes = [other]  # C2
                other_codes.extend(other.Translation)  # all T2
                for left, right in itertools.product(my_codes, other_codes):
                    try:
                        if left.coding.equals(right.coding, raiseNotComparableException):
                            if not raiseNotComparableException:
                                return True
                            else:
                                foundMatch = True
                    except NotCompareableVersionError as ex:
                        not_comparables.append(str(ex))
                if not_comparables:
                    raise NotCompareableVersionError(';'.join(not_comparables))
                else:
                    return foundMatch
        else:
            # simplified compare: compare to 11073 coding system where codes are integers, ignore translations
            return self.coding.equals(other, raiseNotComparableException)

    @classmethod
    def fromNode(cls, node):
        nodeType = None
        nodeTypeText = node.get(namespaces.QN_TYPE)
        if nodeTypeText is not None:
            nodeType = namespaces.txt2QName(node.get(namespaces.QN_TYPE), node.nsmap)
        if nodeType == namespaces.domTag('CodedWithTranslations'):
            cls = CodedWithTranslations  # pylint: disable=self-cls-assignment

        obj = cls(None)
        obj.updateFromNode(node)
        obj.mkCoding()
        return obj


def have_matching_codes(code_a: Union[CodedValue, Coding], code_b: Union[CodedValue, Coding]) -> bool:
    """A CodedValue is a set of codings (coding plus translations), a Coding is a set with only one element.
     Function returns false if no coding is found in both sets, otherwise True."""
    codes_a = set()
    codes_b = set()
    for the_set, the_code in [(codes_a, code_a), (codes_b, code_b)]:
        try:
            the_set.add(the_code.coding)
            if the_code.Translation is not None:
                for tr in the_code.Translation:
                    the_set.add(tr.coding)
        except AttributeError:
            the_set.add(the_code)
    common_codes = codes_a.intersection(codes_b)
    return len(common_codes) > 0


class Annotation(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty([namespaces.domTag('Type')], valueClass=CodedValue)
    _props = ['ext_Extension', 'Type']
    
    codedValue = Type
    ''' An Annotation contains a Type Element that is a CodedValue.
    This is intended as an immutable object. After it has been created, no modification shall be done. '''
    def __init__(self, codedValue):
        self.Type = codedValue
        self.coding = codedValue.coding

    @classmethod
    def fromNode(cls, node):
        typeNode = node.find(namespaces.domTag('Type'))
        codedValue = CodedValue.fromNode(typeNode)
        return cls(codedValue)


class OperationGroup(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty([namespaces.domTag('Type')], valueClass=CodedValue)
    OperatingMode = cp.NodeAttributeProperty('OperatingMode')
    Operations = cp.NodeAttributeListProperty('Operations') #pm:OperationRef
    _props = ['ext_Extension', 'Type', 'OperatingMode', 'Operations']

    def __init__(self, codedValue=None, operatingMode=None, operations=None):
        """
        @param codedValue: a CodedValue instances or None
        @param operatingMode:  xsd:string string
        @param operations: a xsd:string
        """
        self.Type = codedValue
        self.OperatingMode = operatingMode
        self.Operations = operations

    @classmethod
    def fromNode(cls, node):
        ret = cls()
        ret.updateFromNode(node)
        return ret


class InstanceIdentifier(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty([namespaces.domTag('Type')], valueClass=CodedValue)
    IdentifierName = cp.SubElementListProperty([namespaces.domTag('IdentifierName')], cls = LocalizedText)
    Root = cp.NodeAttributeProperty('Root', defaultPyValue='biceps.uri.unk') # xsd:anyURI string, default is defined in R0135
    Extension = cp.NodeAttributeProperty('Extension') # a xsd:string
    _props = ('ext_Extension', 'Type', 'IdentifierName', 'Root', 'Extension')

    def __init__(self, root, type_codedValue=None, identifierNames=None, extensionString=None):
        """
        @param root:  xsd:anyURI string
        @param type_codedValue: a CodedValue instances or None
        @param identifierNames: a list of LocalizedText instances or None
        @param extensionString: a xsd:string
        """
        self.Root = root
        self.Type = type_codedValue
        self.IdentifierName = [] if identifierNames is None else identifierNames
        self.Extension = extensionString
        self.node = None


    @classmethod
    def fromNode(cls, node):
        root = None
        extensionString = None
        type_codedValue = None
        identifierNames = None
        ret = cls(root, type_codedValue=type_codedValue, identifierNames=identifierNames, extensionString=extensionString)
        ret.updateFromNode(node)
        ret.node = node 
        return ret

    def __repr__(self):
        return 'InstanceIdentifier(root={!r}, Type={} ext={!r})'.format(self.Root, self.Type, self.Extension)


class Range(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Lower = cp.DecimalAttributeProperty('Lower') # optional, an integer or float
    Upper = cp.DecimalAttributeProperty('Upper') # optional, an integer or float
    StepWidth = cp.DecimalAttributeProperty('StepWidth') # optional, an integer or float
    RelativeAccuracy = cp.DecimalAttributeProperty('RelativeAccuracy') # optional, an integer or float
    AbsoluteAccuracy = cp.DecimalAttributeProperty('AbsoluteAccuracy') # optional, an integer or float
    _props= ['ext_Extension', 'Lower', 'Upper', 'StepWidth', 'RelativeAccuracy', 'AbsoluteAccuracy']
    
    def __init__(self, lower=None, upper=None, stepWidth=None, relativeAccuracy=None, absoluteAccuracy=None):
        """
        @param lower: The including lower bound of the range. A value as float or integer, can be None
        @param upper: The including upper bound of the range. A value as float or integer, can be None
        @param stepWidth: The numerical distance between two values in the range of the given upper and lower bound. A value as float or integer, can be None
        @param relativeAccuracy: Maximum relative error in relation to the correct value within the given range. A value as float or integer, can be None
        @param absoluteAccuracy: Maximum absolute error in relation to the correct value within the given range. A value as float or integer, can be None
        """
        self.Lower = lower
        self.Upper = upper
        self.StepWidth = stepWidth
        self.RelativeAccuracy = relativeAccuracy
        self.AbsoluteAccuracy = absoluteAccuracy

    def __repr__(self):
        return 'Range (Lower={!r}, Upper={!r}, StepWidth={!r}, RelativeAccuracy={!r}, AbsoluteAccuracy={!r})'\
            .format(self.Lower, self.Upper, self.StepWidth, self.RelativeAccuracy, self.AbsoluteAccuracy)


class Measurement(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    MeasurementUnit = cp.SubElementProperty([namespaces.domTag('MeasurementUnit')], valueClass=CodedValue) # mandatory
    MeasuredValue = cp.DecimalAttributeProperty('MeasuredValue')
    _props = ['ext_Extension', 'MeasurementUnit', 'MeasuredValue']
    
    def __init__(self, value, unit):
        """
        @param value: a value as string, float or integer
        @param unit: a CodedValue instance
        """
        self.MeasuredValue = value
        self.MeasurementUnit = unit

    @classmethod
    def fromNode(cls, node):
        value = node.get('MeasuredValue')
        if value is not None:
            value = float(value)
        unit = None
        unitNode = node.find(namespaces.domTag('MeasurementUnit'))
        if unitNode is not None:
            unit = CodedValue.fromNode(unitNode)
        return cls(value, unit)

    def __repr__(self):
        return 'Measurement(value={!r}, Unit={!r})'.format(self.MeasuredValue, self.MeasurementUnit)


class AllowedValue(PropertyBasedPMType):
    Value = cp.NodeTextProperty([namespaces.domTag('Value')], isOptional=False)
    Type = cp.SubElementProperty([namespaces.domTag('Type')], valueClass=CodedValue)
    _props=['Value', 'Type']
    typeCoding = Type
    value = Value
    
    def __init__(self, valueString, typeCoding=None):
        """One AllowedValue of a EnumStringMetricDescriptor. It has up to two sub elements "Value" and "Type"(optional).
        A StringEnumMetricDescriptor has a list of AllowedValues.
        @param valueString: a string
        @param typeCoding: an optional CodedValue instance
        """
        self.Value = valueString
        self.Type = typeCoding

    @classmethod
    def fromNode(cls, node):
        valueString = node.find(namespaces.domTag('Value')).text
        typeNode = node.find(namespaces.domTag('Type'))
        if typeNode is None:
            typeCoding = None
        else:
            typeCoding = CodedValue.fromNode(typeNode)
        return cls(valueString, typeCoding)


class AbstractMetricValue(PropertyBasedPMType):
    """ This is the base class for metric values inside metric states"""
    ext_Extension = cp.ExtensionNodeProperty()
    StartTime = cp.TimestampAttributeProperty('StartTime') # time.time() value (float)
    StopTime = cp.TimestampAttributeProperty('StopTime') # time.time() value (float)
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime') # time.time() value (float)
    MQ_Extension = cp.ExtensionNodeProperty([namespaces.domTag('MetricQuality')])
    Validity = cp.NodeAttributeProperty('Validity', [namespaces.domTag('MetricQuality')]) # pm:MeasurementValidity
    Mode = cp.NodeAttributeProperty('Mode', [namespaces.domTag('MetricQuality')], impliedPyValue='Real') # pm:GenerationMode
    Qi = cp.DecimalAttributeProperty('Qi', [namespaces.domTag('MetricQuality')], impliedPyValue=1) # pm:QualityIndicator
    Annotation = cp.SubElementListProperty([namespaces.domTag('Annotation')], Annotation)
    _props = ('ext_Extension', 'StartTime', 'StopTime', 'DeterminationTime', 'MQ_Extension', 'Validity', 'Mode', 'Qi', 'Annotation')
    
    Annotations = Annotation

    def __init__(self, nsmapper, node=None):
        self._nsmapper = nsmapper
        # attributes of root node
        self.node = node
        if node is not None:
            self.updateFromNode(node)
        else:
            self.Validity = 'Vld' # mandatory value, for convenience it is preset to Vld

    def updateFromNode(self, node):
        for dummy, prop in self._sortedContainerProperties():
            prop.updateFromNode(self, node)
        self.node = node    

    def asEtreeNode(self, qname, nsmap):
        node = super(AbstractMetricValue, self).asEtreeNode(qname, nsmap)
        node.set(namespaces.QN_TYPE, namespaces.docNameFromQName(self.QType, nsmap))
        return node

    @classmethod
    def fromNode(cls, node):
        obj = cls(node.nsmap, node)
        return obj


class NumericMetricValue(AbstractMetricValue):
    QType = namespaces.domTag('NumericMetricValue')
    Value = cp.DecimalAttributeProperty('Value') # an integer or float
    _props = ('Value',)    

    def __repr__(self):
        return '{} Validity={} Value={} DeterminationTime={}'.format(self.__class__.__name__,
                                                                     self.Validity,
                                                                     self.Value,
                                                                     self.DeterminationTime)

    def __eq__(self, other):
        """ compares all properties, special handling of Value member"""
        try:
            for name, dummy in self._sortedContainerProperties():
                if name == 'Value':
                    my_value = getattr(self, name)
                    other_value = getattr(other, name)
                    if my_value is None and other_value is None:
                        continue
                    elif my_value is None or other_value is None:
                        return False  # only one is None, they are not equal
                    else:
                        if decimal.Decimal(str(my_value)) != decimal.Decimal(str(other_value)):
                            return False
                else:
                    if getattr(self, name) == getattr(other, name):
                        continue
                    else:
                        return False
            return True
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other


class StringMetricValue(AbstractMetricValue):
    QType = namespaces.domTag('StringMetricValue')
    Value = cp.NodeAttributeProperty('Value')  # a string
    _props = ('Value',)    

    def __repr__(self):
        return '{} Validity={} Value={} DeterminationTime={}'.format(self.__class__.__name__,
                                                                     self.Validity,
                                                                     self.Value,
                                                                     self.DeterminationTime)


class ApplyAnnotation(PropertyBasedPMType):
    AnnotationIndex = cp.IntegerAttributeProperty('AnnotationIndex')
    SampleIndex = cp.IntegerAttributeProperty('SampleIndex')
    _props = ['AnnotationIndex', 'SampleIndex']

    def __init__(self, annotationIndex, sampleIndex):
        self.AnnotationIndex = annotationIndex
        self.SampleIndex = sampleIndex   

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        cls.AnnotationIndex.updateFromNode(obj, node)
        cls.SampleIndex.updateFromNode(obj, node)
        return obj

    def __repr__(self):
        return '{} AnnotationIndex={} SampleIndex={}'.format(self.__class__.__name__, self.AnnotationIndex, self.SampleIndex)


class SampleArrayValue(AbstractMetricValue):
    QType = namespaces.domTag('SampleArrayValue')
    Samples = cp.DecimalListAttributeProperty('Samples') # list of xs:decimal types 
    ApplyAnnotations = cp.SubElementListProperty([namespaces.domTag('ApplyAnnotation')], ApplyAnnotation)
    _props = ('Samples', 'ApplyAnnotations')    

    def __repr__(self):
        return '{} Samples={} ApplyAnnotations={}'.format(self.__class__.__name__, self.Samples, self.ApplyAnnotations)

    def __eq__(self, other):
        """ compares all properties, special handling of Value member"""
        try:
            for name, dummy in self._sortedContainerProperties():
                if name == 'Samples':
                    own_sample = getattr(self, name)
                    other_sample = getattr(other, name)
                    # avoid None values; treat None and empty list as equal
                    if own_sample is None:
                        own_sample = []
                    if other_sample is None:
                        other_sample = []
                    if len(own_sample) != len(other_sample):
                        return False
                    for pair in zip(own_sample, other_sample):
                        if decimal.Decimal(str(pair[0])) != decimal.Decimal(str(pair[1])):
                            return False
                else:
                    if getattr(self, name) == getattr(other, name):
                        continue
                    else:
                        return False
            return True
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other


class RemedyInfo(PropertyBasedPMType):
    """An Element that has
         0..1 Subelement "Extension" (not handled here)
         0..n SubElements "Description" type=pm:LocalizedText."""
    ext_Extension = cp.ExtensionNodeProperty()
    Description = cp.SubElementListProperty([namespaces.domTag('Description')], cls = LocalizedText)
    _props = ['ext_Extension', 'Description']

    def __init__(self, descriptions=None):
        """
        @param descriptions : a list of LocalizedText objects or None
        """
        if descriptions:
            self.Description = descriptions
        

class CauseInfo(PropertyBasedPMType):
    """An Element that has
         0..1 Subelement "RemedyInfo", type = pm:RemedyInfo
         0..n SubElements "Description" type=pm:LocalizedText."""
    ext_Extension = cp.ExtensionNodeProperty()
    RemedyInfo = cp.SubElementProperty([namespaces.domTag('RemedyInfo')], valueClass=RemedyInfo)
    Description = cp.SubElementListProperty([namespaces.domTag('Description')], cls=LocalizedText)
    _props = ['ext_Extension', 'RemedyInfo', 'Description']

    def __init__(self, remedyInfo, descriptions):
        """
        @param remedyInfo: a RemedyInfo instance or None
        @param descriptions : a list of LocalizedText objects or None
        """
        self.RemedyInfo = remedyInfo
        self.Description = descriptions

    @classmethod
    def fromNode(cls, node):
        remedyInfoNode = node.find(namespaces.domTag('RemedyInfo'))
        if remedyInfoNode is not None:
            remedyInfo = RemedyInfo.fromNode(remedyInfoNode)
        else:
            remedyInfo = None
        descriptions = []
        descriptionNodes = node.findall(namespaces.domTag('Description'))
        for d in descriptionNodes:
            descriptions.append(LocalizedText.fromNode(d))
        return cls(remedyInfo, descriptions)    


class Argument(PropertyBasedPMType):
    """An Element that has
         1 Subelement "ArgName", type = pm:CodedValue
         1 SubElement "Arg" type=QName."""
    ArgName = cp.SubElementProperty([namespaces.domTag('ArgName')], valueClass=CodedValue)
    Arg = cp.NodeTextQNameProperty([namespaces.domTag('Arg')])
    _props = ['ArgName', 'Arg']
    def __init__(self, argName, arg):
        """
        @param argName: a CodedValue instance
        @param arg : etree_.QName instance
        """
        self.ArgName = argName
        self.Arg = arg

    @classmethod
    def fromNode(cls, node):
        argNameNode = node.find(namespaces.domTag('ArgName'))
        argName = CodedValue.fromNode(argNameNode)
        argNode = node.find(namespaces.domTag('Arg'))
        arg_QName = namespaces.txt2QName(argNode.text, node.nsmap)
        return cls(argName, arg_QName)    

    def __repr__(self):
        return 'Argument(argName={}, arg={})'.format(self.ArgName, self.Arg)


class PhysicalConnectorInfo(PropertyBasedPMType):
    """PhysicalConnectorInfo defines a number in order to allow to guide the clinical user for a failure,
    e.g., in case of a disconnection of a sensor or an ultrasonic handpiece.
    Only in BICEPS final!"""
    ext_Extension = cp.ExtensionNodeProperty()
    Label = cp.SubElementListProperty([namespaces.domTag('Label')], cls=LocalizedText) # A human-readable label that describes the physical connector.
    Number = cp.IntegerAttributeProperty('Number')# Number designates the connector number of the physical connector.
    _props = ['ext_Extension', 'Label', 'Number']

    def __init__(self, labels, number):
        """
        @param labels: a  list of LocalizedText
        @param number : an integer
        """
        self.Label = labels
        self.Number = number

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        cls.Label.updateFromNode(obj, node)
        cls.Number.updateFromNode(obj, node)
        return obj

    def __repr__(self):
        return 'PhysicalConnectorInfo(label={}, number={})'.format(self.Label, self.Number)


class SystemSignalActivation(PropertyBasedPMType):
    Manifestation = cp.NodeAttributeProperty('Manifestation', defaultPyValue='Oth') # required, pmtypes.AlertSignalManifestation
    State = cp.NodeAttributeProperty('State', defaultPyValue='On')                 # required,  pmtypes.AlertActivation
    _props = ['Manifestation', 'State']

    def __init__(self, manifestation, state):
        """
        @param manifestation: a pmtypes.AlertSignalManifestation value
        @param state : a pmtypes.AlertActivation value
        """
        self.Manifestation = manifestation
        self.State = state

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        obj.updateFromNode(node)
        return obj

    def __repr__(self):
        return 'SystemSignalActivation(Manifestation={}, State={})'.format(self.Manifestation, self.State)


class ProductionSpecification(PropertyBasedPMType):
    SpecType = cp.SubElementProperty([namespaces.domTag('SpecType')], valueClass=CodedValue)
    ProductionSpec = cp.NodeTextProperty([namespaces.domTag('ProductionSpec')], isOptional=False)
    ComponentId = cp.SubElementProperty([namespaces.domTag('ComponentId')], valueClass=InstanceIdentifier) # optional
    _props = ['SpecType', 'ProductionSpec', 'ComponentId']

    def __init__(self, spectype, productionspec, componentid=None):
        """
        @param spectype: a pmtypes.CodedValue value
        @param productionspec: a string
        @param componentid : a pmtypes.InstanceIdentifier value
        """
        self.SpecType = spectype
        self.ProductionSpec = productionspec
        self.ComponentId = componentid

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        obj.updateFromNode(node)
        return obj


class BaseDemographics(PropertyBasedPMType):
    Givenname = cp.NodeTextProperty([namespaces.domTag('Givenname')])
    Middlename = cp.SubElementListProperty([namespaces.domTag('Middlename')], cls=ElementWithTextOnly)  # 0...n Elements
    Familyname = cp.NodeTextProperty([namespaces.domTag('Familyname')])
    Birthname = cp.NodeTextProperty([namespaces.domTag('Birthname')])
    Title = cp.NodeTextProperty([namespaces.domTag('Title')])
    _props = ['Givenname', 'Middlename', 'Familyname', 'Birthname', 'Title']

    def __init__(self, givenname=None, middlenames=None, familyname=None, birthname=None, title=None):
        self.Givenname = givenname
        if isinstance(middlenames, str):
            self.Middlename = [middlenames]
        else:
            self.Middlename = middlenames or []
        self.Familyname = familyname
        self.Birthname = birthname
        self.Title = title


class PersonReference(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Identification = cp.SubElementListProperty([namespaces.domTag('Identification')], cls=InstanceIdentifier) # 1...n
    Name = cp.SubElementProperty([namespaces.domTag('Name')], valueClass=BaseDemographics) # optional
    _props = ['ext_Extension', 'Identification', 'Name']

    def __init__(self, identifications=None, name=None):
        """
        :param identifications: a list of InstanceIdentifier objects
        :param name: a BaseDemographics object
        """
        if identifications:
            self.Identification = identifications
        self.Name = name


class LocationDetail(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    PoC = cp.NodeAttributeProperty('PoC')
    Room = cp.NodeAttributeProperty('Room')
    Bed = cp.NodeAttributeProperty('Bed')
    Facility = cp.NodeAttributeProperty('Facility')
    Building = cp.NodeAttributeProperty('Building')
    Floor = cp.NodeAttributeProperty('Floor')
    _props = ('ext_Extension', 'PoC', 'Room', 'Bed', 'Facility', 'Building', 'Floor')

    def __init__(self, poc=None, room=None, bed=None, facility=None, building=None, floor=None):
        self.PoC = poc
        self.Room = room
        self.Bed = bed
        self.Facility = facility
        self.Building = building
        self.Floor = floor


class LocationReference(PropertyBasedPMType): # BICEPS Final
    Identification = cp.SubElementListProperty([namespaces.domTag('Identification')], cls=InstanceIdentifier) # 1...n
    LocationDetail = cp.SubElementProperty([namespaces.domTag('LocationDetail')], valueClass=LocationDetail) # optional
    _props = ['Identification', 'LocationDetail']

    def __init__(self, identifications=None, locationdetail=None):
        if identifications:
            self.Identification = identifications
        self.LocationDetail = locationdetail


class PersonParticipation(PersonReference):
    Role = cp.SubElementListProperty([namespaces.domTag('Role')], cls=CodedValue) #0...n
    _props = ['Role',]

    def __init__(self, identifications=None, name=None, roles=None):
        super(PersonParticipation, self).__init__(identifications, name)
        if roles:
            self.Role = roles


class ClinicalInfo(PropertyBasedPMType):
    Type = cp.SubElementProperty([namespaces.domTag('Type')], valueClass=CodedValue) # optional
    Description = cp.SubElementListProperty([namespaces.domTag('Description')], cls=LocalizedText) #0...n
    RelatedMeasurement = cp.SubElementListProperty([namespaces.domTag('RelatedMeasurement')], cls=Measurement) #0...n
    _props = ['Type', 'Description', 'RelatedMeasurement']

    def __init__(self, typecode=None, descriptions=None, relatedmeasurements=None):
        """
        :param typecode: a CodedValue Instance
        :param descriptions: a list of LocalizedText objects
        :param relatedmeasurements: a list of Measurement objects
        """
        self.Type = typecode
        if descriptions:
            self.Description = descriptions
        if relatedmeasurements:
            self.RelatedMeasurement = relatedmeasurements


class ImagingProcedure(PropertyBasedPMType):
    AccessionIdentifier = cp.SubElementProperty([namespaces.domTag('AccessionIdentifier')], valueClass=InstanceIdentifier) # mandatory
    RequestedProcedureId = cp.SubElementProperty([namespaces.domTag('RequestedProcedureId')], valueClass=InstanceIdentifier) # mandatory
    StudyInstanceUid = cp.SubElementProperty([namespaces.domTag('StudyInstanceUid')], valueClass=InstanceIdentifier) # mandatory
    ScheduledProcedureStepId = cp.SubElementProperty([namespaces.domTag('ScheduledProcedureStepId')], valueClass=InstanceIdentifier) # mandatory
    Modality = cp.SubElementProperty([namespaces.domTag('Modality')], valueClass=CodedValue) # optional
    ProtocolCode = cp.SubElementProperty([namespaces.domTag('ProtocolCode')], valueClass=CodedValue) # optional
    _props = ['AccessionIdentifier', 'RequestedProcedureId', 'StudyInstanceUid', 'ScheduledProcedureStepId',
              'Modality', 'ProtocolCode']

    def __init__(self, accessionidentifier, requestedprocedureid, studyinstanceuid, scheduledprocedurestepid,
                 modality=None, protocolcode=None):
        self.AccessionIdentifier = accessionidentifier
        self.RequestedProcedureId = requestedprocedureid
        self.StudyInstanceUid = studyinstanceuid
        self.ScheduledProcedureStepId = scheduledprocedurestepid
        self.Modality = modality
        self.ProtocolCode = protocolcode

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None, None, None)
        obj.updateFromNode(node)
        return obj


class OrderDetail(PropertyBasedPMType):
    Start = cp.NodeTextProperty([namespaces.domTag('Start')]) # optional, xsd:dateTime
    End = cp.NodeTextProperty([namespaces.domTag('End')]) # optional, xsd:dateTime
    Performer = cp.SubElementListProperty([namespaces.domTag('Performer')], cls=PersonParticipation) #0...n)
    Service = cp.SubElementListProperty([namespaces.domTag('Service')], cls=CodedValue) #0...n)
    ImagingProcedure = cp.SubElementListProperty([namespaces.domTag('ImagingProcedure')], cls=ImagingProcedure)
    _props = ['Start', 'End', 'Performer', 'Service', 'ImagingProcedure']

    def __init__(self, start=None, end=None, performer=None, service=None, imagingprocedure=None):
        """
        :param start: a xsd:DateTime string
        :param end: a xsd:DateTime string
        :param performer: a list of PersonParticipation objects
        :param service: a list of CodedValue objects
        @param imagingprocedure: a list of ImagingProcedure objects
        """
        self.Start = start
        self.End = end
        if performer:
            self.Performer = performer
        if service:
            self.Service = service
        if imagingprocedure:
            self.ImagingProcedure = imagingprocedure


class RequestedOrderDetail(OrderDetail):
    ReferringPhysician = cp.SubElementProperty([namespaces.domTag('ReferringPhysician')], valueClass=PersonReference) # optional
    RequestingPhysician = cp.SubElementProperty([namespaces.domTag('RequestingPhysician')], valueClass=PersonReference) # optional
    PlacerOrderNumber = cp.SubElementProperty([namespaces.domTag('PlacerOrderNumber')], valueClass=InstanceIdentifier) # mandatory
    _props = ['ReferringPhysician', 'RequestingPhysician', 'PlacerOrderNumber']


    def __init__(self, start=None, end=None, performer=None, service=None, imagingprocedure=None,
                 referringphysician=None, requestingphysician=None, placerordernumber=None):
        """
        :param referringphysician:  a PersonReference
        :param requestingphysician: a PersonReference
        :param placerordernumber:   an InstanceIdentifier
        """
        super(RequestedOrderDetail, self).__init__(start, end, performer, service, imagingprocedure)
        self.ReferringPhysician = referringphysician
        self.RequestingPhysician = requestingphysician
        self.PlacerOrderNumber = placerordernumber


class PerformedOrderDetail(OrderDetail):
    FillerOrderNumber = cp.SubElementProperty([namespaces.domTag('FillerOrderNumber')], valueClass=InstanceIdentifier) # optional
    ResultingClinicalInfo = cp.SubElementListProperty([namespaces.domTag('RelevantClinicalInfo')], cls=ClinicalInfo)
    _props = ['FillerOrderNumber', 'ResultingClinicalInfo']

    def __init__(self, start=None, end=None, performer=None, service=None, imagingprocedure=None,
                 fillerordernumber=None, resultingclinicalinfos=None):
        super(PerformedOrderDetail, self).__init__(start, end, performer, service, imagingprocedure)
        self.FillerOrderNumber = fillerordernumber
        if resultingclinicalinfos:
            self.ResultingClinicalInfo = resultingclinicalinfos



class WorkflowDetail(PropertyBasedPMType):
    Patient = cp.SubElementProperty([namespaces.domTag('Patient')], valueClass=PersonReference) # optional
    AssignedLocation = cp.SubElementProperty([namespaces.domTag('AssignedLocation')], valueClass=LocationReference) # optional
    VisitNumber = cp.SubElementProperty([namespaces.domTag('VisitNumber')], valueClass=InstanceIdentifier) # optional
    DangerCode = cp.SubElementListProperty([namespaces.domTag('Reason')], cls=CodedValue)
    RelevantClinicalInfo = cp.SubElementListProperty([namespaces.domTag('RelevantClinicalInfo')], cls=ClinicalInfo)
    RequestedOrderDetail = cp.SubElementProperty([namespaces.domTag('RequestedOrderDetail')], valueClass=RequestedOrderDetail) # optional
    PerformedOrderDetail = cp.SubElementProperty([namespaces.domTag('PerformedOrderDetail')], valueClass=PerformedOrderDetail) # optional
    _props = ['Patient', 'AssignedLocation', 'VisitNumber', 'DangerCode',
              'RelevantClinicalInfo', 'RequestedOrderDetail', 'PerformedOrderDetail']


    def __init__(self, patient=None, assignedlocation=None, visitnumber=None, dangercode=None,
                 relevantclinicalinfo=None, requestedorderdetail=None, performedorderdetail=None):
        self.Patient = patient
        self.AssignedLocation = assignedlocation
        self.VisitNumber = visitnumber
        if dangercode:
            self.DangerCode = dangercode
        if relevantclinicalinfo:
            self.RelevantClinicalInfo = relevantclinicalinfo
        self.RequestedOrderDetail = requestedorderdetail
        self.PerformedOrderDetail = performedorderdetail



class Relation(PropertyBasedPMType):
    """Relation allows the modelling of relationships between a metric and other containtment tree entries."""
    Code = cp.SubElementProperty([namespaces.domTag('Code')], valueClass=CodedValue) #optional
    Identification = cp.SubElementProperty([namespaces.domTag('Identification')], valueClass=InstanceIdentifier) # optional
    Kind = cp.NodeAttributeProperty('Kind') # required, Rcm, PS, SST, ECE, DCE, Oth
    Entries = cp.NodeAttributeListProperty('Entries')
    _props = ['Code', 'Identification', 'Kind', 'Entries']


# SafetyReq definitions
class T_Selector(PropertyBasedPMType):
    Id = cp.NodeAttributeProperty('Id')
    text = cp.NodeTextProperty()
    _props = ['Id', 'text']
    def __init__(self, id_, text):
        """
        @param id_: a string
        @param text : a string
        """
        self.Id = id_
        self.text = text
        

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        cls.Id.updateFromNode(obj, node)
        cls.text.updateFromNode(obj, node)
        return obj

    
    
class T_DualChannelDef(PropertyBasedPMType):
    Selector = cp.SubElementListProperty([namespaces.mdpwsTag('Selector')], cls=T_Selector)
    Algorithm = cp.NodeAttributeProperty('Algorithm')
    Transform = cp.NodeAttributeProperty('Transform')
    _props = ['Selector', 'Algorithm', 'Transform']
    
    def __init__(self, selectors, algorithm=None, transform=None):
        """
        @param selectors: a list of Selector objects
        @param algorithm : a string
        @param transform : a string
        """
        self.Selector = selectors
        self.Algorithm = algorithm
        self.Transform = transform
        

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None, None)
        cls.Selector.updateFromNode(obj, node)
        cls.Algorithm.updateFromNode(obj, node)
        cls.Transform.updateFromNode(obj, node)
        return obj



class T_SafetyContextDef(PropertyBasedPMType):
    Selector = cp.SubElementListProperty([namespaces.siTag('Selector')], cls=T_Selector)
    _props = ['Selector',]
    
    def __init__(self, selectors):
        """
        @param selectors: a list of Selector objects
        """
        self.Selector = selectors
        

    @classmethod
    def fromNode(cls, node):
        obj = cls(None)
        cls.Selector.updateFromNode(obj, node)
        return obj


class T_SafetyReq(PropertyBasedPMType):
    DualChannelDef = cp.SubElementProperty([namespaces.siTag('DualChannelDef')], valueClass=T_DualChannelDef)  # optional
    SafetyContextDef = cp.SubElementProperty([namespaces.siTag('SafetyContextDef')], valueClass=T_SafetyContextDef) #optional
    _props = ['DualChannelDef', 'SafetyContextDef']
    
    def __init__(self, dualChannelDef, safetyContextDef):
        self.DualChannelDef = dualChannelDef
        self.SafetyContextDef = safetyContextDef
        

    @classmethod
    def fromNode(cls, node):
        obj = cls(None, None)
        cls.DualChannelDef.updateFromNode(obj, node)
        cls.SafetyContextDef.updateFromNode(obj, node)
        return obj



class T_Udi(PropertyBasedPMType):
    DeviceIdentifier = cp.NodeTextProperty([namespaces.domTag('DeviceIdentifier')], isOptional=False)
    HumanReadableForm = cp.NodeTextProperty([namespaces.domTag('HumanReadableForm')], isOptional=False)
    Issuer = cp.SubElementProperty([namespaces.domTag('Issuer')], valueClass=InstanceIdentifier)
    Jurisdiction = cp.SubElementProperty([namespaces.domTag('Jurisdiction')], valueClass=InstanceIdentifier)
    _props = ['DeviceIdentifier', 'HumanReadableForm', 'Issuer', 'Jurisdiction']

    def __init__(self, device_identifier=None, humanreadable_form=None, issuer=None, jurisdiction=None):
        """
        UDI fragments as defined by the FDA. (Only used in BICEPS Final)
        :param device_identifier: a string
        :param humanreadable_form: a string
        :param issuer: an InstanceIdentifier
        :param jurisdiction: an InstanceIdentifier (optional)
        """
        self.DeviceIdentifier = device_identifier
        self.HumanReadableForm = humanreadable_form
        self.Issuer = issuer
        self.Jurisdiction = jurisdiction

        
###################################################################################
# following : classes that serve only as name spaces


class StringEnum(str, enum.Enum):

    def __str__(self):
        return self.value


class SafetyClassification(StringEnum):
    INF = 'Inf'
    MED_A = 'MedA'
    MED_B = 'MedB'
    MED_C = 'MedC'


class MdsOperatingMode(StringEnum):
    NORMAL = 'Nml'
    DEMO = 'Dmo'
    SERVICE = 'Srv'
    MAINTENANCE = 'Mtn'
    

class OperatingMode(StringEnum):
    DISABLED = 'Dis'
    ENABLED = 'En'
    NA = 'NA'

    
class ComponentActivation(StringEnum):
    ON = 'On'
    NOT_READY = 'NotRdy'
    STANDBY = 'StndBy'
    OFF = 'Off'
    SHUTDOWN = 'Shtdn'
    FAILURE = 'Fail'


class ContextAssociation(StringEnum):
    NO_ASSOCIATION = 'No'
    PRE_ASSOCIATION = 'Pre'
    ASSOCIATED = 'Assoc'
    DISASSOCIATED = 'Dis'


class AlertConditionMonitoredLimits(StringEnum):
    ALL_ON = 'All'
    LOW_OFF = 'LoOff'
    HIGH_OFF = 'HiOff'
    ALL_OFF = 'None'


class AlertConditionPriority(StringEnum):
    NONE = 'None'
    LOW = 'Lo'
    MEDIUM = 'Me'
    HIGH = 'Hi'


class AlertConditionKind(StringEnum):
    PHYSIOLOGICAL = 'Phy'
    TECHNICAL = 'Tec'
    OTHER = 'Oth'


class AlertActivation(StringEnum):
    ON = 'On'
    OFF = 'Off'
    PAUSED = 'Psd'


class AlertSignalPresence(StringEnum):
    ON = 'On'
    OFF = 'Off'
    LATCH = 'Latch'
    ACK = 'Ack'


class AlertSignalManifestation(StringEnum):
    AUD = 'Aud' # Aud = Audible. The ALERT SIGNAL manifests in an audible manner, i.e., the alert can be heard. Example: an alarm sound.
    VIS = 'Vis' # Vis = Visible. The ALERT SIGNAL manifests in a visible manner, i.e., the alert can be seen. Example: a red flashing light.
    TAN = 'Tan' # Tan = Tangible. The ALERT SIGNAL manifests in a tangible manner, i.e., the alert can be felt. Example: vibration.
    OTH = 'Oth' # Oth = Other. The ALERT SIGNAL manifests in a manner not further specified.


class MetricAvailability(StringEnum):
    INTERMITTENT = 'Intr'
    CONTINUOUS = 'Cont'


class MetricCategory(StringEnum):
    UNSPECIFIED = 'Unspec'
    MEASUREMENT = 'Msrmt'
    CALCULATION = 'Clc'
    SETTING = 'Set'
    PRESETTING = 'Preset'
    RECOMMENDATION = 'Rcmm'


class MeasurementValidity(StringEnum):
    """Level of validity of a measured value.
    Used in BICEPS Final"""
    VALID = 'Vld'
    VALIDATED_DATA = 'Vldated'
    MEASUREMENT_ONGOING = 'Ong'
    QUESTIONABLE = 'Qst'
    CALIBRATION_ONGOING = 'Calib'
    INVALID = 'Inv'
    OVERFLOW = 'Oflw'
    UNDERFLOW = 'Uflw'
    NA = 'NA'

    
class InvocationState(StringEnum): # a namespace class
    WAIT = 'Wait'                  # Wait = Waiting. The operation has been queued and waits for execution. 
    START = 'Start'                # Start = Started. The execution of the operation has been started
    CANCELLED = 'Cnclld'           # Cnclld = Cancelled. The execution has been cancelled by the SERVICE PROVIDER.
    CANCELLED_MANUALLY = 'CnclldMan' # CnclldMan = Cancelled Manually. The execution has been cancelled by the operator.
    FINISHED = 'Fin'               # Fin = Finished. The execution has been finished.
    FINISHED_MOD = 'FinMod'        # FinMod = Finished with modification. As the requested target value could not be reached, the next best value has been chosen and used as target value.
    FAILED = 'Fail'                # The execution has been failed.


class InvocationError(StringEnum):
    UNSPECIFIED = 'Unspec'         # An unspecified error has occurred. No more information about the error is available.
    UNKNOWN_OPERATION = 'Unkn'     # Unknown Operation. The HANDLE to the operation object is not known.
    INVALID_VALUE = 'Inv'          # Invalid Value. The HANDLE to the operation object does not match the invocation request message
    OTHER = 'Oth'                  # Another type of error has occurred. More information on the error MAY be available.


class GenerationMode(StringEnum):
    """Describes whether METRIC data is generated by real measurements or under unreal settings (demo or test data)."""
    REAL = 'Real'                  # Real Data. A value that is generated under real conditions
    TEST = 'Test'                  # Test Data. A value that is arbitrary and is for testing purposes only
    DEMO = 'Demo'                  # Demo Data. A value that is arbitrary and is for demonstration purposes only


class Units(object):
    UnitLess = CodedValue('262656') # used if a metric has no unit


class DescriptionModificationTypes(StringEnum):
    CREATE = 'Crt'
    UPDATE = 'Upt'
    DELETE = 'Del'


class PatientType(StringEnum):
    UNSPECIFIED = 'Unspec'
    ADULT = 'Ad'
    ADOLESCENT = 'Ado'
    PEDIATRIC = 'Ped'
    INFANT = 'Inf'
    NEONATAL = 'Neo'
    OTHER = 'Oth'
