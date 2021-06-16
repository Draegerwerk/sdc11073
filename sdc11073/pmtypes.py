""" Implementation of data types used in Participant Model"""
from collections import namedtuple
import traceback
import inspect
import itertools
import enum
from lxml import etree as etree_
from math import isclose
from .namespaces import domTag, QN_TYPE, docNameFromQName, txt2QName
from .mdib import containerproperties  as cp
'''
Interface of  pmTypes:

fromEtreeNode (class method) is a constructor that is used to create a type object from XML node

asEtreeNode: returns an etree node that represents the object
'''

class StringEnum(str, enum.Enum):

    def __str__(self):
        return self.value

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
                raise RuntimeError('In {}.{}, {} could not update: {}'.format(
                    self.__class__.__name__, prop_name, str(prop), traceback.format_exc()))


    def updateFromNode(self, node):
        for dummy, prop in self._sortedContainerProperties():
            prop.updateFromNode(self, node)

    def update_from_other(self, other):
        """copies the python values, no xml involved"""
        for dummy, prop in self._sortedContainerProperties():
            prop.update_from_other(self, other)


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

    def __repr__(self):
        return f'{self.__class__.__name__}({self._sortedContainerProperties()})'

    @classmethod
    def from_node(cls, node):
        """ default from_node Constructor that provides no arguments for class __init__"""
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
    def from_node(cls, node):
        text = node.text
        return cls(text)    


class T_TextWidth(StringEnum):
    XS = 'xs'
    S = 's'
    M = 'm'
    L = 'l'
    XL = 'xl'
    XXL = 'xxl'

class LocalizedText(PropertyBasedPMType):
    text = cp.NodeTextProperty()  # this is the text of the node. Here attribute is lower case!
    Ref = cp.StringAttributeProperty('Ref')
    Lang = cp.StringAttributeProperty('Lang')
    Version = cp.IntegerAttributeProperty('Version')
    TextWidth = cp.EnumAttributeProperty('TextWidth', enum_cls=T_TextWidth)
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

    @classmethod
    def from_node(cls, node):
        text = node.text
        lang = node.get('Lang')
        ref = node.get('Ref')
        version = node.get('Version')
        if version is not None:
            version = int(version)
        text_width = node.get('TextWidth')
        enum_text_width = T_TextWidth(text_width) if text_width is not None else None
        return cls(text, lang, ref, version, enum_text_width)


        
DefaultCodingSystem = 'urn:oid:1.2.840.10004.1.1.1.0.0.1' # ISO/IEC 11073-10101


class NotCompareableVersionError(Exception):
    """ This exception says that two coded values cannot be compared, because one has a coding system version, the other one not.
    In that case it is not possible to decide if they are equal."""
    pass

_CodingBase = namedtuple('_CodingBase', 'code codingSystem codingSystemVersion')
class Coding(_CodingBase):
    """ Immutable representation of a coding. Can be used as key in dictionaries"""


    def __new__(cls, code, codingSystem=DefaultCodingSystem, codingSystemVersion=None):
        return super(Coding, cls).__new__(cls,
                                          str(code),
                                          codingSystem,
                                          codingSystemVersion)

    def equals(self, other, raiseNotComparableException=False):
        """ different compare method to __eq__, overwriting __eq__ makes Coding un-hashable!
         other can be an int, a string, or a Coding.
         Simple comparision with int or string only works if self.codingSystem == DefaultCodingSystem
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
    def from_node(cls, node):
        """ Read Code and CodingSystem attributes of a node (CodedValue). """
        code = node.get('Code')
        codingSystem = node.get('CodingSystem', DefaultCodingSystem)
        codingSystemVersion = node.get('CodingSystemVersion')
        return cls(code, codingSystem, codingSystemVersion)


def mkCoding(code, codingSystem=DefaultCodingSystem, codingSystemVersion=None):
    return Coding(code, codingSystem, codingSystemVersion)



class T_Translation(PropertyBasedPMType):
    """
    Translation is part of CodedValue in BICEPS FINAL
    """
    ext_Extension = cp.ExtensionNodeProperty()
    Code = cp.StringAttributeProperty('Code', isOptional=False)
    CodingSystem = cp.StringAttributeProperty('CodingSystem', impliedPyValue=DefaultCodingSystem)
    CodingSystemVersion = cp.StringAttributeProperty('CodingSystemVersion')

    _props = ['ext_Extension', 'Code', 'CodingSystem', 'CodingSystemVersion']

    def __init__(self, code, codingsystem=None, codingSystemVersion=None):
        """
        @param code: a string or an int
        @param codingSystem: anyURI or None, defaults to ISO/IEC 11073-10101 if None
        @param codingSystemVersion: a string, min. length = 1
        """
        self.Code = str(code)
        self.CodingSystem = codingsystem
        self.CodingSystemVersion = codingSystemVersion
        self.coding = None # set by self._mkCoding()
        self.mkCoding()

    def __repr__(self):
        if self.CodingSystem is None:
            return 'CodedValue({})'.format(self.Code)
        elif self.CodingSystemVersion is None:
            return 'CodedValue({}, codingsystem={})'.format(self.Code, self.CodingSystem)
        else:
            return 'CodedValue({}, codingsystem={}, codingsystemversion={})'.format(self.Code, self.CodingSystem, self.CodingSystemVersion)

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
    def from_node(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        obj.mkCoding()
        return obj


class CodedValue(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    CodingSystemName = cp.SubElementListProperty(domTag('CodingSystemName'), valueClass = LocalizedText)
    ConceptDescription = cp.SubElementListProperty(domTag('ConceptDescription'), valueClass = LocalizedText)
    Translation = cp.SubElementListProperty(domTag('Translation'), valueClass = T_Translation)
    Code = cp.StringAttributeProperty('Code', isOptional=False)
    CodingSystem = cp.StringAttributeProperty('CodingSystem', impliedPyValue=DefaultCodingSystem)
    CodingSystemVersion = cp.StringAttributeProperty('CodingSystemVersion')
    SymbolicCodeName = cp.StringAttributeProperty('SymbolicCodeName')
    _props = ['ext_Extension', 'CodingSystemName', 'ConceptDescription', 'Translation',
              'Code', 'CodingSystem', 'CodingSystemVersion', 'SymbolicCodeName']

    def __init__(self, code, codingsystem=None, codingSystemVersion=None, codingSystemNames=None, conceptDescriptions=None, symbolicCodeName=None):
        """
        @param code: a string or an int
        @param codingSystem: anyURI or None, defaults to ISO/IEC 11073-10101 if None
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
        self.coding = None # set by self.mkCoding()
        self.mkCoding()


    def mkCoding(self):
        if self.Code is not None:
            self.coding = Coding(self.Code, self.CodingSystem, self.CodingSystemVersion)
        else:
            self.coding = None

    def __repr__(self):
        if self.CodingSystem is None:
            return 'CodedValue({})'.format(self.Code)
        elif self.CodingSystemVersion is None:
            return 'CodedValue({}, codingsystem={})'.format(self.Code, self.CodingSystem)
        else:
            return 'CodedValue({}, codingsystem={}, codingsystemversion={})'.format(self.Code, self.CodingSystem, self.CodingSystemVersion)


    def equals(self, other, raiseNotComparableException=True):
        """
        Compare this CodedValue with another one.
        A simplified compare with an int or string is also possible, it assumes DefaultCodingSystem and no CodingSystemVersion
        :param other: int, str or another CodedValue
        :param raiseNotComparableException: if False, not comparable versions are handled as different versions
                        if True, a NotCompareableVersionError is thrown if any of the Codings are not comparable
        :return: boolean
        """
        if isinstance(other, self.__class__):
                # Two CodedValue objects C1 and C2 are equivalent, if there exists a CodedValue object T1 in C1/pm:Translation
                # and a CodedValue object T2 in C2/pm:Translation such that T1 and T2 are equivalent, C1 and T2 are equivalent, or C2 and T1 are equivalent.
                foundMatch = False
                not_comparables = []
                my_codes = [self] # C1
                my_codes.extend(self.Translation) # all T1
                other_codes = [other]   #C2
                other_codes.extend(other.Translation) # all T2
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
    def from_node(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        obj.mkCoding()
        return obj


class Annotation(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty(domTag('Type'), valueClass=CodedValue)
    _props = ['ext_Extension', 'Type']
    
    codedValue = Type
    ''' An Annotation contains a Type Element that is a CodedValue.
    This is intended as an immutable object. After it has been created, no modification shall be done. '''
    def __init__(self, codedValue):
        self.Type = codedValue
        self.coding = codedValue.coding
 
 
    @classmethod
    def from_node(cls, node):
        typeNode = node.find(domTag('Type'))
        codedValue = CodedValue.from_node(typeNode)
        return cls(codedValue)


class OperatingMode(StringEnum):
    DISABLED = 'Dis'
    ENABLED = 'En'
    NA = 'NA'


class OperationGroup(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty(domTag('Type'), valueClass=CodedValue)
    OperatingMode = cp.EnumAttributeProperty('OperatingMode', enum_cls=OperatingMode)
    Operations = cp.OperationRefListAttributeProperty('Operations')
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
    def from_node(cls, node):
        typeNode = node.find(domTag('Type'))
        codedValue = CodedValue.from_node(typeNode)
        operatingMode = cls.OperatingMode.getPyValueFromNode(None, node)
        operations = cls.Operations.getPyValueFromNode(None, node)
        ret = cls(codedValue=codedValue, operatingMode=operatingMode, operations=operations)
        ret.node = node
        return ret
 
    

class InstanceIdentifier(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Type = cp.SubElementProperty(domTag('Type'), valueClass=CodedValue)
    IdentifierName = cp.SubElementListProperty(domTag('IdentifierName'), valueClass=LocalizedText)
    Root = cp.StringAttributeProperty('Root', defaultPyValue='biceps.uri.unk') # xsd:anyURI string, default is defined in R0135
    Extension = cp.StringAttributeProperty('Extension') # a xsd:string
    _props=('ext_Extension', 'Type', 'IdentifierName', 'Root', 'Extension')

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
    def from_node(cls, node):
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


class OperatingJurisdiction(InstanceIdentifier):
    _props = tuple() # no properties


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
        return 'Range (Lower={!r}, Upper={!r})'.format(self.Lower, self.Upper)



class Measurement(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    MeasurementUnit = cp.SubElementProperty(domTag('MeasurementUnit'), valueClass=CodedValue) # mandatory
    MeasuredValue = cp.DecimalAttributeProperty('MeasuredValue', isOptional=False)
    _props = ['ext_Extension', 'MeasurementUnit', 'MeasuredValue']
    
    def __init__(self, value, unit):
        """
        @param value: a value as string, float or integer
        @param unit: a CodedValue instance
        """
        self.MeasuredValue = value
        self.MeasurementUnit = unit
    
    
    @classmethod
    def from_node(cls, node):
        value = node.get('MeasuredValue')
        if value is not None:
            value = float(value)
        unit = None
        unitNode = node.find(domTag('MeasurementUnit'))
        if unitNode is not None:
            unit = CodedValue.from_node(unitNode)
        return cls(value, unit)


    def __repr__(self):
        return 'Measurement(value={!r}, Unit={!r})'.format(self.MeasuredValue, self.MeasurementUnit)



class AllowedValue(PropertyBasedPMType):
    Value = cp.NodeTextProperty(domTag('Value'))
    Type = cp.SubElementProperty(domTag('Type'), valueClass=CodedValue)
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
    def from_node(cls, node):
        valueString = node.find(domTag('Value')).text
        typeNode = node.find(domTag('Type'))
        if typeNode is None:
            typeCoding = None
        else:
            typeCoding = CodedValue.from_node(typeNode)
        return cls(valueString, typeCoding)


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

class GenerationMode(StringEnum):
    """Describes whether METRIC data is generated by real measurements or under unreal settings (demo or test data)."""
    REAL = 'Real'                  # Real Data. A value that is generated under real conditions
    TEST = 'Test'                  # Test Data. A value that is arbitrary and is for testing purposes only
    DEMO = 'Demo'                  # Demo Data. A value that is arbitrary and is for demonstration purposes only


class T_MetricQuality(PropertyBasedPMType):
    Validity = cp.EnumAttributeProperty('Validity', enum_cls=MeasurementValidity)
    Mode = cp.EnumAttributeProperty('Mode', impliedPyValue='Real', enum_cls=GenerationMode) # pm:GenerationMode
    Qi = cp.DecimalAttributeProperty('Qi', impliedPyValue=1) # pm:QualityIndicator
    _props = ('Validity', 'Mode', 'Qi')

    def __init__(self):
        super().__init__()

class AbstractMetricValue(PropertyBasedPMType):
    """ This is the base class for metric values inside metric states"""
    ext_Extension = cp.ExtensionNodeProperty()
    StartTime = cp.TimestampAttributeProperty('StartTime')
    StopTime = cp.TimestampAttributeProperty('StopTime')
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime')
    MetricQuality = cp.SubElementProperty(domTag('MetricQuality'), valueClass=T_MetricQuality)
    Annotation = cp.SubElementListProperty(domTag('Annotation'), Annotation)
    _props = ('ext_Extension', 'StartTime', 'StopTime', 'DeterminationTime', 'MetricQuality', 'Annotation')
    Annotations = Annotation  # alternative name that makes it clearer that this is a list

    def __init__(self, node=None):
        # attributes of root node
        self.node = node
        self.MetricQuality = T_MetricQuality()
        if node is not None:
            self.updateFromNode(node)
        else:
            self.MetricQuality.Validity = MeasurementValidity.VALID # mandatory value, for convenience it is preset


    def updateFromNode(self, node):
        for dummy, prop in self._sortedContainerProperties():
            prop.updateFromNode(self, node)
        self.node = node    


    def asEtreeNode(self, qname, nsmap):
        node = super().asEtreeNode(qname, nsmap)
        node.set(QN_TYPE, docNameFromQName(self.QType, nsmap))
        return node


    @classmethod
    def from_node(cls, node):
        obj = cls(node)
        return obj



class NumericMetricValue(AbstractMetricValue):
    QType = domTag('NumericMetricValue')
    Value = cp.DecimalAttributeProperty('Value') # an integer or float
    _props = ('Value',)    


    def __repr__(self):
        return '{} Validity={} Value={} DeterminationTime={}'.format(self.__class__.__name__,
                                                                     self.MetricQuality.Validity,
                                                                     self.Value,
                                                                     self.DeterminationTime)

    def __eq__(self, other):
        """ compares all properties, special handling of Value member"""
        try:
            for name, dummy in self._sortedContainerProperties():
                if name == 'Value':
                    # check if more than 0.01 off
                    my_value = getattr(self, name)
                    other_value = getattr(other, name)
                    if my_value is None and other_value is None:
                        continue
                    elif my_value is None or other_value is None:
                        return False # only one is None, they are not equal
                    else:
                        if abs( my_value - other_value) < 0.001:
                            continue
                        else:
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
    QType = domTag('StringMetricValue')
    Value = cp.StringAttributeProperty('Value') # a string
    _props = ('Value',)    
    

    def __repr__(self):
        return '{} Validity={} Value={} DeterminationTime={}'.format(self.__class__.__name__,
                                                                     self.MetricQuality.Validity,
                                                                     self.Value,
                                                                     self.DeterminationTime)



class ApplyAnnotation(PropertyBasedPMType):
    AnnotationIndex = cp.IntegerAttributeProperty('AnnotationIndex', isOptional=False)
    SampleIndex = cp.IntegerAttributeProperty('SampleIndex', isOptional=False)
    _props = ['AnnotationIndex', 'SampleIndex']

    def __init__(self, annotationIndex=None, sampleIndex=None):
        self.AnnotationIndex = annotationIndex
        self.SampleIndex = sampleIndex   

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        cls.AnnotationIndex.updateFromNode(obj, node)
        cls.SampleIndex.updateFromNode(obj, node)
        return obj

    def __repr__(self):
        return '{}(AnnotationIndex={}, SampleIndex={})'.format(self.__class__.__name__, self.AnnotationIndex, self.SampleIndex)



class SampleArrayValue(AbstractMetricValue):
    QType = domTag('SampleArrayValue')
    Samples = cp.DecimalListAttributeProperty('Samples') # list of xs:decimal types 
    ApplyAnnotation = cp.SubElementListProperty(domTag('ApplyAnnotation'), ApplyAnnotation)
    ApplyAnnotations = ApplyAnnotation  # alternative name that makes it clearer that this is a list
    _props = ('Samples', 'ApplyAnnotation')


    def __repr__(self):
        return '{} Samples={} ApplyAnnotations={}'.format(self.__class__.__name__, self.Samples, self.ApplyAnnotations)


    def __eq__(self, other):
        """ compares all properties, special handling of Value member"""
        try:
            for name, dummy in self._sortedContainerProperties():
                if name == 'Samples':
                    ownsample = getattr(self, name)
                    othersample = getattr(other, name)
                    if ownsample is None:
                        ownsample = []
                    if othersample is None:
                        othersample = []
                    if len(ownsample) != len(othersample):
                        return False
                    for pair in zip(ownsample, othersample):
                        # check if more than 0.01 off
                        diff = pair[0] - pair[1]
                        if abs(diff) < 0.01:
                            continue
                        else:
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
    Description = cp.SubElementListProperty(domTag('Description'), valueClass=LocalizedText)
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
    RemedyInfo = cp.SubElementProperty(domTag('RemedyInfo'), valueClass=RemedyInfo)
    Description = cp.SubElementListProperty(domTag('Description'), valueClass=LocalizedText)
    _props = ['ext_Extension', 'RemedyInfo', 'Description']
    def __init__(self, remedyInfo=None, descriptions=None):
        """
        @param remedyInfo: a RemedyInfo instance or None
        @param descriptions : a list of LocalizedText objects or None
        """
        self.RemedyInfo = remedyInfo
        self.Description = descriptions or []


    @classmethod
    def from_node(cls, node):
        remedyInfoNode = node.find(domTag('RemedyInfo'))
        if remedyInfoNode is not None:
            remedyInfo = RemedyInfo.from_node(remedyInfoNode)
        else:
            remedyInfo = None
        descriptions = []
        descriptionNodes = node.findall(domTag('Description'))
        for d in descriptionNodes:
            descriptions.append(LocalizedText.from_node(d))
        return cls(remedyInfo, descriptions)    



class  ActivateOperationDescriptorArgument(PropertyBasedPMType):
    """Argument for ActivateOperationDescriptor.
         1 Subelement "ArgName", type = pm:CodedValue
         1 SubElement "Arg" type=QName."""
    ArgName = cp.SubElementProperty(domTag('ArgName'), valueClass=CodedValue, isOptional=False)
    Arg = cp.NodeTextQNameProperty(domTag('Arg'), isOptional=False)
    _props = ['ArgName', 'Arg']
    def __init__(self, argName=None, arg=None):
        """
        @param argName: a CodedValue instance
        @param arg : etree_.QName instance
        """
        self.ArgName = argName
        self.Arg = arg
        

    @classmethod
    def from_node(cls, node):
        argNameNode = node.find(domTag('ArgName'))
        argName = CodedValue.from_node(argNameNode)
        argNode = node.find(domTag('Arg'))
        arg_QName = txt2QName(argNode.text, node.nsmap)
        return cls(argName, arg_QName)    


    def __repr__(self):
        return 'ActivateOperationDescriptorArgument(argName={}, arg={})'.format(self.ArgName, self.Arg)



class PhysicalConnectorInfo(PropertyBasedPMType):
    """PhysicalConnectorInfo defines a number in order to allow to guide the clinical user for a failure,
    e.g., in case of a disconnection of a sensor or an ultrasonic handpiece.
    Only in BICEPS final!"""
    ext_Extension = cp.ExtensionNodeProperty()
    Label = cp.SubElementListProperty(domTag('Label'), valueClass=LocalizedText) # A human-readable label that describes the physical connector.
    Number = cp.IntegerAttributeProperty('Number')# Number designates the connector number of the physical connector.
    _props = ['ext_Extension', 'Label', 'Number']

    def __init__(self, labels=None, number=None):
        """
        @param labels: a  list of LocalizedText
        @param number : an integer
        """
        self.Label = labels or []
        self.Number = number

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        cls.Label.updateFromNode(obj, node)
        cls.Number.updateFromNode(obj, node)
        return obj

    def __repr__(self):
        return 'PhysicalConnectorInfo(label={}, number={})'.format(self.Label, self.Number)


class AlertSignalManifestation(StringEnum):
    AUD = 'Aud' # Aud = Audible. The ALERT SIGNAL manifests in an audible manner, i.e., the alert can be heard. Example: an alarm sound.
    VIS = 'Vis' # Vis = Visible. The ALERT SIGNAL manifests in a visible manner, i.e., the alert can be seen. Example: a red flashing light.
    TAN = 'Tan' # Tan = Tangible. The ALERT SIGNAL manifests in a tangible manner, i.e., the alert can be felt. Example: vibration.
    OTH = 'Oth' # Oth = Other. The ALERT SIGNAL manifests in a manner not further specified.


class AlertActivation(StringEnum):
    ON = 'On'
    OFF = 'Off'
    PAUSED = 'Psd'


class SystemSignalActivation(PropertyBasedPMType):
    Manifestation = cp.EnumAttributeProperty('Manifestation', defaultPyValue=AlertSignalManifestation.OTH,
                                             enum_cls=AlertSignalManifestation, isOptional=False)
    State = cp.EnumAttributeProperty('State', defaultPyValue=AlertActivation.ON,
                                     enum_cls=AlertActivation, isOptional=False)
    _props = ['Manifestation', 'State']

    def __init__(self, manifestation=None, state=None):
        """
        @param manifestation: a pmtypes.AlertSignalManifestation value
        @param state : a pmtypes.AlertActivation value
        """
        self.Manifestation = manifestation
        self.State = state

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        obj.updateFromNode(node)
        return obj

    def __repr__(self):
        return 'SystemSignalActivation(Manifestation={}, State={})'.format(self.Manifestation, self.State)


class ProductionSpecification(PropertyBasedPMType):
    SpecType = cp.SubElementProperty(domTag('SpecType'), valueClass=CodedValue)
    ProductionSpec = cp.NodeTextProperty(domTag('ProductionSpec'))
    ComponentId = cp.SubElementProperty(domTag('ComponentId'),
                                        valueClass=InstanceIdentifier, isOptional=True)
    _props = ['SpecType', 'ProductionSpec', 'ComponentId']

    def __init__(self, spectype=None, productionspec=None, componentid=None):
        """
        @param spectype: a pmtypes.CodedValue value
        @param productionspec: a string
        @param componentid : a pmtypes.InstanceIdentifier value
        """
        self.SpecType = spectype
        self.ProductionSpec = productionspec
        self.ComponentId = componentid

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        obj.updateFromNode(node)
        return obj


class BaseDemographics(PropertyBasedPMType):
    Givenname = cp.NodeTextProperty(domTag('Givenname'), isOptional=True)
    Middlename = cp.SubElementTextListProperty(domTag('Middlename'))
    Familyname = cp.NodeTextProperty(domTag('Familyname'), isOptional=True)
    Birthname = cp.NodeTextProperty(domTag('Birthname'), isOptional=True)
    Title = cp.NodeTextProperty(domTag('Title'), isOptional=True)
    _props = ('Givenname', 'Middlename', 'Familyname', 'Birthname', 'Title')

    def __init__(self, given_name=None, middle_names=None, family_name=None, birth_name = None, title=None):
        super().__init__()
        self.Givenname = given_name
        self.Middlename = middle_names or []
        self.Familyname = family_name
        self.Birthname = birth_name
        self.Title = title


class PersonReference(PropertyBasedPMType):
    ext_Extension = cp.ExtensionNodeProperty()
    Identification = cp.SubElementListProperty(domTag('Identification'), valueClass=InstanceIdentifier) # 1...n
    Name = cp.SubElementProperty(domTag('Name'), valueClass=BaseDemographics) # optional
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
    PoC = cp.StringAttributeProperty('PoC')
    Room = cp.StringAttributeProperty('Room')
    Bed = cp.StringAttributeProperty('Bed')
    Facility = cp.StringAttributeProperty('Facility')
    Building = cp.StringAttributeProperty('Building')
    Floor = cp.StringAttributeProperty('Floor')
    _props = ('ext_Extension', 'PoC', 'Room', 'Bed', 'Facility', 'Building', 'Floor')

    def __init__(self, poc=None, room=None, bed=None, facility=None, building=None, floor=None):
        self.PoC = poc
        self.Room = room
        self.Bed = bed
        self.Facility = facility
        self.Building = building
        self.Floor = floor


class LocationReference(PropertyBasedPMType): # BICEPS Final
    Identification = cp.SubElementListProperty(domTag('Identification'), valueClass=InstanceIdentifier) # 1...n
    LocationDetail = cp.SubElementProperty(domTag('LocationDetail'), valueClass=LocationDetail) # optional
    _props = ['Identification', 'LocationDetail']

    def __init__(self, identifications=None, locationdetail=None):
        if identifications:
            self.Identification = identifications
        self.LocationDetail = locationdetail


class PersonParticipation(PersonReference):
    Role = cp.SubElementListProperty(domTag('Role'), valueClass=CodedValue) #0...n
    _props = ['Role',]

    def __init__(self, identifications=None, name=None, roles=None):
        super().__init__(identifications, name)
        if roles:
            self.Role = roles


class ReferenceRange(PropertyBasedPMType):
    """Representation of the normal or abnormal reference range for the measurement"""
    Range = cp.SubElementProperty(domTag('Range'), valueClass=Range)
    Meaning = cp.SubElementProperty(domTag('Meaning'), valueClass=CodedValue, isOptional=True)
    _props = ['Range', 'Meaning']

    def __init__(self, range, meaning=None):
        self.Range = range
        if meaning is not None:
            self.Meaning = meaning


class RelatedMeasurement(PropertyBasedPMType):
    """Related measurements for this clinical observation"""
    Value = cp.SubElementProperty(domTag('Value'), valueClass=Measurement)
    ReferenceRange = cp.SubElementListProperty(domTag('ReferenceRange'), valueClass=ReferenceRange)  # 0...n
    _props = ['Value', 'ReferenceRange']

    def __init__(self, value, reference_range=None):
        self.Value = value
        if reference_range is not None:
            self.ReferenceRange = reference_range


class ClinicalInfo(PropertyBasedPMType):
    Type = cp.SubElementProperty(domTag('Type'), valueClass=CodedValue)  # optional
    Description = cp.SubElementListProperty(domTag('Description'), valueClass=LocalizedText)  # 0...n
    RelatedMeasurement = cp.SubElementListProperty(domTag('RelatedMeasurement'), valueClass=Measurement)  # 0...n
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
    AccessionIdentifier = cp.SubElementProperty(domTag('AccessionIdentifier'), valueClass=InstanceIdentifier) # mandatory
    RequestedProcedureId = cp.SubElementProperty(domTag('RequestedProcedureId'), valueClass=InstanceIdentifier) # mandatory
    StudyInstanceUid = cp.SubElementProperty(domTag('StudyInstanceUid'), valueClass=InstanceIdentifier) # mandatory
    ScheduledProcedureStepId = cp.SubElementProperty(domTag('ScheduledProcedureStepId'), valueClass=InstanceIdentifier) # mandatory
    Modality = cp.SubElementProperty(domTag('Modality'), valueClass=CodedValue) # optional
    ProtocolCode = cp.SubElementProperty(domTag('ProtocolCode'), valueClass=CodedValue) # optional
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
    def from_node(cls, node):
        obj = cls(None, None, None, None)
        obj.updateFromNode(node)
        return obj


class OrderDetail(PropertyBasedPMType):
    Start = cp.NodeTextProperty(domTag('Start'), isOptional=True)  # xsd:dateTime
    End = cp.NodeTextProperty(domTag('End'), isOptional=True)  # xsd:dateTime
    Performer = cp.SubElementListProperty(domTag('Performer'), valueClass=PersonParticipation)  # 0...n
    Service = cp.SubElementListProperty(domTag('Service'), valueClass=CodedValue)  # 0...n
    ImagingProcedure = cp.SubElementListProperty(domTag('ImagingProcedure'), valueClass=ImagingProcedure)
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
    ReferringPhysician = cp.SubElementProperty(domTag('ReferringPhysician'), valueClass=PersonReference) # optional
    RequestingPhysician = cp.SubElementProperty(domTag('RequestingPhysician'), valueClass=PersonReference) # optional
    PlacerOrderNumber = cp.SubElementProperty(domTag('PlacerOrderNumber'), valueClass=InstanceIdentifier) # mandatory
    _props = ['ReferringPhysician', 'RequestingPhysician', 'PlacerOrderNumber']


    def __init__(self, start=None, end=None, performer=None, service=None, imagingprocedure=None,
                 referringphysician=None, requestingphysician=None, placerordernumber=None):
        """
        :param referringphysician:  a PersonReference
        :param requestingphysician: a PersonReference
        :param placerordernumber:   an InstanceIdentifier
        """
        super().__init__(start, end, performer, service, imagingprocedure)
        self.ReferringPhysician = referringphysician
        self.RequestingPhysician = requestingphysician
        self.PlacerOrderNumber = placerordernumber


class PerformedOrderDetail(OrderDetail):
    FillerOrderNumber = cp.SubElementProperty(domTag('FillerOrderNumber'), valueClass=InstanceIdentifier) # optional
    ResultingClinicalInfo = cp.SubElementListProperty(domTag('RelevantClinicalInfo'), valueClass=ClinicalInfo)
    _props = ['FillerOrderNumber', 'ResultingClinicalInfo']

    def __init__(self, start=None, end=None, performer=None, service=None, imagingprocedure=None,
                 fillerordernumber=None, resultingclinicalinfos=None):
        super().__init__(start, end, performer, service, imagingprocedure)
        self.FillerOrderNumber = fillerordernumber
        if resultingclinicalinfos:
            self.ResultingClinicalInfo = resultingclinicalinfos



class WorkflowDetail(PropertyBasedPMType):
    Patient = cp.SubElementProperty(domTag('Patient'), valueClass=PersonReference)
    AssignedLocation = cp.SubElementProperty(domTag('AssignedLocation'),
                                             valueClass=LocationReference, isOptional=True)
    VisitNumber = cp.SubElementProperty(domTag('VisitNumber'),
                                        valueClass=InstanceIdentifier, isOptional=True)
    DangerCode = cp.SubElementListProperty(domTag('Reason'), valueClass=CodedValue)
    RelevantClinicalInfo = cp.SubElementListProperty(domTag('RelevantClinicalInfo'), valueClass=ClinicalInfo)
    RequestedOrderDetail = cp.SubElementProperty(domTag('RequestedOrderDetail'),
                                                 valueClass=RequestedOrderDetail, isOptional=True)
    PerformedOrderDetail = cp.SubElementProperty(domTag('PerformedOrderDetail'),
                                                 valueClass=PerformedOrderDetail, isOptional=True)
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


class AbstractMetricDescriptorRelationKindEnum(StringEnum):
    RECOMMENDATION = 'Rcm'
    PRE_SETTING = 'PS'
    SET_OF_SUMMARY_STATISTICS = 'SST'
    EFFECT_ON_CONTAINMENT_TREE_ENTRIES = 'ECE'
    DERIVED_FROM_CONTAINMENT_TREE_ENTRIES = 'DCE'
    OTHER = 'Oth'


class AbstractMetricDescriptorRelation(PropertyBasedPMType):
    """ Relation allows the modelling of relationships between a metric and other containment tree entries.
    """
    Code = cp.SubElementProperty(domTag('Code'), valueClass=CodedValue, isOptional=True)
    Identification = cp.SubElementProperty(domTag('Identification'), valueClass=InstanceIdentifier,
                                           isOptional=True)
    Kind = cp.EnumAttributeProperty('Kind',enum_cls=AbstractMetricDescriptorRelationKindEnum, isOptional=False)
    Entries = cp.NodeAttributeListProperty('Entries')
    _props = ['Code', 'Identification', 'Kind', 'Entries']

    def __init__(self):
        super().__init__()
Relation = AbstractMetricDescriptorRelation


class PatientType(StringEnum):
    UNSPECIFIED = 'Unspec'
    ADULT = 'Ad'
    ADOLESCENT = 'Ado'
    PEDIATRIC = 'Ped'
    INFANT = 'Inf'
    NEONATAL = 'Neo'
    OTHER = 'Oth'

class T_Sex(StringEnum):
    UNSPEC = 'Unspec'
    MALE = 'M'
    FEMALE = 'F'
    UNKNOWN = 'Unkn'


class PatientDemographicsCoreData(BaseDemographics):
    Sex = cp.NodeEnumTextProperty(T_Sex, domTag('Sex'), isOptional=True)
    PatientType = cp.NodeEnumTextProperty(PatientType,domTag('PatientType'), isOptional=True)
    DateOfBirth = cp.DateOfBirthProperty(domTag('DateOfBirth'), isOptional=True)
    Height = cp.SubElementProperty(domTag('Height'), valueClass=Measurement, isOptional=True)
    Weight = cp.SubElementProperty(domTag('Weight'), valueClass=Measurement, isOptional=True)
    Race = cp.SubElementProperty(domTag('Race'), valueClass=CodedValue, isOptional=True)
    _props = ('Sex', 'PatientType', 'DateOfBirth', 'Height', 'Weight', 'Race')

    def setBirthdate(self, dateTimeOfBirth_string):
        """ this method accepts a string, format acc. to XML Schema: xsd:dateTime, xsd:date, xsd:gYearMonth or xsd:gYear
        Internally it holds it as a datetime object, so specific formatting of the dateTimeOfBirth_string will be lost."""
        if not dateTimeOfBirth_string:
            self.DateOfBirth = None
        else:
            datetime = cp.DateOfBirthProperty.mk_value_object(dateTimeOfBirth_string)
            self.DateOfBirth = datetime


class NeonatalPatientDemographicsCoreData(PatientDemographicsCoreData):
    GestationalAge = cp.SubElementProperty(domTag('GestationalAge'), valueClass=Measurement,
                                           isOptional=True)
    BirthLength = cp.SubElementProperty(domTag('BirthLength'), valueClass=Measurement)
    BirthWeight = cp.SubElementProperty(domTag('BirthWeight'), valueClass=Measurement)
    HeadCircumference = cp.SubElementProperty(domTag('HeadCircumference'), valueClass=Measurement)
    Mother = cp.SubElementProperty(domTag('GestationalAge'), valueClass=PersonReference)
    _props = ('GestationalAge', 'BirthLength', 'BirthWeight', 'HeadCircumference', 'Mother')



class T_Udi(PropertyBasedPMType):
    """Part of Meta data"""
    DeviceIdentifier = cp.NodeTextProperty(domTag('DeviceIdentifier'))
    HumanReadableForm = cp.NodeTextProperty(domTag('HumanReadableForm'))
    Issuer = cp.SubElementProperty(domTag('Issuer'), valueClass=InstanceIdentifier)
    Jurisdiction = cp.SubElementProperty(domTag('Jurisdiction'),
                                         valueClass=InstanceIdentifier, isOptional=True)
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


class MetaData(PropertyBasedPMType):
    Udi = cp.SubElementListProperty(domTag('Udi'), valueClass=T_Udi)
    LotNumber = cp.NodeTextProperty(domTag('LotNumber'), isOptional=True)
    Manufacturer = cp.SubElementListProperty(domTag('Manufacturer'), valueClass=LocalizedText)
    ManufactureDate = cp.NodeTextProperty(domTag('ManufactureDate'), isOptional=True)
    ExpirationDate = cp.NodeTextProperty(domTag('ExpirationDate'), isOptional=True)
    ModelName = cp.SubElementListProperty(domTag('ModelName'), valueClass=LocalizedText)
    ModelNumber = cp.NodeTextProperty(domTag('ModelNumber'), isOptional=True)
    SerialNumber = cp.SubElementTextListProperty(domTag('SerialNumber'))
    _props = ['Udi', 'LotNumber', 'Manufacturer', 'ManufactureDate', 'ExpirationDate',
              'ModelName', 'ModelNumber', 'SerialNumber']

    def __init__(self):
        super().__init__()

###################################################################################
# following : classes that serve only as name spaces

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


class CanEscalateAlertConditionPriority(StringEnum):
    LOW = 'Lo'
    MEDIUM = 'Me'
    HIGH = 'Hi'


class CanDeEscalateAlertConditionPriority(StringEnum):
    MEDIUM = 'Me'
    LOW = 'Lo'
    NONE = 'None'


class AlertSignalPresence(StringEnum):
    ON = 'On'
    OFF = 'Off'
    LATCH = 'Latch'
    ACK = 'Ack'




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


class Units(object):
    UnitLess = CodedValue('262656') # used if a metric has no unit


class DescriptionModificationTypes(StringEnum):
    CREATE = 'Crt'
    UPDATE = 'Upt'
    DELETE = 'Del'


class DerivationMethod(StringEnum):
    AUTOMATIC = 'Auto'
    MANUAL = 'Man'

class T_AccessLevel(StringEnum):
    USER = 'Usr'
    CLINICAL_SUPER_USER = 'CSUsr'
    RESPONSIBLE_ORGANIZATION = 'RO'
    SERVICE_PERSONNEL = 'SP'
    OTHER = 'Oth'


class AlertSignalPrimaryLocation(StringEnum):
    LOCAL = 'Loc'
    REMOTE = 'Rem'
