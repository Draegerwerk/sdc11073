""" Implementation of data types used in SafetyInformation Model"""
from .pmtypes import PropertyBasedPMType
from .namespaces import mdpwsTag, siTag
from .mdib import containerproperties as cp
class T_Selector(PropertyBasedPMType):
    Id = cp.StringAttributeProperty('Id')
    text = cp.NodeTextProperty()
    _props = ['Id', 'text']

    def __init__(self, id_, text):
        """
        @param id: a string
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
    Selector = cp.SubElementListProperty([mdpwsTag('Selector')], cls=T_Selector)
    Algorithm = cp.StringAttributeProperty('Algorithm')
    Transform = cp.StringAttributeProperty('Transform')
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
    Selector = cp.SubElementListProperty([siTag('Selector')], cls=T_Selector)
    _props = ['Selector', ]

    def __init__(self, selectors):
        """
        @param selectors: a list of Selector objects
        @param algorithm : a string
        @param transform : a string
        """
        self.Selector = selectors

    @classmethod
    def fromNode(cls, node):
        obj = cls(None)
        cls.Selector.updateFromNode(obj, node)
        return obj


class T_SafetyReq(PropertyBasedPMType):
    DualChannelDef = cp.SubElementProperty([siTag('DualChannelDef')],
                                           valueClass=T_DualChannelDef)  # optional
    SafetyContextDef = cp.SubElementProperty([siTag('SafetyContextDef')],
                                             valueClass=T_SafetyContextDef)  # optional
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


