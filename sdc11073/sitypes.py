""" Implementation of data types used in SafetyInformation Model"""
from .pmtypes import PropertyBasedPMType
from .namespaces import mdpwsTag, siTag
from .mdib import containerproperties as cp


class T_Selector(PropertyBasedPMType): # pylint: disable=invalid-name
    Id = cp.StringAttributeProperty('Id')
    text = cp.NodeTextProperty()
    _props = ['Id', 'text']

    def __init__(self, id_, text):
        """
        @param id: a string
        @param text : a string
        """
        self.Id = id_  # pylint: disable=invalid-name
        self.text = text

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        cls.Id.update_from_node(obj, node)
        cls.text.update_from_node(obj, node)
        return obj


class T_DualChannelDef(PropertyBasedPMType): # pylint: disable=invalid-name
    # pylint: disable=invalid-name
    Selector = cp.SubElementListProperty([mdpwsTag('Selector')], value_class=T_Selector)
    Algorithm = cp.StringAttributeProperty('Algorithm')
    Transform = cp.StringAttributeProperty('Transform')
    # pylint: enable=invalid-name
    _props = ['Selector', 'Algorithm', 'Transform']

    def __init__(self, selectors, algorithm=None, transform=None):
        """
        @param selectors: a list of Selector objects
        @param algorithm : a string
        @param transform : a string
        """
        # pylint: disable=invalid-name
        self.Selector = selectors
        self.Algorithm = algorithm
        self.Transform = transform
        # pylint: enable=invalid-name

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None, None)
        cls.Selector.update_from_node(obj, node)
        cls.Algorithm.update_from_node(obj, node)
        cls.Transform.update_from_node(obj, node)
        return obj


class T_SafetyContextDef(PropertyBasedPMType): # pylint: disable=invalid-name
    Selector = cp.SubElementListProperty([siTag('Selector')], value_class=T_Selector)  # pylint: disable=invalid-name
    _props = ['Selector', ]

    def __init__(self, selectors):
        """
        @param selectors: a list of Selector objects
        @param algorithm : a string
        @param transform : a string
        """
        self.Selector = selectors  # pylint: disable=invalid-name

    @classmethod
    def from_node(cls, node):
        obj = cls(None)
        cls.Selector.update_from_node(obj, node)
        return obj


class T_SafetyReq(PropertyBasedPMType): # pylint: disable=invalid-name
    # pylint: disable=invalid-name
    DualChannelDef = cp.SubElementProperty([siTag('DualChannelDef')],
                                           value_class=T_DualChannelDef)  # optional
    SafetyContextDef = cp.SubElementProperty([siTag('SafetyContextDef')],
                                             value_class=T_SafetyContextDef)  # optional
    # pylint: enable=invalid-name
    _props = ['DualChannelDef', 'SafetyContextDef']

    def __init__(self, dualChannelDef, safetyContextDef):
        # pylint: disable=invalid-name
        self.DualChannelDef = dualChannelDef
        self.SafetyContextDef = safetyContextDef
        # pylint: enable=invalid-name

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        cls.DualChannelDef.update_from_node(obj, node)
        cls.SafetyContextDef.update_from_node(obj, node)
        return obj
