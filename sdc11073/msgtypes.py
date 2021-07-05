""" Implementation of some data types used in Message Model"""
import enum
from .pmtypes import PropertyBasedPMType
from .namespaces import msgTag
from .mdib import containerproperties as cp


class RetrievabilityMethod(enum.Enum):
    GET = 'Get'
    PERIODIC = 'Per'
    EPISODIC = 'Ep'
    STREAM = 'Strm'


class RetrievabilityInfo(PropertyBasedPMType):
    Method = cp.EnumAttributeProperty('Method', enum_cls=RetrievabilityMethod, isOptional=False)
    UpdatePeriod = cp.DurationAttributeProperty('UpdatePeriod', impliedPyValue=1.0)
    _props = ['Method', 'UpdatePeriod']

    def __init__(self, method: RetrievabilityMethod, update_period: [float, None] = None):
        self.Method = method
        self.UpdatePeriod = update_period

    @classmethod
    def from_node(cls, node):
        obj = cls(None, None)
        obj.updateFromNode(node)
        return obj


class Retrievability(PropertyBasedPMType):
    By = cp.SubElementListProperty(msgTag('By'), valueClass=RetrievabilityInfo)
    _props = ['By']

    def __init__(self, retrievability_info_list=None):
        self.By = retrievability_info_list or []

    @classmethod
    def from_node(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        return obj
