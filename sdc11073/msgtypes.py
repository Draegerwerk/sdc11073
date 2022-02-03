""" Implementation of some data types used in Message Model"""
import enum
from .pmtypes import PropertyBasedPMType
from .namespaces import msgTag
from .mdib import containerproperties as cp

# pylint: disable=invalid-name

class RetrievabilityMethod(enum.Enum):
    GET = 'Get'
    PERIODIC = 'Per'
    EPISODIC = 'Ep'
    STREAM = 'Strm'


class RetrievabilityInfo(PropertyBasedPMType):
    Method = cp.NodeAttributeProperty('Method')
    UpdatePeriod = cp.DurationAttributeProperty('UpdatePeriod', impliedPyValue=1.0)
    _props = ['Method', 'UpdatePeriod']

    def __init__(self, method: RetrievabilityMethod, update_period: [float, None] = None):
        self.Method = method.value
        self.UpdatePeriod = update_period

    @classmethod
    def fromNode(cls, node):
        obj = cls(RetrievabilityMethod.GET, None)  # any allowed value, will be overwritten in update_node
        obj.updateFromNode(node)
        return obj

    def __repr__(self):
        return f'{self.__class__.__name__} {self.Method} period={self.UpdatePeriod}'


class Retrievability(PropertyBasedPMType):
    By = cp.SubElementListProperty([msgTag('By')], cls=RetrievabilityInfo)
    _props = ['By']

    def __init__(self, retrievability_info_list=None):
        self.By = retrievability_info_list or []

    @classmethod
    def fromNode(cls, node):
        obj = cls(None)
        obj.updateFromNode(node)
        return obj
