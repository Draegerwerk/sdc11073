""" Implementation of some data types used in Message Model"""
import enum
from .pmtypes import PropertyBasedPMType
from .mdib import containerproperties as cp
from . import msg_qnames as msg

# pylint: disable=invalid-name

class RetrievabilityMethod(enum.Enum):
    GET = 'Get'
    PERIODIC = 'Per'
    EPISODIC = 'Ep'
    STREAM = 'Strm'


class RetrievabilityInfo(PropertyBasedPMType):
    NODETYPE = msg.RetrievabilityInfo
    Method = cp.EnumAttributeProperty('Method', enum_cls=RetrievabilityMethod, is_optional=False)
    UpdatePeriod = cp.DurationAttributeProperty('UpdatePeriod', implied_py_value=1.0)
    _props = ['Method', 'UpdatePeriod']

    def __init__(self, method: RetrievabilityMethod, update_period: [float, None] = None):
        self.Method = method
        self.UpdatePeriod = update_period

    @classmethod
    def from_node(cls, node):
        obj = cls(RetrievabilityMethod.GET, None)  # any allowed value, will be overwritten in update_node
        obj.update_from_node(node)
        return obj

    def __repr__(self):
        return f'{self.__class__.__name__} {self.Method} period={self.UpdatePeriod}'

class Retrievability(PropertyBasedPMType):
    By = cp.SubElementListProperty(msg.By, value_class=RetrievabilityInfo)
    _props = ['By']

    def __init__(self, retrievability_info_list=None):
        self.By = retrievability_info_list or []

    @classmethod
    def from_node(cls, node):
        obj = cls(None)
        obj.update_from_node(node)
        return obj
