""" Implementation of some data types used in Message Model"""
import enum

from . import ext_qnames as ext
from . import msg_qnames as msg
from .dataconverters import UnsignedIntConverter
from .mdib import containerproperties as cp
from .pmtypes import PropertyBasedPMType, LocalizedText, InstanceIdentifier


# pylint: disable=invalid-name

class StringEnum(str, enum.Enum):

    def __str__(self):
        return str(self.value)


class DescriptionModificationType(StringEnum):
    CREATE = 'Crt'
    UPDATE = 'Upt'
    DELETE = 'Del'


class RetrievabilityMethod(StringEnum):
    GET = 'Get'
    PERIODIC = 'Per'
    EPISODIC = 'Ep'
    STREAM = 'Strm'


class InvocationState(StringEnum):
    WAIT = 'Wait'  # Wait = Waiting. The operation has been queued and waits for execution.
    START = 'Start'  # Start = Started. The execution of the operation has been started
    CANCELLED = 'Cnclld'  # Cnclld = Cancelled. The execution has been cancelled by the SERVICE PROVIDER.
    CANCELLED_MANUALLY = 'CnclldMan'  # CnclldMan = Cancelled Manually. The execution has been cancelled by the operator.
    FINISHED = 'Fin'  # Fin = Finished. The execution has been finished.
    FINISHED_MOD = 'FinMod'  # FinMod = Finished with modification. As the requested target value could not be reached, the next best value has been chosen and used as target value.
    FAILED = 'Fail'  # The execution has been failed.


class InvocationError(StringEnum):
    UNSPECIFIED = 'Unspec'  # An unspecified error has occurred. No more information about the error is available.
    UNKNOWN_OPERATION = 'Unkn'  # Unknown Operation. The HANDLE to the operation object is not known.
    INVALID_VALUE = 'Inv'  # Invalid Value. The HANDLE to the operation object does not match the invocation request message
    OTHER = 'Oth'  # Another type of error has occurred. More information on the error MAY be available.


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


class InvocationInfo(PropertyBasedPMType):
    TransactionId = cp.NodeTextProperty(msg.TransactionId, UnsignedIntConverter, min_length=1)
    InvocationState = cp.NodeEnumTextProperty(InvocationState, msg.InvocationState)
    InvocationError = cp.NodeEnumTextProperty(InvocationError, msg.InvocationError, is_optional=True)
    InvocationErrorMessage = cp.SubElementListProperty(msg.InvocationErrorMessage, value_class=LocalizedText)
    _props = ['TransactionId', 'InvocationState', 'InvocationError', 'InvocationErrorMessage']

    def __repr__(self):
        if self.InvocationError:
            text = ', '.join([e.text for e in self.InvocationErrorMessage])
            return f'{self.__class__.__name__}(TransactionId={self.TransactionId}, ' \
                   f'InvocationState={self.InvocationState.value}, ' \
                   f'InvocationError={self.InvocationError}), ' \
                   f'InvocationErrorMessage={text}'
        return f'{self.__class__.__name__}(TransactionId={self.TransactionId}, InvocationState={self.InvocationState})'


class OperationInvokedReportPart(PropertyBasedPMType):
    InvocationInfo = cp.SubElementProperty(msg.InvocationInfo, value_class=InvocationInfo)
    InvocationSource = cp.SubElementProperty(msg.InvocationSource, value_class=InstanceIdentifier)
    OperationHandleRef = cp.HandleRefAttributeProperty('OperationHandleRef', is_optional=False)
    OperationTarget = cp.HandleRefAttributeProperty('OperationTarget')
    _props = ['InvocationInfo', 'InvocationSource', 'OperationHandleRef', 'OperationTarget']


class AbstractSetResponse(PropertyBasedPMType):
    Extension = cp.ExtensionNodeProperty(ext.Extension)
    InvocationInfo = cp.SubElementProperty(msg.InvocationInfo, value_class=InvocationInfo)
    MdibVersion = cp.VersionCounterAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.AnyURIAttributeProperty('SequenceId', is_optional=False)
    InstanceId = cp.UnsignedIntAttributeProperty('InstanceId')
    _props = ['Extension', 'InvocationInfo', 'MdibVersion', 'SequenceId', 'InstanceId']
