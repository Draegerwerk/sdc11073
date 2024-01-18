from __future__ import annotations

import inspect
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Protocol

from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.exceptions import ApiUsageError
from sdc11073.xml_types import pm_qnames as pm

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.pysoap.soapenvelope import ReceivedSoapMessage
    from sdc11073.xml_types.msg_types import AbstractSet, InvocationState
    from sdc11073.xml_types.pm_types import CodedValue, OperatingMode


class OperationDefinitionProtocol(Protocol):
    """Interface that ExecuteHandlers need."""

    handle: str
    operation_target_handle: str
    current_value: Any
    last_called_time: float | None
    descriptor_container: AbstractDescriptorProtocol


@dataclass
class ExecuteParameters:
    """The argument of the ExecuteHandler call."""

    operation_instance: OperationDefinitionProtocol
    operation_request: AbstractSet
    soap_message: ReceivedSoapMessage


@dataclass
class ExecuteResult:
    """The return value of the ExecuteHandler call."""

    operation_target_handle: str
    invocation_state: InvocationState  # = InvocationState.FINISHED # only return a final state, not WAIT or STARTED


# ExecuteHandler also get the full soap message as parameter, because the soap header might contain
# relevant information, e.g. safety requirements.
ExecuteHandler = Callable[[ExecuteParameters], ExecuteResult]
TimeoutHandler = Callable[[OperationDefinitionProtocol], None]


class OperationDefinitionBase:
    """Base class of all provided operations.

    An operation is a point for remote control over the network.
    """

    current_value: Any = properties.ObservableProperty(fire_only_on_changed_value=False)
    current_request = properties.ObservableProperty(fire_only_on_changed_value=False)
    current_argument = properties.ObservableProperty(fire_only_on_changed_value=False)
    on_timeout = properties.ObservableProperty(fire_only_on_changed_value=False)
    OP_DESCR_QNAME: QName | None = None  # to be defined in derived classes
    OP_STATE_QNAME: QName | None = None # to be defined in derived classes

    def __init__(self,  # noqa: PLR0913
                 handle: str,
                 operation_target_handle: str,
                 operation_handler: ExecuteHandler,
                 timeout_handler: TimeoutHandler | None = None,
                 coded_value: CodedValue | None = None,
                 delayed_processing: bool = True,
                 log_prefix: str | None = None):
        """Construct a OperationDefinitionBase.

        :param handle: the handle of the operation itself.
        :param operation_target_handle: the handle of the modified data (MdDescription)
        :param coded_value: a pmtypes.CodedValue instance
        :param delayed_processing: if True, device returns WAIT, and sends notifications WAIT, STARTED, FINISHED/FAILED
                                   if False, device returns FINISHED/FAILED, and sends same state in notification
        """
        self._logger = loghelper.get_logger_adapter(f'sdc.device.op.{self.__class__.__name__}', log_prefix)
        self._mdib: ProviderMdib | None = None
        self._descriptor_container = None
        self._operation_state_container = None
        self.handle: str = handle
        self.operation_target_handle: str = operation_target_handle
        # documentation of operation_target_handle:
        # A HANDLE reference this operation is targeted to. In case of a single state this is the HANDLE of the
        # descriptor.
        # In case that multiple states may belong to one descriptor (pm:AbstractMultiState),
        # OperationTarget is the HANDLE of one of the state instances (if the state is modified by the operation).
        self._operation_handler = operation_handler
        self._timeout_handler = timeout_handler
        self._coded_value = coded_value
        self.delayed_processing = delayed_processing
        self.calls = []  # record when operation was called
        self.last_called_time = None

    @property
    def descriptor_container(self) -> AbstractDescriptorProtocol:  # noqa: D102
        return self._descriptor_container

    def execute_operation(self,
                          soap_request: ReceivedSoapMessage,
                          operation_request: AbstractSet) -> ExecuteResult:
        """Execute the operation itself.

        This method calls the provided operation_handler.
        """
        self.calls.append((time.time(), soap_request))
        execute_result = self._operation_handler(ExecuteParameters(self, operation_request, soap_request))
        self.current_request = soap_request
        self.current_argument = operation_request.argument
        self.last_called_time = time.time()
        return execute_result

    def check_timeout(self):
        """Set on_timeout observable if timeout is detected."""
        if self.last_called_time is None:
            return
        if self._descriptor_container.InvocationEffectiveTimeout is None:
            return
        age = time.time() - self.last_called_time
        if age < self._descriptor_container.InvocationEffectiveTimeout:
            return
        if self._timeout_handler is not None:
            self._timeout_handler(self)
        self.on_timeout = True  # let observable fire

    def set_mdib(self, mdib: ProviderMdib, parent_descriptor_handle: str):
        """Set mdib reference.

        The operation needs to know the mdib that it operates on.
        This is called by SubscriptionManager on registration.
        Needs to be implemented by derived classes if specific things have to be initialized.
        """
        if self._mdib is not None:
            raise ApiUsageError('Mdib is already set')
        self._mdib = mdib
        self._logger.log_prefix = mdib.log_prefix  # use same prefix as mdib for logging
        self._descriptor_container = self._mdib.descriptions.handle.get_one(self.handle, allow_none=True)
        if self._descriptor_container is not None:
            # there is already a descriptor
            self._logger.debug('descriptor for operation "%s" is already present, re-using it', self.handle)
        else:
            cls = mdib.data_model.get_descriptor_container_class(self.OP_DESCR_QNAME)
            self._descriptor_container = cls(self.handle, parent_descriptor_handle)
            self._init_operation_descriptor_container()
            # ToDo: transaction context for flexibility to add operations at runtime
            mdib.descriptions.add_object(self._descriptor_container)

        self._operation_state_container = self._mdib.states.descriptor_handle.get_one(self.handle, allow_none=True)
        if self._operation_state_container is not None:
            self._logger.debug('operation state for operation "%s" is already present, re-using it', self.handle)
        else:
            cls = mdib.data_model.get_state_container_class(self.OP_STATE_QNAME)
            self._operation_state_container = cls(self._descriptor_container)
            mdib.states.add_object(self._operation_state_container)

    def _init_operation_descriptor_container(self):
        self._descriptor_container.OperationTarget = self.operation_target_handle
        if self._coded_value is not None:
            self._descriptor_container.Type = self._coded_value

    def set_operating_mode(self, mode: OperatingMode):
        """Set OperatingMode member in state in transaction context."""
        with self._mdib.operational_state_transaction() as mgr:
            state = mgr.get_state(self.handle)
            state.OperatingMode = mode

    def __str__(self):
        code = None if self._descriptor_container is None else self._descriptor_container.Type
        return (f'{self.__class__.__name__} handle={self.handle} code={code} '
               f'operation-target={self.operation_target_handle}')


class SetStringOperation(OperationDefinitionBase):
    """Implementation of SetString operation."""

    OP_DESCR_QNAME = pm.SetStringOperationDescriptor
    OP_STATE_QNAME = pm.SetStringOperationState


class SetValueOperation(OperationDefinitionBase):
    """Implementation of SetValue operation."""

    OP_DESCR_QNAME = pm.SetValueOperationDescriptor
    OP_STATE_QNAME = pm.SetValueOperationState


class SetContextStateOperation(OperationDefinitionBase):
    """Implementation of SetContextOperation."""

    OP_DESCR_QNAME = pm.SetContextStateOperationDescriptor
    OP_STATE_QNAME = pm.SetContextStateOperationState


class ActivateOperation(OperationDefinitionBase):
    """Parameters of an ActivateOperation."""

    OP_DESCR_QNAME = pm.ActivateOperationDescriptor
    OP_STATE_QNAME = pm.ActivateOperationState


class SetAlertStateOperation(OperationDefinitionBase):
    """Parameters of an SetAlertStateOperation."""

    OP_DESCR_QNAME = pm.SetAlertStateOperationDescriptor
    OP_STATE_QNAME = pm.SetAlertStateOperationState


class SetComponentStateOperation(OperationDefinitionBase):
    """Parameters of an SetComponentStateOperation."""

    OP_DESCR_QNAME = pm.SetComponentStateOperationDescriptor
    OP_STATE_QNAME = pm.SetComponentStateOperationState


class SetMetricStateOperation(OperationDefinitionBase):
    """Parameters of an SetMetricStateOperation."""

    OP_DESCR_QNAME = pm.SetMetricStateOperationDescriptor
    OP_STATE_QNAME = pm.SetMetricStateOperationState


# mapping of states: xsi:type information to classes
# find all classes in this module that have a member "OP_DESCR_QNAME"
_classes = inspect.getmembers(sys.modules[__name__],
                              lambda member: inspect.isclass(member) and member.__module__ == __name__)
_classes_with_qname = [c[1] for c in _classes if hasattr(c[1], 'OP_DESCR_QNAME') and c[1].OP_DESCR_QNAME is not None]
# make a dictionary from found classes: (Key is OP_DESCR_QNAME, value is the class itself
_operation_lookup_by_type = {c.OP_DESCR_QNAME: c for c in _classes_with_qname}


def get_operation_class(q_name: QName) -> type[OperationDefinitionBase]:
    """:param q_name: a QName instance"""
    return _operation_lookup_by_type.get(q_name)
