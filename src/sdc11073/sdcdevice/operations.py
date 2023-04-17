import inspect
import sys

from .sco import OperationDefinition
from ..xml_types import msg_qnames as msg, pm_qnames as pm


class SetStringOperation(OperationDefinition):
    OP_DESCR_QNAME = pm.SetStringOperationDescriptor
    OP_STATE_QNAME = pm.SetStringOperationState
    OP_QNAME = msg.SetString

    def __init__(self, handle, operation_target_handle, initial_value=None, coded_value=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value)
        self.current_value = initial_value

    @classmethod
    def from_operation_container(cls, operation_container):
        return cls(handle=operation_container.handle,
                   operation_target_handle=operation_container.OperationTarget,
                   initial_value=None, coded_value=None)


class SetValueOperation(OperationDefinition):
    OP_DESCR_QNAME = pm.SetValueOperationDescriptor
    OP_STATE_QNAME = pm.SetValueOperationState
    OP_QNAME = msg.SetValue

    def __init__(self, handle, operation_target_handle, initial_value=None, coded_value=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value)
        self.current_value = initial_value


class SetContextStateOperation(OperationDefinition):
    """Default implementation of SetContextOperation."""
    OP_DESCR_QNAME = pm.SetContextStateOperationDescriptor
    OP_STATE_QNAME = pm.SetContextStateOperationState
    OP_QNAME = msg.SetContextState

    def __init__(self, handle, operation_target_handle, coded_value=None):
        super().__init__(handle,
                         operation_target_handle,
                         coded_value=coded_value)

    @property
    def operation_target_storage(self):
        return self._mdib.context_states

    def _init_operation_target_container(self):
        """ initially no patient context is created."""

    @classmethod
    def from_operation_container(cls, operation_container):
        return cls(handle=operation_container.handle,
                   operation_target_handle=operation_container.OperationTarget)


class ActivateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = pm.ActivateOperationDescriptor
    OP_STATE_QNAME = pm.ActivateOperationState
    OP_QNAME = msg.Activate

    def __init__(self, handle, operation_target_handle, coded_value=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value)


class SetAlertStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = pm.SetAlertStateOperationDescriptor
    OP_STATE_QNAME = pm.SetAlertStateOperationState
    OP_QNAME = msg.SetAlertState

    def __init__(self, handle, operation_target_handle, coded_value=None, log_prefix=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value,
                         log_prefix=log_prefix)


class SetComponentStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = pm.SetComponentStateOperationDescriptor
    OP_STATE_QNAME = pm.SetComponentStateOperationState
    OP_QNAME = msg.SetComponentState

    def __init__(self, handle, operation_target_handle, coded_value=None, log_prefix=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value,
                         log_prefix=log_prefix)


class SetMetricStateOperation(OperationDefinition):
    """ This default implementation only registers calls, no manipulation of operation target
    """
    OP_DESCR_QNAME = pm.SetMetricStateOperationDescriptor
    OP_STATE_QNAME = pm.SetMetricStateOperationState
    OP_QNAME = msg.SetMetricState

    def __init__(self, handle, operation_target_handle, coded_value=None, log_prefix=None):
        super().__init__(handle=handle,
                         operation_target_handle=operation_target_handle,
                         coded_value=coded_value,
                         log_prefix=log_prefix)


# mapping of states: xsi:type information to classes
# find all classes in this module that have a member "OP_DESCR_QNAME"
_classes = inspect.getmembers(sys.modules[__name__],
                              lambda member: inspect.isclass(member) and member.__module__ == __name__)
_classes_with_QNAME = [c[1] for c in _classes if hasattr(c[1], 'OP_DESCR_QNAME') and c[1].OP_DESCR_QNAME is not None]
# make a dictionary from found classes: (Key is OP_DESCR_QNAME, value is the class itself
_operation_lookup_by_type = {c.OP_DESCR_QNAME: c for c in _classes_with_QNAME}


def get_operation_class(q_name):
    """
    :param q_name: a QName instance
    """
    return _operation_lookup_by_type.get(q_name)
