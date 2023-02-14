""" Implementation of some data types used in Message Model"""
import enum
from abc import abstractmethod

from . import ext_qnames as ext
from . import msg_qnames as msg
from . import pm_qnames as pm
from .dataconverters import UnsignedIntConverter, StringConverter
from .mdib import containerproperties as cp
from .mdib.descriptorcontainers import AbstractDescriptorContainer, MdsDescriptorContainer, VmdDescriptorContainer
from .mdib.descriptorcontainers import ChannelDescriptorContainer, AbstractMetricDescriptorContainer
from .mdib.descriptorcontainers import get_container_class as get_descriptor_container_class
from .mdib.statecontainers import AbstractAlertStateContainer, AbstractDeviceComponentStateContainer
from .mdib.statecontainers import AbstractStateContainer, AbstractContextStateContainer, AbstractOperationStateContainer
from .mdib.statecontainers import RealTimeSampleArrayMetricStateContainer
from .mdib.statecontainers import get_container_class as get_state_container_class
from .namespaces import default_ns_helper
from .pmtypes import PropertyBasedPMType, LocalizedText, InstanceIdentifier


# pylint: disable=invalid-name

class StringEnum(str, enum.Enum):

    def __str__(self):
        return str(self.value)


class DescriptionModificationType(StringEnum):
    CREATE = 'Crt'
    UPDATE = 'Upt'
    DELETE = 'Del'


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


### Reports ###
class AbstractReportPart(PropertyBasedPMType):
    SourceMds = cp.NodeTextProperty(msg.SourceMds, StringConverter, is_optional=True)
    _props = ['SourceMds']


class AbstractReport(PropertyBasedPMType):
    MdibVersion = cp.IntegerAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.StringAttributeProperty('SequenceId')
    InstanceId = cp.IntegerAttributeProperty('InstanceId')
    _props = ['MdibVersion', 'SequenceId', 'InstanceId']

    def set_mdib_version_group(self, mdib_version_group):
        self.MdibVersion = mdib_version_group.mdib_version
        self.SequenceId = mdib_version_group.sequence_id
        self.InstanceId = mdib_version_group.instance_id


class ReportPartValuesList(AbstractReportPart):
    @property
    @abstractmethod
    def values_list(self):
        return NotImplementedError


##### Metric Report ###
class MetricReportPart(ReportPartValuesList):
    MetricState = cp.ContainerListProperty(msg.MetricState,
                                           value_class=AbstractContextStateContainer,
                                           cls_getter=get_state_container_class,
                                           ns_helper=default_ns_helper)
    _props = ['MetricState']

    @property
    def values_list(self):
        return self.MetricState


class AbstractMetricReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=MetricReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> MetricReportPart:
        self.ReportPart.append(MetricReportPart())
        return self.ReportPart[-1]


##### Context Report ###
class ContextReportPart(ReportPartValuesList):
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper)
    _props = ['ContextState']

    @property
    def values_list(self):
        return self.ContextState


class AbstractContextReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=ContextReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> ContextReportPart:
        self.ReportPart.append(ContextReportPart())
        return self.ReportPart[-1]


##### Operational State Report ###
class OperationalStateReportPart(ReportPartValuesList):
    OperationState = cp.ContainerListProperty(msg.OperationState,
                                              value_class=AbstractOperationStateContainer,
                                              cls_getter=get_state_container_class,
                                              ns_helper=default_ns_helper)
    _props = ['OperationState']

    @property
    def values_list(self):
        return self.OperationState


class AbstractOperationalStateReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=OperationalStateReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> OperationalStateReportPart:
        self.ReportPart.append(OperationalStateReportPart())
        return self.ReportPart[-1]


##### Alert Report ###
class AlertReportPart(ReportPartValuesList):
    AlertState = cp.ContainerListProperty(msg.AlertState,
                                          value_class=AbstractAlertStateContainer,
                                          cls_getter=get_state_container_class,
                                          ns_helper=default_ns_helper)
    _props = ['AlertState']

    @property
    def values_list(self):
        return self.AlertState


class AbstractAlertReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=AlertReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> AlertReportPart:
        self.ReportPart.append(AlertReportPart())
        return self.ReportPart[-1]


##### Component Report ###
class ComponentReportPart(ReportPartValuesList):
    ComponentState = cp.ContainerListProperty(msg.ComponentState,
                                              value_class=AbstractDeviceComponentStateContainer,
                                              cls_getter=get_state_container_class,
                                              ns_helper=default_ns_helper)
    _props = ['ComponentState']

    @property
    def values_list(self):
        return self.ComponentState


class AbstractComponentReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=ComponentReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> ComponentReportPart:
        self.ReportPart.append(ComponentReportPart())
        return self.ReportPart[-1]


##### Operation Invoked Report ###
class OperationInvokedReportPart(AbstractReportPart):
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


### WaveformStream ###
# no report parts in this report!
class WaveformStream(AbstractReport):
    State = cp.ContainerListProperty(msg.State,
                                     value_class=RealTimeSampleArrayMetricStateContainer,
                                     cls_getter=get_state_container_class,
                                     ns_helper=default_ns_helper)
    _props = ['State']


### DescriptionModificationReport ###
class DescriptionModificationReportPart(AbstractReportPart):
    Descriptor = cp.ContainerListProperty(msg.Descriptor,
                                          value_class=AbstractDescriptorContainer,
                                          cls_getter=get_descriptor_container_class,
                                          ns_helper=default_ns_helper)
    State = cp.ContainerListProperty(msg.State,
                                     value_class=AbstractStateContainer,
                                     cls_getter=get_state_container_class,
                                     ns_helper=default_ns_helper)
    ParentDescriptor = cp.HandleRefAttributeProperty('ParentDescriptor')
    ModificationType = cp.EnumAttributeProperty('ModificationType',
                                                enum_cls=DescriptionModificationType,
                                                implied_py_value=DescriptionModificationType.UPDATE)
    _props = ['Descriptor', 'State', 'ParentDescriptor', 'ModificationType']


class DescriptionModificationReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=DescriptionModificationReportPart)
    _props = ['ReportPart']

    def add_report_part(self) -> DescriptionModificationReportPart:
        self.ReportPart.append(DescriptionModificationReportPart())
        return self.ReportPart[-1]


class AbstractGetResponse(PropertyBasedPMType):
    MdibVersion = cp.IntegerAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.StringAttributeProperty('SequenceId')
    InstanceId = cp.IntegerAttributeProperty('InstanceId')
    _props = ['MdibVersion', 'SequenceId', 'InstanceId']

    def set_mdib_version_group(self, mdib_version_group):
        self.MdibVersion = mdib_version_group.mdib_version
        self.SequenceId = mdib_version_group.sequence_id
        self.InstanceId = mdib_version_group.instance_id


class Channel(PropertyBasedPMType):
    container = cp.ContainerProperty(None,
                                     value_class=ChannelDescriptorContainer,
                                     cls_getter=get_descriptor_container_class,
                                     ns_helper=default_ns_helper)
    Metric = cp.ContainerListProperty(pm.Metric,
                                      value_class=AbstractMetricDescriptorContainer,
                                      cls_getter=get_descriptor_container_class,
                                      ns_helper=default_ns_helper)
    _props = ['container', 'Metric']


class Vmd(PropertyBasedPMType):
    container = cp.ContainerProperty(None,
                                     value_class=VmdDescriptorContainer,
                                     cls_getter=get_descriptor_container_class,
                                     ns_helper=default_ns_helper)
    Channel = cp.SubElementListProperty(pm.Channel, value_class=Channel)
    _props = ['container', 'Channel']


class Mds(PropertyBasedPMType):
    container = cp.ContainerProperty(None,
                                     value_class=MdsDescriptorContainer,
                                     cls_getter=get_descriptor_container_class,
                                     ns_helper=default_ns_helper)
    Vmd = cp.SubElementListProperty(pm.Vmd, value_class=Vmd)
    _props = ['container', 'Vmd']


class MdDescription(PropertyBasedPMType):
    Mds = cp.SubElementListProperty(pm.Mds, value_class=Mds)
    _props = ['Mds']


class GetMdDescriptionResponse(AbstractGetResponse):
    MdDescription = cp.SubElementProperty(msg.MdDescription, value_class=MdDescription)
    _props = ['MdDescription']


class MdState(PropertyBasedPMType):
    State = cp.ContainerListProperty(pm.State,
                                     value_class=AbstractStateContainer,
                                     cls_getter=get_state_container_class,
                                     ns_helper=default_ns_helper
                                     )
    _props = ['State']


class GetMdStateResponse(AbstractGetResponse):
    MdState = cp.SubElementProperty(msg.MdState, value_class=MdState)
    _props = ['MdState']


class GetContextStatesResponse(AbstractGetResponse):
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ['ContextState']


class GetContextStatesByIdentificationResponse(AbstractGetResponse):
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ['ContextState']


class GetContextStatesByFilterResponse(AbstractGetResponse):
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ['ContextState']
