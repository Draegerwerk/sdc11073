""" Implementation of some data types used in Message Model"""
import enum
from abc import abstractmethod

from . import ext_qnames as ext
from . import msg_qnames as msg
from . import pm_qnames as pm
from . import pm_types
from . import xml_structure as cp
from .actions import Actions
from .basetypes import MessageType
from .dataconverters import UnsignedIntConverter, DecimalConverter
from .pm_types import PropertyBasedPMType, LocalizedText, InstanceIdentifier, LocalizedTextWidth, ContainmentTree
from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer, MdsDescriptorContainer, VmdDescriptorContainer
from sdc11073.mdib.descriptorcontainers import ChannelDescriptorContainer, AbstractMetricDescriptorContainer
from sdc11073.mdib.descriptorcontainers import get_container_class as get_descriptor_container_class
from sdc11073.mdib.statecontainers import AbstractAlertStateContainer, AbstractDeviceComponentStateContainer
from sdc11073.mdib.statecontainers import AbstractMetricStateContainer
from sdc11073.mdib.statecontainers import AbstractOperationStateContainer
from sdc11073.mdib.statecontainers import AbstractStateContainer, AbstractContextStateContainer
from sdc11073.mdib.statecontainers import RealTimeSampleArrayMetricStateContainer
from sdc11073.mdib.statecontainers import get_container_class as get_state_container_class
from sdc11073.namespaces import default_ns_helper


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


### Reports ###
class AbstractReportPart(PropertyBasedPMType):
    SourceMds = cp.NodeStringProperty(msg.SourceMds, is_optional=True)
    _props = ('SourceMds',)


class AbstractReport(MessageType):
    MdibVersion = cp.IntegerAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.StringAttributeProperty('SequenceId')
    InstanceId = cp.IntegerAttributeProperty('InstanceId')
    _props = ('MdibVersion', 'SequenceId', 'InstanceId')
    additional_namespaces = (default_ns_helper.XSI,)

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
    _props = ('MetricState',)

    @property
    def values_list(self):
        return self.MetricState


class AbstractMetricReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=MetricReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> MetricReportPart:
        self.ReportPart.append(MetricReportPart())
        return self.ReportPart[-1]


class EpisodicMetricReport(AbstractMetricReport):
    NODETYPE = msg.EpisodicMetricReport
    action = Actions.EpisodicMetricReport


class PeriodicMetricReport(AbstractMetricReport):
    NODETYPE = msg.PeriodicMetricReport
    action = Actions.PeriodicMetricReport


##### Context Report ###
class ContextReportPart(ReportPartValuesList):
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper)
    _props = ('ContextState',)

    @property
    def values_list(self):
        return self.ContextState


class AbstractContextReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=ContextReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> ContextReportPart:
        self.ReportPart.append(ContextReportPart())
        return self.ReportPart[-1]


class EpisodicContextReport(AbstractContextReport):
    NODETYPE = msg.EpisodicContextReport
    action = Actions.EpisodicContextReport


class PeriodicContextReport(AbstractContextReport):
    NODETYPE = msg.PeriodicContextReport
    action = Actions.PeriodicContextReport


##### Operational State Report ###
class OperationalStateReportPart(ReportPartValuesList):
    OperationState = cp.ContainerListProperty(msg.OperationState,
                                              value_class=AbstractOperationStateContainer,
                                              cls_getter=get_state_container_class,
                                              ns_helper=default_ns_helper)
    _props = ('OperationState',)

    @property
    def values_list(self):
        return self.OperationState


class AbstractOperationalStateReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=OperationalStateReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> OperationalStateReportPart:
        self.ReportPart.append(OperationalStateReportPart())
        return self.ReportPart[-1]


class EpisodicOperationalStateReport(AbstractOperationalStateReport):
    NODETYPE = msg.EpisodicOperationalStateReport
    action = Actions.EpisodicOperationalStateReport


class PeriodicOperationalStateReport(AbstractOperationalStateReport):
    NODETYPE = msg.PeriodicOperationalStateReport
    action = Actions.PeriodicOperationalStateReport


##### Alert Report ###
class AlertReportPart(ReportPartValuesList):
    AlertState = cp.ContainerListProperty(msg.AlertState,
                                          value_class=AbstractAlertStateContainer,
                                          cls_getter=get_state_container_class,
                                          ns_helper=default_ns_helper)
    _props = ('AlertState',)

    @property
    def values_list(self):
        return self.AlertState


class AbstractAlertReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=AlertReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> AlertReportPart:
        self.ReportPart.append(AlertReportPart())
        return self.ReportPart[-1]


class EpisodicAlertReport(AbstractAlertReport):
    NODETYPE = msg.EpisodicAlertReport
    action = Actions.EpisodicAlertReport


class PeriodicAlertReport(AbstractAlertReport):
    NODETYPE = msg.PeriodicAlertReport
    action = Actions.PeriodicAlertReport


##### Component Report ###
class ComponentReportPart(ReportPartValuesList):
    ComponentState = cp.ContainerListProperty(msg.ComponentState,
                                              value_class=AbstractDeviceComponentStateContainer,
                                              cls_getter=get_state_container_class,
                                              ns_helper=default_ns_helper)
    _props = ('ComponentState',)

    @property
    def values_list(self):
        return self.ComponentState


class AbstractComponentReport(AbstractReport):
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=ComponentReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> ComponentReportPart:
        self.ReportPart.append(ComponentReportPart())
        return self.ReportPart[-1]


class EpisodicComponentReport(AbstractComponentReport):
    NODETYPE = msg.EpisodicComponentReport
    action = Actions.EpisodicComponentReport


class PeriodicComponentReport(AbstractComponentReport):
    NODETYPE = msg.PeriodicComponentReport
    action = Actions.PeriodicComponentReport


##### Operation Invoked Report ###
class InvocationInfo(PropertyBasedPMType):
    TransactionId = cp.NodeTextProperty(msg.TransactionId, UnsignedIntConverter, min_length=1)
    InvocationState = cp.NodeEnumTextProperty(msg.InvocationState, InvocationState)
    InvocationError = cp.NodeEnumTextProperty(msg.InvocationError, InvocationError, is_optional=True)
    InvocationErrorMessage = cp.SubElementListProperty(msg.InvocationErrorMessage, value_class=LocalizedText)
    _props = ('TransactionId', 'InvocationState', 'InvocationError', 'InvocationErrorMessage')

    def __repr__(self):
        if self.InvocationError:
            text = ', '.join([e.text for e in self.InvocationErrorMessage])
            return f'{self.__class__.__name__}(TransactionId={self.TransactionId}, ' \
                   f'InvocationState={self.InvocationState.value}, ' \
                   f'InvocationError={self.InvocationError}), ' \
                   f'InvocationErrorMessage={text}'
        return f'{self.__class__.__name__}(TransactionId={self.TransactionId}, InvocationState={self.InvocationState})'

    def add_error_message(self, text: str, lang=None, ref=None, version=None, text_width=None):
        self.InvocationErrorMessage.append(LocalizedText(text, lang, ref, version, text_width))


class OperationInvokedReportPart(AbstractReportPart):
    InvocationInfo = cp.SubElementProperty(msg.InvocationInfo,
                                           value_class=InvocationInfo,
                                           default_py_value=InvocationInfo())
    InvocationSource = cp.SubElementProperty(msg.InvocationSource, value_class=InstanceIdentifier)
    OperationHandleRef = cp.HandleRefAttributeProperty('OperationHandleRef', is_optional=False)
    OperationTarget = cp.HandleRefAttributeProperty('OperationTarget')
    _props = ('InvocationInfo', 'InvocationSource', 'OperationHandleRef', 'OperationTarget')


class OperationInvokedReport(AbstractReport):
    NODETYPE = msg.OperationInvokedReport
    action = Actions.OperationInvokedReport
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=OperationInvokedReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> OperationInvokedReportPart:
        self.ReportPart.append(OperationInvokedReportPart())
        return self.ReportPart[-1]


class AbstractSetResponse(MessageType):
    Extension = cp.ExtensionNodeProperty(ext.Extension)
    InvocationInfo = cp.SubElementProperty(msg.InvocationInfo,
                                           value_class=InvocationInfo,
                                           default_py_value=InvocationInfo())
    MdibVersion = cp.VersionCounterAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.AnyURIAttributeProperty('SequenceId', is_optional=False)
    InstanceId = cp.UnsignedIntAttributeProperty('InstanceId')
    _props = ('Extension', 'InvocationInfo', 'MdibVersion', 'SequenceId', 'InstanceId')


class SetContextStateResponse(AbstractSetResponse):
    NODETYPE = msg.SetContextStateResponse
    action = Actions.SetContextStateResponse


class SetValueResponse(AbstractSetResponse):
    NODETYPE = msg.SetValueResponse
    action = Actions.SetValueResponse


class SetStringResponse(AbstractSetResponse):
    NODETYPE = msg.SetStringResponse
    action = Actions.SetStringResponse


class ActivateResponse(AbstractSetResponse):
    NODETYPE = msg.ActivateResponse
    action = Actions.ActivateResponse


class SetAlertStateResponse(AbstractSetResponse):
    NODETYPE = msg.SetAlertStateResponse
    action = Actions.SetAlertStateResponse


class SetComponentStateResponse(AbstractSetResponse):
    NODETYPE = msg.SetComponentStateResponse
    action = Actions.SetComponentStateResponse


class SetMetricStateResponse(AbstractSetResponse):
    NODETYPE = msg.SetMetricStateResponse
    action = Actions.SetMetricStateResponse


### WaveformStream ###
# no report parts in this report!
class WaveformStream(AbstractReport):
    NODETYPE = msg.WaveformStream
    action = Actions.Waveform
    State = cp.ContainerListProperty(msg.State,
                                     value_class=RealTimeSampleArrayMetricStateContainer,
                                     cls_getter=get_state_container_class,
                                     ns_helper=default_ns_helper)
    _props = ('State',)


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
    _props = ('Descriptor', 'State', 'ParentDescriptor', 'ModificationType')


class DescriptionModificationReport(AbstractReport):
    NODETYPE = msg.DescriptionModificationReport
    action = Actions.DescriptionModificationReport
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=DescriptionModificationReportPart)
    _props = ('ReportPart',)

    def add_report_part(self) -> DescriptionModificationReportPart:
        self.ReportPart.append(DescriptionModificationReportPart())
        return self.ReportPart[-1]

    @classmethod
    def from_node(cls, node):
        instance = super().from_node(node)
        # ser parent_handle members of descriptors
        for report_part in instance.ReportPart:
            for d in report_part.Descriptor:
                d.parent_handle = report_part.ParentDescriptor
        return instance


class SystemErrorReportPart(AbstractReportPart):
    ErrorCode = cp.SubElementProperty(msg.ErrorCode, value_class=pm_types.CodedValue)
    ErrorInfo = cp.SubElementListProperty(msg.ErrorInfo, value_class=pm_types.LocalizedText)
    _props = ('ErrorCode', 'ErrorInfo')


class SystemErrorReport(AbstractReport):
    NODETYPE = msg.SystemErrorReport
    action = Actions.SystemErrorReport
    ReportPart = cp.SubElementListProperty(msg.ReportPart, value_class=SystemErrorReportPart)
    _props = ('ReportPart',)


class AbstractGet(MessageType):
    pass


class AbstractSet(MessageType):
    OperationHandleRef = cp.NodeStringProperty(msg.OperationHandleRef)
    _props = ('OperationHandleRef',)

    @property
    def argument(self):
        return None


class AbstractGetResponse(MessageType):
    MdibVersion = cp.IntegerAttributeProperty('MdibVersion', implied_py_value=0)
    SequenceId = cp.StringAttributeProperty('SequenceId')
    InstanceId = cp.IntegerAttributeProperty('InstanceId')
    _props = ('MdibVersion', 'SequenceId', 'InstanceId')

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
    _props = ('container', 'Metric')


class Vmd(PropertyBasedPMType):
    container = cp.ContainerProperty(None,
                                     value_class=VmdDescriptorContainer,
                                     cls_getter=get_descriptor_container_class,
                                     ns_helper=default_ns_helper)
    Channel = cp.SubElementListProperty(pm.Channel, value_class=Channel)
    _props = ('container', 'Channel')


class Mds(PropertyBasedPMType):
    container = cp.ContainerProperty(None,
                                     value_class=MdsDescriptorContainer,
                                     cls_getter=get_descriptor_container_class,
                                     ns_helper=default_ns_helper)
    Vmd = cp.SubElementListProperty(pm.Vmd, value_class=Vmd)
    _props = ('container', 'Vmd')


class MdDescription(PropertyBasedPMType):
    Mds = cp.SubElementListProperty(pm.Mds, value_class=Mds)
    _props = ('Mds',)


class MdState(PropertyBasedPMType):
    State = cp.ContainerListProperty(pm.State,
                                     value_class=AbstractStateContainer,
                                     cls_getter=get_state_container_class,
                                     ns_helper=default_ns_helper
                                     )
    _props = ('State',)


# class Mdib(PropertyBasedPMType):
#     Extension = cp.ExtensionNodeProperty(ext.Extension)
#     MdDescription = cp.SubElementProperty(pm.MdDescription, value_class=MdDescription)
#     MdState = cp.SubElementProperty(pm.MdState, value_class=MdState)
#     _props = ['Extension', 'MdDescription', 'MdState']
#

class GetMdib(AbstractGet):
    NODETYPE = msg.GetMdib
    action = Actions.GetMdib


class GetMdibResponse(AbstractGetResponse):
    NODETYPE = msg.GetMdibResponse
    action = Actions.GetMdibResponse
    Mdib = cp.AnyEtreeNodeProperty(None)
    _props = ('Mdib',)


class GetMdDescription(AbstractGet):
    NODETYPE = msg.GetMdDescription
    action = Actions.GetMdDescription
    HandleRef = cp.SubElementHandleRefListProperty(msg.HandleRef)
    _props = ('HandleRef',)


class GetMdDescriptionResponse(AbstractGetResponse):
    NODETYPE = msg.GetMdDescriptionResponse
    action = Actions.GetMdDescriptionResponse
    MdDescription = cp.SubElementProperty(msg.MdDescription,
                                          value_class=MdDescription,
                                          default_py_value=MdDescription())
    _props = ('MdDescription',)


class GetMdState(AbstractGet):
    NODETYPE = msg.GetMdState
    action = Actions.GetMdState
    HandleRef = cp.SubElementHandleRefListProperty(msg.HandleRef)
    _props = ('HandleRef',)


class GetMdStateResponse(AbstractGetResponse):
    NODETYPE = msg.GetMdStateResponse
    action = Actions.GetMdStateResponse
    MdState = cp.SubElementProperty(msg.MdState, value_class=MdState, default_py_value=MdState())
    _props = ('MdState',)


class GetDescriptor(AbstractGet):
    NODETYPE = msg.GetDescriptor
    action = Actions.GetDescriptor
    HandleRef = cp.SubElementHandleRefListProperty(msg.HandleRef)
    _props = ('HandleRef',)


class GetDescriptorResponse(AbstractGetResponse):
    NODETYPE = msg.GetDescriptorResponse
    action = Actions.GetDescriptorResponse
    Descriptor = cp.ContainerListProperty(msg.Descriptor,
                                          value_class=AbstractDescriptorContainer,
                                          cls_getter=get_state_container_class,
                                          ns_helper=default_ns_helper
                                          )
    _props = ('Descriptor',)


class GetContainmentTree(AbstractGet):
    NODETYPE = msg.GetContainmentTree
    action = Actions.GetContainmentTree
    HandleRef = cp.SubElementHandleRefListProperty(msg.HandleRef)
    _props = ('HandleRef',)


class GetContainmentTreeResponse(AbstractGetResponse):
    NODETYPE = msg.GetContainmentTreeResponse
    action = Actions.GetContainmentTreeResponse
    ContainmentTree = cp.SubElementListProperty(msg.ContainmentTree, value_class=ContainmentTree)
    _props = ('ContainmentTree',)


class GetContextStates(AbstractGet):
    NODETYPE = msg.GetContextStates
    action = Actions.GetContextStates
    HandleRef = cp.SubElementHandleRefListProperty(msg.HandleRef)
    _props = ('HandleRef',)


class GetContextStatesResponse(AbstractGetResponse):
    NODETYPE = msg.GetContextStatesResponse
    action = Actions.GetContextStatesResponse
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ('ContextState',)


class GetContextStatesByIdentification(AbstractGet):
    NODETYPE = msg.GetContextStatesByIdentification
    action = Actions.GetContextStatesByIdentification
    Identification = cp.SubElementListProperty(msg.Identification, value_class=InstanceIdentifier)
    ContextType = cp.QNameAttributeProperty('ContextType')
    _props = ('HandleRef',)


class GetContextStatesByIdentificationResponse(AbstractGetResponse):
    NODETYPE = msg.GetContextStatesByIdentificationResponse
    action = Actions.GetContextStatesByIdentificationResponse
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ('ContextState',)


class GetContextStatesByFilter(AbstractGet):
    NODETYPE = msg.GetContextStatesByFilter
    action = Actions.GetContextStatesByFilter
    Filter = cp.SubElementStringListProperty(msg.Filter)
    _props = ('HandleRef',)


class GetContextStatesByFilterResponse(AbstractGetResponse):
    NODETYPE = msg.GetContextStatesByFilterResponse
    action = Actions.GetContextStatesByFilterResponse
    ContextState = cp.ContainerListProperty(msg.ContextState,
                                            value_class=AbstractContextStateContainer,
                                            cls_getter=get_state_container_class,
                                            ns_helper=default_ns_helper
                                            )
    _props = ('ContextState',)


class GetSupportedLanguages(AbstractGet):
    NODETYPE = msg.GetSupportedLanguages
    action = Actions.GetSupportedLanguages


class GetSupportedLanguagesResponse(AbstractGetResponse):
    NODETYPE = msg.GetSupportedLanguagesResponse
    action = Actions.GetSupportedLanguagesResponse
    Lang = cp.SubElementStringListProperty(msg.Lang)
    _props = ('Lang',)


class GetLocalizedText(AbstractGet):
    NODETYPE = msg.GetLocalizedText
    action = Actions.GetLocalizedText
    Ref = cp.SubElementHandleRefListProperty(msg.Ref)
    Version = cp.NodeIntProperty(msg.Version, is_optional=True)
    Lang = cp.SubElementStringListProperty(msg.Lang)
    TextWidth = cp.SubElementTextListProperty(msg.TextWidth, value_class=LocalizedTextWidth)
    NumberOfLines = cp.SubElementTextListProperty(msg.NumberOfLines, value_class=int)
    _props = ('Ref', 'Version', 'Lang', 'TextWidth', 'NumberOfLines')


class GetLocalizedTextResponse(AbstractGetResponse):
    NODETYPE = msg.GetLocalizedTextResponse
    action = Actions.GetLocalizedTextResponse
    Text = cp.SubElementListProperty(msg.Text, value_class=LocalizedText)
    _props = ('Text',)


class SetContextState(AbstractSet):
    NODETYPE = msg.SetContextState
    action = Actions.SetContextState
    ProposedContextState = cp.ContainerListProperty(msg.ProposedContextState,
                                                    value_class=AbstractContextStateContainer,
                                                    cls_getter=get_state_container_class,
                                                    ns_helper=default_ns_helper)
    _props = ('ProposedContextState',)

    @property
    def argument(self):
        return self.ProposedContextState


class SetValue(AbstractSet):
    NODETYPE = msg.SetValue
    action = Actions.SetValue
    RequestedNumericValue = cp.NodeDecimalProperty(msg.RequestedNumericValue)
    _props = ('RequestedNumericValue',)

    @property
    def argument(self):
        return self.RequestedNumericValue


class SetString(AbstractSet):
    NODETYPE = msg.SetString
    action = Actions.SetString
    RequestedStringValue = cp.NodeStringProperty(msg.RequestedStringValue)
    _props = ('RequestedStringValue',)

    @property
    def argument(self):
        return self.RequestedStringValue


class SetAlertState(AbstractSet):
    NODETYPE = msg.SetAlertState
    action = Actions.SetAlertState
    ProposedAlertState = cp.ContainerProperty(msg.ProposedAlertState,
                                              value_class=AbstractAlertStateContainer,
                                              cls_getter=get_state_container_class,
                                              ns_helper=default_ns_helper)
    _props = ('ProposedAlertState',)

    @property
    def argument(self):
        return self.ProposedAlertState


class SetMetricState(AbstractSet):
    NODETYPE = msg.SetMetricState
    action = Actions.SetMetricState
    ProposedMetricState = cp.ContainerListProperty(msg.ProposedMetricState,
                                                   value_class=AbstractMetricStateContainer,
                                                   cls_getter=get_state_container_class,
                                                   ns_helper=default_ns_helper)
    _props = ('ProposedMetricState',)

    @property
    def argument(self):
        return self.ProposedMetricState


class Argument(PropertyBasedPMType):
    ArgValue = cp.NodeStringProperty(msg.ArgValue)
    _props = ('ArgValue',)


class Activate(AbstractSet):
    NODETYPE = msg.Activate
    action = Actions.Activate
    Argument = cp.SubElementListProperty(msg.Argument, value_class=Argument)
    _props = ('Argument',)

    def add_argument(self, arg_value):
        arg = Argument()
        arg.ArgValue = str(arg_value)
        self.Argument.append(arg)

    @property
    def argument(self):
        return self.Argument


class SetComponentState(AbstractSet):
    NODETYPE = msg.SetComponentState
    action = Actions.SetComponentState
    ProposedComponentState = cp.ContainerListProperty(msg.ProposedComponentState,
                                                      value_class=AbstractDeviceComponentStateContainer,
                                                      cls_getter=get_state_container_class,
                                                      ns_helper=default_ns_helper)
    _props = ('ProposedComponentState',)

    @property
    def argument(self):
        return self.ProposedComponentState
