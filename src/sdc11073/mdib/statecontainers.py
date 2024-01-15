from __future__ import annotations

import inspect
import sys
import time
import uuid
from typing import TYPE_CHECKING, Protocol

from sdc11073.xml_types import ext_qnames as ext
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import xml_structure as x_struct

from .containerbase import ContainerBase

if TYPE_CHECKING:
    from decimal import Decimal

    from lxml.etree import QName

    from sdc11073.location import SdcLocation
    from sdc11073.namespaces import NamespaceHelper
    from sdc11073.xml_types.isoduration import DurationType
    from sdc11073.xml_types.xml_structure import ExtensionLocalValue
    from sdc11073 import xml_utils

    from .descriptorcontainers import AbstractDescriptorProtocol


class AbstractStateProtocol(Protocol):
    """The common Interface of all states."""

    NODETYPE: QName
    is_state_container: bool
    is_realtime_sample_array_metric_state: bool
    is_metric_state: bool
    is_operational_state: bool
    is_component_state: bool
    is_alert_state: bool
    is_alert_signal: bool
    is_alert_condition: bool
    is_multi_state: bool
    is_context_state: bool

    DescriptorHandle: str
    DescriptorVersion: int
    StateVersion: int

    def __init__(self, descriptor_container: AbstractDescriptorProtocol | None):
        ...

    def update_from_other_container(self, other: AbstractStateProtocol,
                                    skipped_properties: list[str] | None = None):
        """Copy all properties except the skipped ones to self."""

    def update_from_node(self, node: xml_utils.LxmlElement):
        """Update members from node."""


class AbstractStateContainer(ContainerBase):
    """Base class of all states."""

    # these class variables allow easy type-checking. Derived classes will set corresponding values to True
    is_state_container = True
    is_realtime_sample_array_metric_state = False
    is_metric_state = False
    is_operational_state = False
    is_component_state = False
    is_alert_state = False
    is_alert_signal = False
    is_alert_condition = False
    is_multi_state = False
    is_context_state = False

    Extension: ExtensionLocalValue = x_struct.ExtensionNodeProperty(ext.Extension)
    DescriptorHandle: str = x_struct.HandleRefAttributeProperty('DescriptorHandle', is_optional=False)
    DescriptorVersion: int = x_struct.ReferencedVersionAttributeProperty('DescriptorVersion', implied_py_value=0)
    StateVersion: int = x_struct.VersionCounterAttributeProperty('StateVersion', implied_py_value=0)
    _props = ('Extension', 'DescriptorHandle', 'DescriptorVersion', 'StateVersion')

    def __init__(self, descriptor_container: AbstractDescriptorProtocol):
        super().__init__()
        self.descriptor_container = descriptor_container
        if descriptor_container is not None:
            # pylint: disable=invalid-name
            self.DescriptorHandle = descriptor_container.Handle
            self.DescriptorVersion = descriptor_container.DescriptorVersion
            # pylint: enable=invalid-name

    def mk_state_node(self, tag: QName,
                      nsmapper: NamespaceHelper,
                      set_xsi_type: bool = True) -> xml_utils.LxmlElement:
        """Create an etree node from instance data."""
        return super().mk_node(tag, nsmapper, set_xsi_type=set_xsi_type)

    def update_from_other_container(self, other: AbstractStateContainer,
                                    skipped_properties: list[str] | None = None):
        """Copy all properties except the skipped ones to self."""
        if other.DescriptorHandle != self.DescriptorHandle:
            raise ValueError(
                f'Update from a node with different descriptor handle is not possible! '
                f'Have "{self.DescriptorHandle}", got "{other.DescriptorHandle}"')
        self._update_from_other(other, skipped_properties)
        self.node = other.node

    def increment_state_version(self):
        """Add one."""
        self.StateVersion += 1

    def update_descriptor_version(self):
        """Set self.DescriptorVersion to version of descriptor."""
        if self.descriptor_container is None:
            raise ValueError(f'State {self} has no descriptor_container')
        if self.descriptor_container.DescriptorVersion != self.DescriptorVersion:
            self.DescriptorVersion = self.descriptor_container.DescriptorVersion

    @property
    def source_mds(self) -> str:
        """Get source mds handle."""
        return self.descriptor_container.source_mds

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" StateVersion={self.StateVersion}'

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement,
                  descriptor_container: AbstractDescriptorProtocol | None = None) -> AbstractStateContainer:
        """Create an instance from XML node."""
        obj = cls(descriptor_container)
        obj.update_from_node(node)
        return obj


class AbstractOperationStateContainer(AbstractStateContainer):
    """Represents AbstractOperationState in BICEPS."""

    NODETYPE = pm.AbstractOperationState
    is_operational_state = True
    OperatingMode: pm_types.OperatingMode = x_struct.EnumAttributeProperty('OperatingMode',
                                                                           default_py_value=pm_types.OperatingMode.ENABLED,
                                                                           enum_cls=pm_types.OperatingMode)
    _props = ('OperatingMode',)


class SetValueOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetValueOperationState in BICEPS."""

    NODETYPE = pm.SetValueOperationState
    AllowedRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm.AllowedRange, value_class=pm_types.Range)
    _props = ('AllowedRange',)


class AllowedValuesType(pm_types.PropertyBasedPMType):  # pylint: disable=invalid-name
    """Represents a list of values, in xml it is a list of pm.Value elements with one value as text."""

    Value: list[str] = x_struct.SubElementStringListProperty(pm.Value)
    _props = ('Value',)

    def is_empty(self) -> bool:
        """Return True if Value is empty."""
        return self.Value is None or len(self.Value) == 0


class SetStringOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetStringOperationState in BICEPS."""

    NODETYPE = pm.SetStringOperationState
    AllowedValues: AllowedValuesType = x_struct.SubElementWithSubElementListProperty(pm.AllowedValues,
                                                                                     default_py_value=AllowedValuesType(),
                                                                                     value_class=AllowedValuesType)
    _props = ('AllowedValues',)


class ActivateOperationStateContainer(AbstractOperationStateContainer):
    """Represents ActivateOperationState in BICEPS."""

    NODETYPE = pm.ActivateOperationState


class SetContextStateOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetContextStateOperationState in BICEPS."""

    NODETYPE = pm.SetContextStateOperationState


class SetMetricStateOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetMetricStateOperationState in BICEPS."""

    NODETYPE = pm.SetMetricStateOperationState


class SetComponentStateOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetComponentStateOperationState in BICEPS."""

    NODETYPE = pm.SetComponentStateOperationState


class SetAlertStateOperationStateContainer(AbstractOperationStateContainer):
    """Represents SetAlertStateOperationState in BICEPS."""

    NODETYPE = pm.SetAlertStateOperationState


class AbstractMetricStateContainer(AbstractStateContainer):
    """Represents AbstractMetricState in BICEPS."""

    is_metric_state = True
    BodySite: list[pm_types.CodedValue] = x_struct.SubElementListProperty(pm.BodySite, value_class=pm_types.CodedValue)
    PhysicalConnector: pm_types.PhysicalConnectorInfo = x_struct.SubElementProperty(pm.PhysicalConnector,
                                                                                    value_class=pm_types.PhysicalConnectorInfo,
                                                                                    is_optional=True)
    ActivationState: pm_types.ComponentActivation | None = x_struct.EnumAttributeProperty(
        'ActivationState', implied_py_value=pm_types.ComponentActivation.ON, enum_cls=pm_types.ComponentActivation)
    ActiveDeterminationPeriod: DurationType | None = x_struct.DurationAttributeProperty(
        'ActiveDeterminationPeriod')
    LifeTimePeriod: DurationType | None = x_struct.DurationAttributeProperty('LifeTimePeriod')
    _props = ('BodySite', 'PhysicalConnector', 'ActivationState', 'ActiveDeterminationPeriod', 'LifeTimePeriod')


class MetricStateProtocol(AbstractStateProtocol):
    MetricValue: None | pm_types.NumericMetricValue | pm_types.StringMetricValue | pm_types.SampleArrayValue

    def mk_metric_value(self) -> pm_types.NumericMetricValue | pm_types.StringMetricValue | pm_types.SampleArrayValue:
        ...


class NumericMetricStateContainer(AbstractMetricStateContainer):
    """Represents NumericMetricState in BICEPS."""

    NODETYPE = pm.NumericMetricState
    MetricValue: pm_types.NumericMetricValue | None = x_struct.SubElementProperty(pm.MetricValue,
                                                                                  value_class=pm_types.NumericMetricValue,
                                                                                  is_optional=True)
    PhysiologicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm.PhysiologicalRange,
                                                                               value_class=pm_types.Range)
    ActiveAveragingPeriod: DurationType | None = x_struct.DurationAttributeProperty('ActiveAveragingPeriod')
    _props = ('MetricValue', 'PhysiologicalRange', 'ActiveAveragingPeriod')

    def mk_metric_value(self) -> pm_types.NumericMetricValue:
        """Instantiate self.MetricValue."""
        if self.MetricValue is None:
            self.MetricValue = pm_types.NumericMetricValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')


class StringMetricStateContainer(AbstractMetricStateContainer):
    """Represents StringMetricState in BICEPS."""

    NODETYPE = pm.StringMetricState
    MetricValue: pm_types.StringMetricValue | None = x_struct.SubElementProperty(pm.MetricValue,
                                                                                 value_class=pm_types.StringMetricValue,
                                                                                 is_optional=True)
    _props = ('MetricValue',)

    def mk_metric_value(self) -> pm_types.StringMetricValue:
        """Instantiate self.MetricValue."""
        if self.MetricValue is None:
            self.MetricValue = pm_types.StringMetricValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')


class EnumStringMetricStateContainer(StringMetricStateContainer):
    """Represents EnumStringMetricState in BICEPS."""

    NODETYPE = pm.EnumStringMetricState


class RealTimeSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    """Represents RealTimeSampleArrayMetricState in BICEPS."""

    NODETYPE = pm.RealTimeSampleArrayMetricState
    is_realtime_sample_array_metric_state = True
    MetricValue: pm_types.SampleArrayValue | None = x_struct.SubElementProperty(pm.MetricValue,
                                                                                value_class=pm_types.SampleArrayValue,
                                                                                is_optional=True)
    PhysiologicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm.PhysiologicalRange,
                                                                               value_class=pm_types.Range)
    _props = ('MetricValue', 'PhysiologicalRange')

    def mk_metric_value(self) -> pm_types.SampleArrayValue:
        """Instantiate self.MetricValue."""
        if self.MetricValue is None:
            self.MetricValue = pm_types.SampleArrayValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')

    def __repr__(self) -> str:
        samples_count = 0
        if self.MetricValue is not None and self.MetricValue.Samples is not None:
            samples_count = len(self.MetricValue.Samples)
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'Activation="{self.ActivationState}" Samples={samples_count}')


class DistributionSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    """Represents DistributionSampleArrayMetricState in BICEPS."""

    NODETYPE = pm.DistributionSampleArrayMetricState
    MetricValue = x_struct.SubElementProperty(pm.MetricValue, value_class=pm_types.SampleArrayValue, is_optional=True)
    PhysiologicalRange: list[pm_types.Range] = x_struct.SubElementListProperty(pm.PhysiologicalRange,
                                                                               value_class=pm_types.Range)
    _props = ('MetricValue', 'PhysiologicalRange')

    def mk_metric_value(self) -> pm_types.SampleArrayValue:
        """Instantiate self.MetricValue."""
        if self.MetricValue is None:
            self.MetricValue = pm_types.SampleArrayValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')

    def __repr__(self) -> str:
        samples_count = 0
        if self.MetricValue is not None and self.MetricValue.Samples is not None:
            samples_count = len(self.MetricValue.Samples)
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'Activation="{self.ActivationState}" Samples={samples_count}')


class AbstractDeviceComponentStateContainer(AbstractStateContainer):
    """Represents AbstractDeviceComponentState in BICEPS."""

    is_component_state = True
    CalibrationInfo: pm_types.CalibrationInfo | None = x_struct.SubElementProperty(pm.CalibrationInfo,
                                                                                   value_class=pm_types.CalibrationInfo,
                                                                                   is_optional=True)
    NextCalibration: pm_types.CalibrationInfo | None = x_struct.SubElementProperty(pm.NextCalibration,
                                                                                   value_class=pm_types.CalibrationInfo,
                                                                                   is_optional=True)
    PhysicalConnector: pm_types.PhysicalConnectorInfo | None = x_struct.SubElementProperty(pm.PhysicalConnector,
                                                                                           value_class=pm_types.PhysicalConnectorInfo,
                                                                                           is_optional=True)

    ActivationState: pm_types.ComponentActivation = x_struct.EnumAttributeProperty('ActivationState',
                                                                                   enum_cls=pm_types.ComponentActivation,
                                                                                   implied_py_value=pm_types.ComponentActivation.ON)
    OperatingHours: int | None = x_struct.UnsignedIntAttributeProperty('OperatingHours')
    OperatingCycles: int | None = x_struct.UnsignedIntAttributeProperty('OperatingCycles')
    _props = (
        'CalibrationInfo', 'NextCalibration', 'PhysicalConnector', 'ActivationState', 'OperatingHours',
        'OperatingCycles')


class AbstractComplexDeviceComponentStateContainer(AbstractDeviceComponentStateContainer):
    """Represents AbstractComplexDeviceComponentState in BICEPS."""

    NODETYPE = pm.AbstractComplexDeviceComponentState


class MdsStateContainer(AbstractComplexDeviceComponentStateContainer):
    """Represents MdsState in BICEPS."""

    NODETYPE = pm.MdsState
    OperatingJurisdiction: pm_types.OperatingJurisdiction | None = x_struct.SubElementProperty(pm.OperatingJurisdiction,
                                                                                               value_class=pm_types.OperatingJurisdiction,
                                                                                               is_optional=True)
    Lang: str = x_struct.StringAttributeProperty('Lang', implied_py_value='en')
    OperatingMode: pm_types.MdsOperatingMode = x_struct.EnumAttributeProperty('OperatingMode',
                                                                              implied_py_value=pm_types.MdsOperatingMode.NORMAL,
                                                                              enum_cls=pm_types.MdsOperatingMode)
    _props = ('OperatingJurisdiction', 'OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    """Represents ScoState in BICEPS."""

    NODETYPE = pm.ScoState
    OperationGroup: list[pm_types.OperationGroup] = x_struct.SubElementListProperty(pm.OperationGroup,
                                                                                    value_class=pm_types.OperationGroup)
    InvocationRequested: list[str] | None = x_struct.OperationRefListAttributeProperty('InvocationRequested')
    InvocationRequired: list[str] | None = x_struct.OperationRefListAttributeProperty('InvocationRequired')
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractComplexDeviceComponentStateContainer):
    """Represents VmdState in BICEPS."""

    NODETYPE = pm.VmdState
    OperatingJurisdiction: pm_types.OperatingJurisdiction = x_struct.SubElementProperty(pm.OperatingJurisdiction,
                                                                                        value_class=pm_types.OperatingJurisdiction,
                                                                                        is_optional=True)
    _props = ('OperatingJurisdiction',)


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    """Represents ChannelState in BICEPS."""

    NODETYPE = pm.ChannelState


class ClockStateContainer(AbstractDeviceComponentStateContainer):
    """Represents ClockState in BICEPS."""

    NODETYPE = pm.ClockState
    ActiveSyncProtocol: pm_types.CodedValue | None = x_struct.SubElementProperty(pm.ActiveSyncProtocol,
                                                                                 value_class=pm_types.CodedValue,
                                                                                 is_optional=True)
    ReferenceSource: list[str] = x_struct.SubElementStringListProperty(pm.ReferenceSource)
    DateAndTime: float = x_struct.CurrentTimestampAttributeProperty('DateAndTime')
    RemoteSync: bool = x_struct.BooleanAttributeProperty('RemoteSync', default_py_value=True, is_optional=False)
    Accuracy: Decimal | None = x_struct.DecimalAttributeProperty('Accuracy')
    LastSet: float | None = x_struct.TimestampAttributeProperty('LastSet')
    TimeZone: str | None = x_struct.TimeZoneAttributeProperty('TimeZone')
    CriticalUse: bool = x_struct.BooleanAttributeProperty('CriticalUse', implied_py_value=False)
    _props = ('ActiveSyncProtocol', 'ReferenceSource', 'DateAndTime', 'RemoteSync', 'Accuracy', 'LastSet', 'TimeZone',
              'CriticalUse')


class SystemContextStateContainer(AbstractDeviceComponentStateContainer):
    """Represents SystemContextState in BICEPS."""

    NODETYPE = pm.SystemContextState


class BatteryStateContainer(AbstractDeviceComponentStateContainer):
    """Represents BatteryState in BICEPS."""

    class ChargeStatusEnum(pm_types.StringEnum):
        """ChargeStatusEnum contains the allowed values for ChargeStatus."""

        FULL = 'Ful'
        CHARGING = 'ChB'
        DISCHARGING = 'DisChB'
        EMPTY = 'DEB'

    NODETYPE = pm.BatteryState
    CapacityRemaining: pm_types.Measurement | None = x_struct.SubElementProperty(pm.CapacityRemaining,
                                                                                 value_class=pm_types.Measurement,
                                                                                 is_optional=True)
    Voltage: pm_types.Measurement | None = x_struct.SubElementProperty(pm.Voltage,
                                                                       value_class=pm_types.Measurement,
                                                                       is_optional=True)
    Current: pm_types.Measurement | None = x_struct.SubElementProperty(pm.Current,
                                                                       value_class=pm_types.Measurement,
                                                                       is_optional=True)
    Temperature: pm_types.Measurement | None = x_struct.SubElementProperty(pm.Temperature,
                                                                           value_class=pm_types.Measurement,
                                                                           is_optional=True)
    RemainingBatteryTime: pm_types.Measurement | None = x_struct.SubElementProperty(pm.RemainingBatteryTime,
                                                                                    value_class=pm_types.Measurement,
                                                                                    is_optional=True)
    ChargeStatus: ChargeStatusEnum | None = x_struct.EnumAttributeProperty('ChargeStatus', enum_cls=ChargeStatusEnum)
    ChargeCycles: int | None = x_struct.UnsignedIntAttributeProperty(
        'ChargeCycles')  # Number of charge/discharge cycles.
    _props = (
        'CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus',
        'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    """Represents AbstractAlertState in BICEPS."""

    is_alert_state = True
    ActivationState: pm_types.AlertActivation = x_struct.EnumAttributeProperty('ActivationState',
                                                                               default_py_value=pm_types.AlertActivation.ON,
                                                                               enum_cls=pm_types.AlertActivation,
                                                                               is_optional=False)
    _props = ('ActivationState',)


class AlertSystemStateContainer(AbstractAlertStateContainer):
    """Represents AlertSystemState in BICEPS."""

    NODETYPE = pm.AlertSystemState
    SystemSignalActivation: list[pm_types.SystemSignalActivation] = x_struct.SubElementListProperty(
        pm.SystemSignalActivation,
        value_class=pm_types.SystemSignalActivation)
    LastSelfCheck: float | None = x_struct.TimestampAttributeProperty('LastSelfCheck')
    SelfCheckCount: int | None = x_struct.IntegerAttributeProperty('SelfCheckCount')
    PresentPhysiologicalAlarmConditions: list[str] | None = x_struct.AlertConditionRefListAttributeProperty(
        'PresentPhysiologicalAlarmConditions')
    PresentTechnicalAlarmConditions: list[str] | None = x_struct.AlertConditionRefListAttributeProperty(
        'PresentTechnicalAlarmConditions')
    _props = ('SystemSignalActivation', 'LastSelfCheck', 'SelfCheckCount', 'PresentPhysiologicalAlarmConditions',
              'PresentTechnicalAlarmConditions')

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'StateVersion={self.StateVersion} LastSelfCheck={self.LastSelfCheck} '
                f'SelfCheckCount={self.SelfCheckCount} Activation={self.ActivationState}')


class AlertSignalStateContainer(AbstractAlertStateContainer):
    """Represents AlertSignalState in BICEPS."""

    is_alert_signal = True
    NODETYPE = pm.AlertSignalState
    ActualSignalGenerationDelay: DurationType | None = x_struct.DurationAttributeProperty(
        'ActualSignalGenerationDelay')
    Presence: pm_types.AlertSignalPresence = x_struct.EnumAttributeProperty('Presence',
                                                                            implied_py_value=pm_types.AlertSignalPresence.OFF,
                                                                            enum_cls=pm_types.AlertSignalPresence)
    Location: pm_types.AlertSignalPrimaryLocation = x_struct.EnumAttributeProperty('Location',
                                                                                   implied_py_value=pm_types.AlertSignalPrimaryLocation.LOCAL,
                                                                                   enum_cls=pm_types.AlertSignalPrimaryLocation)
    Slot: int | None = x_struct.UnsignedIntAttributeProperty('Slot')
    _props = ('ActualSignalGenerationDelay', 'Presence', 'Location', 'Slot')

    def __init__(self, descriptor_container: AbstractDescriptorProtocol):
        super().__init__(descriptor_container)
        self.last_updated = time.time()

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'StateVersion={self.StateVersion} Location={self.Location} '
                f'Activation={self.ActivationState} Presence={self.Presence}')


class AlertConditionStateContainer(AbstractAlertStateContainer):
    """Represents AlertConditionState in BICEPS."""

    is_alert_condition = True
    NODETYPE = pm.AlertConditionState
    ActualConditionGenerationDelay: DurationType | None = x_struct.DurationAttributeProperty(
        'ActualConditionGenerationDelay')
    ActualPriority: pm_types.AlertConditionPriority | None = x_struct.EnumAttributeProperty('ActualPriority',
                                                                                            enum_cls=pm_types.AlertConditionPriority)
    Rank: int | None = x_struct.IntegerAttributeProperty('Rank')
    DeterminationTime: float | None = x_struct.TimestampAttributeProperty('DeterminationTime')
    Presence: bool = x_struct.BooleanAttributeProperty('Presence', implied_py_value=False)
    _props = ('ActualConditionGenerationDelay', 'ActualPriority', 'Rank', 'DeterminationTime', 'Presence')

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'StateVersion={self.StateVersion} '
                f'Activation={self.ActivationState} Presence={self.Presence}')


class LimitAlertConditionStateContainer(AlertConditionStateContainer):
    """Represents LimitAlertConditionState in BICEPS."""

    NODETYPE = pm.LimitAlertConditionState
    Limits: pm_types.Range = x_struct.SubElementProperty(pm.Limits,
                                                         value_class=pm_types.Range,
                                                         default_py_value=pm_types.Range())
    MonitoredAlertLimits: pm_types.AlertConditionMonitoredLimits = x_struct.EnumAttributeProperty(
        'MonitoredAlertLimits',
        default_py_value=pm_types.AlertConditionMonitoredLimits.NONE,
        enum_cls=pm_types.AlertConditionMonitoredLimits,
        is_optional=False)
    AutoLimitActivationState: pm_types.AlertActivation | None = x_struct.EnumAttributeProperty(
        'AutoLimitActivationState',
        enum_cls=pm_types.AlertActivation)
    _props = ('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateProtocol(AbstractStateProtocol):
    """Protocol for AbstractContextStateContainer."""

    Handle: str


class AbstractMultiStateContainer(AbstractStateContainer):
    """Represents AbstractMultiState in BICEPS."""

    is_multi_state = True
    Category: pm_types.CodedValue | None = x_struct.SubElementProperty(pm.Category,
                                                                       value_class=pm_types.CodedValue,
                                                                       is_optional=True)
    Handle: str = x_struct.HandleAttributeProperty('Handle', is_optional=False)
    _props = ('Category', 'Handle')

    def __init__(self, descriptor_container: AbstractDescriptorProtocol, handle: str | None = None):
        super().__init__(descriptor_container)
        self.Handle = handle  # pylint: disable=invalid-name

    def update_from_other_container(self, other: AbstractMultiStateContainer, skipped_properties: list[str] = None):
        """Copy all properties except the skipped ones to self.

        Accept node only if DescriptorHandle and Handle match.
        """
        if self.Handle is not None and other.Handle != self.Handle:
            raise ValueError(
                f'Update from a node with different handle is not possible! Have "{self.Handle}", got "{other.Handle}"')
        super().update_from_other_container(other, skipped_properties)

    def mk_state_node(self, tag: QName, nsmapper: NamespaceHelper, set_xsi_type: bool = True) -> xml_utils.LxmlElement:
        """Create an etree node from instance data."""
        if self.Handle is None:
            self.Handle = uuid.uuid4().hex
        return super().mk_state_node(tag, nsmapper, set_xsi_type)

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" '
                f'Handle="{self.Handle}" type={self.NODETYPE}')


class AbstractContextStateContainer(AbstractMultiStateContainer):
    """Represents AbstractContextState in BICEPS."""

    is_context_state = True
    Validator: list[pm_types.InstanceIdentifier] = x_struct.SubElementListProperty(pm.Validator,
                                                                                   value_class=pm_types.InstanceIdentifier)
    Identification: list[pm_types.InstanceIdentifier] = x_struct.SubElementListProperty(pm.Identification,
                                                                                        value_class=pm_types.InstanceIdentifier)
    ContextAssociation: pm_types.ContextAssociation = x_struct.EnumAttributeProperty('ContextAssociation',
                                                                                     enum_cls=pm_types.ContextAssociation,
                                                                                     implied_py_value=pm_types.ContextAssociation.NO_ASSOCIATION)
    BindingMdibVersion: int | None = x_struct.ReferencedVersionAttributeProperty('BindingMdibVersion')
    UnbindingMdibVersion: int | None = x_struct.ReferencedVersionAttributeProperty('UnbindingMdibVersion')
    BindingStartTime: float | None = x_struct.TimestampAttributeProperty('BindingStartTime')
    BindingEndTime: float | None = x_struct.TimestampAttributeProperty('BindingEndTime')
    _props = ('Validator', 'Identification', 'ContextAssociation', 'BindingMdibVersion', 'UnbindingMdibVersion',
              'BindingStartTime', 'BindingEndTime')


class LocationContextStateContainer(AbstractContextStateContainer):
    """Represents LocationContextState in BICEPS."""

    NODETYPE = pm.LocationContextState
    # Technically LocationDetail does not need a default_py_value because it is optional,
    # but it makes it much easier for users if LocationDetail always exists.
    # The consequence is a difference in generated xml: The LocationDetail node will always exist, even if nothing was set.
    # To circumvent this you can set LocationDetail to None.
    LocationDetail: pm_types.LocationDetail = x_struct.SubElementProperty(pm.LocationDetail,
                                                                          value_class=pm_types.LocationDetail,
                                                                          default_py_value=pm_types.LocationDetail(),
                                                                          is_optional=True)
    _props = ('LocationDetail',)

    def update_from_sdc_location(self, sdc_location: SdcLocation):
        """Set members according to sdc_location."""
        if self.LocationDetail is None:
            self.LocationDetail = pm_types.LocationDetail()
        self.LocationDetail.PoC = sdc_location.poc
        self.LocationDetail.Room = sdc_location.rm
        self.LocationDetail.Bed = sdc_location.bed
        self.LocationDetail.Facility = sdc_location.fac
        self.LocationDetail.Building = sdc_location.bldng
        self.LocationDetail.Floor = sdc_location.flr
        self.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED

        extension_string = self._mk_extension_string(sdc_location)
        self.Identification = [pm_types.InstanceIdentifier(root=sdc_location.root, extension_string=extension_string)]

    def _mk_extension_string(self, sdc_location: SdcLocation) -> str:
        """Return a string with all location elements separated by /-char.

        None elements are replaces by an empty string.
        """
        return '/'.join([getattr(sdc_location, attr) or '' for attr in sdc_location.url_elements])

    @classmethod
    def from_sdc_location(cls, descriptor_container: AbstractDescriptorProtocol,
                          handle: str,
                          sdc_location: SdcLocation) -> LocationContextStateContainer:
        """Construct LocationContextStateContainer from a sdc location."""
        obj = cls(descriptor_container)
        obj.Handle = handle
        obj.update_from_sdc_location(sdc_location)
        return obj


class PatientContextStateContainer(AbstractContextStateContainer):
    """Represents PatientContextState in BICEPS."""

    NODETYPE = pm.PatientContextState
    # Technically CoreData does not need a default_py_value because it is optional,
    # but it makes it much easier for users if CoreData always exists.
    # The consequence is a difference in generated xml: The CoreData node will always exist, even if nothing was set.
    # To circumvent this you can set CoreData to None.
    CoreData: pm_types.PatientDemographicsCoreData | None = x_struct.SubElementProperty(pm.CoreData,
                                                                                        value_class=pm_types.PatientDemographicsCoreData,
                                                                                        default_py_value=pm_types.PatientDemographicsCoreData(),
                                                                                        is_optional=True)
    _props = ('CoreData',)


class WorkflowContextStateContainer(AbstractContextStateContainer):
    """Represents WorkflowContextState in BICEPS."""

    NODETYPE = pm.WorkflowContextState
    WorkflowDetail: pm_types.WorkflowDetail | None = x_struct.SubElementProperty(pm.WorkflowDetail,
                                                                                 value_class=pm_types.WorkflowDetail,
                                                                                 is_optional=True)
    _props = ('WorkflowDetail',)


class OperatorContextStateContainer(AbstractContextStateContainer):
    """Represents OperatorContextState in BICEPS."""

    NODETYPE = pm.OperatorContextState
    OperatorDetails: pm_types.BaseDemographics | None = x_struct.SubElementProperty(pm.OperatorDetails,
                                                                                    value_class=pm_types.BaseDemographics,
                                                                                    is_optional=True)
    _props = ('OperatorDetails',)


class MeansContextStateContainer(AbstractContextStateContainer):
    """Represents MeansContextState in BICEPS."""

    NODETYPE = pm.MeansContextState
    # class has no own members


class EnsembleContextStateContainer(AbstractContextStateContainer):
    """Represents EnsembleContextState in BICEPS."""

    NODETYPE = pm.EnsembleContextState
    # class has no own members


# mapping of states: xsi:type information to classes
# find all classes in this module that have a member "NODETYPE"
classes = inspect.getmembers(sys.modules[__name__],
                             lambda member: inspect.isclass(member) and member.__module__ == __name__)
classes_with_nodetype = [c[1] for c in classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself
_state_lookup_by_type = {c.NODETYPE: c for c in classes_with_nodetype}


def get_container_class(type_qname: QName) -> type[AbstractStateProtocol]:
    """Return class for given type.

    :param type_qname: the QName of the expected NODETYPE.
    """
    return _state_lookup_by_type.get(type_qname)
