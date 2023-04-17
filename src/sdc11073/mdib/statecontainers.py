import inspect
import sys
import time
import uuid

from .containerbase import ContainerBase
from ..xml_types import pm_types
from ..xml_types import xml_structure as cp
from ..xml_types import pm_qnames as pm
from ..xml_types import ext_qnames as ext


class AbstractStateContainer(ContainerBase):
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

    Extension = cp.ExtensionNodeProperty(ext.Extension)
    DescriptorHandle = cp.HandleRefAttributeProperty('DescriptorHandle', is_optional=False)
    DescriptorVersion = cp.ReferencedVersionAttributeProperty('DescriptorVersion', default_py_value=0)
    StateVersion = cp.VersionCounterAttributeProperty('StateVersion', default_py_value=0)
    _props = ('Extension', 'DescriptorHandle', 'DescriptorVersion', 'StateVersion')

    def __init__(self, descriptor_container):
        super().__init__()
        self.descriptor_container = descriptor_container
        if descriptor_container is not None:
            # pylint: disable=invalid-name
            self.DescriptorHandle = descriptor_container.Handle
            self.DescriptorVersion = descriptor_container.DescriptorVersion
            # pylint: enable=invalid-name

    def mk_state_node(self, tag, nsmapper, set_xsi_type=True):
        return super().mk_node(tag, nsmapper, set_xsi_type=set_xsi_type)

    def update_from_other_container(self, other, skipped_properties=None):
        if other.DescriptorHandle != self.DescriptorHandle:
            raise ValueError(
                f'Update from a node with different descriptor handle is not possible! '
                f'Have "{self.DescriptorHandle}", got "{other.DescriptorHandle}"')
        self._update_from_other(other, skipped_properties)
        self.node = other.node

    def increment_state_version(self):
        # pylint: disable=invalid-name
        if self.StateVersion is None:
            self.StateVersion = 1
        else:
            self.StateVersion += 1
        # pylint: enable=invalid-name

    def update_descriptor_version(self):
        if self.descriptor_container is None:
            raise ValueError(f'State {self} has no descriptor_container')
        if self.descriptor_container.DescriptorVersion != self.DescriptorVersion:
            self.DescriptorVersion = self.descriptor_container.DescriptorVersion

    @property
    def source_mds(self):
        return self.descriptor_container.source_mds

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" StateVersion={self.StateVersion}'

    @classmethod
    def from_node(cls, node, descriptor_container=None):
        obj = cls(descriptor_container)
        obj.update_from_node(node)
        return obj


class AbstractOperationStateContainer(AbstractStateContainer):
    NODETYPE = pm.AbstractOperationState
    is_operational_state = True
    OperatingMode = cp.EnumAttributeProperty('OperatingMode', default_py_value=pm_types.OperatingMode.ENABLED,
                                             enum_cls=pm_types.OperatingMode)
    _props = ('OperatingMode',)


class SetValueOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetValueOperationState
    AllowedRange = cp.SubElementListProperty(pm.AllowedRange, value_class=pm_types.Range)
    _props = ('AllowedRange',)


class T_AllowedValues(pm_types.PropertyBasedPMType):  # pylint: disable=invalid-name
    Value = cp.SubElementStringListProperty(pm.Value)
    _props = ['Value']

    def is_empty(self):
        return self.Value is None or len(self.Value) == 0


class SetStringOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetStringOperationState
    AllowedValues = cp.SubElementWithSubElementListProperty(pm.AllowedValues,
                                                            default_py_value=T_AllowedValues(),
                                                            value_class=T_AllowedValues)
    _props = ('AllowedValues',)


class ActivateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.ActivateOperationState


class SetContextStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetContextStateOperationState


class SetMetricStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetMetricStateOperationState


class SetComponentStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetComponentStateOperationState


class SetAlertStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetAlertStateOperationState


class AbstractMetricStateContainer(AbstractStateContainer):
    is_metric_state = True
    BodySite = cp.SubElementListProperty(pm.BodySite, value_class=pm_types.CodedValue)
    PhysicalConnector = cp.SubElementProperty(pm.PhysicalConnector,
                                              value_class=pm_types.PhysicalConnectorInfo, is_optional=True)
    ActivationState = cp.EnumAttributeProperty('ActivationState', implied_py_value=pm_types.ComponentActivation.ON,
                                               enum_cls=pm_types.ComponentActivation)
    ActiveDeterminationPeriod = cp.DurationAttributeProperty('ActiveDeterminationPeriod')  # xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod')  # xsd:duration, optional
    _props = ('BodySite', 'PhysicalConnector', 'ActivationState', 'ActiveDeterminationPeriod', 'LifeTimePeriod')


class NumericMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.NumericMetricState
    MetricValue = cp.SubElementProperty(pm.MetricValue, value_class=pm_types.NumericMetricValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pm_types.Range)
    ActiveAveragingPeriod = cp.DurationAttributeProperty('ActiveAveragingPeriod')  # xsd:duration
    _props = ('MetricValue', 'PhysiologicalRange', 'ActiveAveragingPeriod')

    def mk_metric_value(self):
        if self.MetricValue is None:
            self.MetricValue = pm_types.NumericMetricValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')


class StringMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.StringMetricState
    MetricValue = cp.SubElementProperty(pm.MetricValue, value_class=pm_types.StringMetricValue, is_optional=True)
    _props = ('MetricValue',)

    def mk_metric_value(self):
        if self.MetricValue is None:
            self.MetricValue = pm_types.StringMetricValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')


class EnumStringMetricStateContainer(StringMetricStateContainer):
    NODETYPE = pm.EnumStringMetricState


class RealTimeSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.RealTimeSampleArrayMetricState
    is_realtime_sample_array_metric_state = True
    MetricValue = cp.SubElementProperty(pm.MetricValue, value_class=pm_types.SampleArrayValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pm_types.Range)
    _props = ('MetricValue', 'PhysiologicalRange')

    def mk_metric_value(self):
        if self.MetricValue is None:
            self.MetricValue = pm_types.SampleArrayValue()
            return self.MetricValue
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')

    def __repr__(self):
        samples_count = 0
        if self.MetricValue is not None and self.MetricValue.Samples is not None:
            samples_count = len(self.MetricValue.Samples)
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" ' \
               f'Activation="{self.ActivationState}" Samples={samples_count}'


class DistributionSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.DistributionSampleArrayMetricState
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pm_types.SampleArrayValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pm_types.Range)
    _props = ('_metric_value', 'PhysiologicalRange')


class AbstractDeviceComponentStateContainer(AbstractStateContainer):
    is_component_state = True
    CalibrationInfo = cp.SubElementProperty(pm.CalibrationInfo,
                                            value_class=pm_types.CalibrationInfo,
                                            is_optional=True)
    NextCalibration = cp.SubElementProperty(pm.NextCalibration,
                                            value_class=pm_types.CalibrationInfo,
                                            is_optional=True)
    PhysicalConnector = cp.SubElementProperty(pm.PhysicalConnector,
                                              value_class=pm_types.PhysicalConnectorInfo,
                                              is_optional=True)

    ActivationState = cp.EnumAttributeProperty('ActivationState', enum_cls=pm_types.ComponentActivation)
    OperatingHours = cp.UnsignedIntAttributeProperty('OperatingHours')  # optional
    OperatingCycles = cp.UnsignedIntAttributeProperty('OperatingCycles')  # optional
    _props = (
        'CalibrationInfo', 'NextCalibration', 'PhysicalConnector', 'ActivationState', 'OperatingHours',
        'OperatingCycles')


class AbstractComplexDeviceComponentStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.AbstractComplexDeviceComponentState


class MdsStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = pm.MdsState
    OperatingJurisdiction = cp.SubElementProperty(pm.OperatingJurisdiction,
                                                  value_class=pm_types.OperatingJurisdiction,
                                                  is_optional=True)
    OperatingMode = cp.EnumAttributeProperty('OperatingMode',
                                             default_py_value=pm_types.MdsOperatingMode.NORMAL,
                                             enum_cls=pm_types.MdsOperatingMode)
    Lang = cp.StringAttributeProperty('Lang', default_py_value='en')
    _props = ('OperatingJurisdiction', 'OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ScoState
    OperationGroup = cp.SubElementListProperty(pm.OperationGroup, value_class=pm_types.OperationGroup)
    InvocationRequested = cp.OperationRefListAttributeProperty('InvocationRequested')
    InvocationRequired = cp.OperationRefListAttributeProperty('InvocationRequired')
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = pm.VmdState
    OperatingJurisdiction = cp.SubElementProperty(pm.OperatingJurisdiction,
                                                  value_class=pm_types.OperatingJurisdiction,
                                                  is_optional=True)
    _props = ('OperatingJurisdiction',)


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ChannelState



class ClockStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ClockState
    ActiveSyncProtocol = cp.SubElementProperty(pm.ActiveSyncProtocol, value_class=pm_types.CodedValue, is_optional=True)
    ReferenceSource = cp.SubElementStringListProperty(pm.ReferenceSource)
    DateAndTime = cp.CurrentTimestampAttributeProperty('DateAndTime')
    RemoteSync = cp.BooleanAttributeProperty('RemoteSync', default_py_value=True, is_optional=False)
    Accuracy = cp.DecimalAttributeProperty('Accuracy')
    LastSet = cp.TimestampAttributeProperty('LastSet')
    TimeZone = cp.TimeZoneAttributeProperty('TimeZone')  # a time zone string
    CriticalUse = cp.BooleanAttributeProperty('CriticalUse', implied_py_value=False)  # optional
    _props = ('ActiveSyncProtocol', 'ReferenceSource', 'DateAndTime', 'RemoteSync', 'Accuracy', 'LastSet', 'TimeZone',
              'CriticalUse')


class SystemContextStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.SystemContextState


class BatteryStateContainer(AbstractDeviceComponentStateContainer):
    class ChargeStatusEnum(pm_types.StringEnum):
        FULL = 'Ful'
        CHARGING = 'ChB'
        DISCHARGING = 'DisChB'
        EMPTY = 'DEB'

    NODETYPE = pm.BatteryState
    CapacityRemaining = cp.SubElementProperty(pm.CapacityRemaining,
                                              value_class=pm_types.Measurement,
                                              is_optional=True)
    Voltage = cp.SubElementProperty(pm.Voltage, value_class=pm_types.Measurement, is_optional=True)
    Current = cp.SubElementProperty(pm.Current, value_class=pm_types.Measurement, is_optional=True)
    Temperature = cp.SubElementProperty(pm.Temperature, value_class=pm_types.Measurement, is_optional=True)
    RemainingBatteryTime = cp.SubElementProperty(pm.RemainingBatteryTime,
                                                 value_class=pm_types.Measurement,
                                                 is_optional=True)
    ChargeStatus = cp.EnumAttributeProperty('ChargeStatus', enum_cls=ChargeStatusEnum)
    ChargeCycles = cp.UnsignedIntAttributeProperty('ChargeCycles')  # Number of charge/discharge cycles.
    _props = (
        'CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus',
        'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    is_alert_state = True
    ActivationState = cp.EnumAttributeProperty('ActivationState',
                                               default_py_value=pm_types.AlertActivation.ON,
                                               enum_cls=pm_types.AlertActivation,
                                               is_optional=False)
    _props = ('ActivationState',)


class AlertSystemStateContainer(AbstractAlertStateContainer):
    NODETYPE = pm.AlertSystemState
    SystemSignalActivation = cp.SubElementListProperty(pm.SystemSignalActivation,
                                                       value_class=pm_types.SystemSignalActivation)
    LastSelfCheck = cp.TimestampAttributeProperty('LastSelfCheck')
    SelfCheckCount = cp.IntegerAttributeProperty('SelfCheckCount')
    PresentPhysiologicalAlarmConditions = cp.AlertConditionRefListAttributeProperty(
        'PresentPhysiologicalAlarmConditions')
    PresentTechnicalAlarmConditions = cp.AlertConditionRefListAttributeProperty('PresentTechnicalAlarmConditions')
    _props = ('SystemSignalActivation', 'LastSelfCheck', 'SelfCheckCount', 'PresentPhysiologicalAlarmConditions',
              'PresentTechnicalAlarmConditions')

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" ' \
               f'StateVersion={self.StateVersion} LastSelfCheck={self.LastSelfCheck} ' \
               f'SelfCheckCount={self.SelfCheckCount} Activation={self.ActivationState}'


class AlertSignalStateContainer(AbstractAlertStateContainer):
    is_alert_signal = True
    NODETYPE = pm.AlertSignalState
    ActualSignalGenerationDelay = cp.DurationAttributeProperty('ActualSignalGenerationDelay')
    Presence = cp.EnumAttributeProperty('Presence', implied_py_value=pm_types.AlertSignalPresence.OFF,
                                        enum_cls=pm_types.AlertSignalPresence)
    Location = cp.EnumAttributeProperty('Location', implied_py_value=pm_types.AlertSignalPrimaryLocation.LOCAL,
                                        enum_cls=pm_types.AlertSignalPrimaryLocation)
    Slot = cp.UnsignedIntAttributeProperty('Slot')
    _props = ('ActualSignalGenerationDelay', 'Presence', 'Location', 'Slot')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_updated = time.time()

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" ' \
               f'StateVersion={self.StateVersion} Location={self.Location} ' \
               f'Activation={self.ActivationState} Presence={self.Presence}'


class AlertConditionStateContainer(AbstractAlertStateContainer):
    is_alert_condition = True
    NODETYPE = pm.AlertConditionState
    ActualConditionGenerationDelay = cp.DurationAttributeProperty('ActualConditionGenerationDelay')
    ActualPriority = cp.EnumAttributeProperty('ActualPriority', enum_cls=pm_types.AlertConditionPriority)
    Rank = cp.IntegerAttributeProperty('Rank')
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime')
    Presence = cp.BooleanAttributeProperty('Presence', implied_py_value=False)
    _props = ('ActualConditionGenerationDelay', 'ActualPriority', 'Rank', 'DeterminationTime', 'Presence')

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" ' \
               f'StateVersion={self.StateVersion} ' \
               f'Activation={self.ActivationState} Presence={self.Presence}'


class LimitAlertConditionStateContainer(AlertConditionStateContainer):
    NODETYPE = pm.LimitAlertConditionState
    Limits = cp.SubElementProperty(pm.Limits, value_class=pm_types.Range, default_py_value=pm_types.Range())
    MonitoredAlertLimits = cp.EnumAttributeProperty('MonitoredAlertLimits',
                                                    default_py_value=pm_types.AlertConditionMonitoredLimits.NONE,
                                                    enum_cls=pm_types.AlertConditionMonitoredLimits,
                                                    is_optional=False)
    AutoLimitActivationState = cp.EnumAttributeProperty('AutoLimitActivationState',
                                                        enum_cls=pm_types.AlertActivation)
    _props = ('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateContainer(AbstractStateContainer):
    is_multi_state = True
    Category = cp.SubElementProperty(pm.Category, value_class=pm_types.CodedValue, is_optional=True)
    Handle = cp.HandleAttributeProperty('Handle', is_optional=False)
    _props = ('Category', 'Handle',)

    def __init__(self, descriptor_container, handle=None):
        super().__init__(descriptor_container)
        self.Handle = handle  # pylint: disable=invalid-name

    def update_from_other_container(self, other, skipped_properties=None):
        # Accept node only if descriptorHandle and Handle match
        if self.Handle is not None and other.Handle != self.Handle:
            raise ValueError(
                f'Update from a node with different handle is not possible! Have "{self.Handle}", got "{other.Handle}"')
        super().update_from_other_container(other, skipped_properties)

    def mk_state_node(self, tag, nsmapper, set_xsi_type=True):
        if self.Handle is None:
            self.Handle = uuid.uuid4().hex
        return super().mk_state_node(tag, nsmapper, set_xsi_type)

    def __repr__(self):
        return f'{self.__class__.__name__} DescriptorHandle="{self.DescriptorHandle}" ' \
               f'Handle="{self.Handle}" type={self.NODETYPE}'


class AbstractContextStateContainer(AbstractMultiStateContainer):
    is_context_state = True
    Validator = cp.SubElementListProperty(pm.Validator, value_class=pm_types.InstanceIdentifier)
    Identification = cp.SubElementListProperty(pm.Identification, value_class=pm_types.InstanceIdentifier)
    ContextAssociation = cp.EnumAttributeProperty('ContextAssociation',
                                                  enum_cls=pm_types.ContextAssociation,
                                                  implied_py_value=pm_types.ContextAssociation.NO_ASSOCIATION)
    BindingMdibVersion = cp.ReferencedVersionAttributeProperty('BindingMdibVersion')
    UnbindingMdibVersion = cp.ReferencedVersionAttributeProperty('UnbindingMdibVersion')
    BindingStartTime = cp.TimestampAttributeProperty('BindingStartTime')
    BindingEndTime = cp.TimestampAttributeProperty('BindingEndTime')
    _props = ('Validator', 'Identification', 'ContextAssociation', 'BindingMdibVersion', 'UnbindingMdibVersion',
              'BindingStartTime', 'BindingEndTime')


class LocationContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.LocationContextState
    LocationDetail = cp.SubElementProperty(pm.LocationDetail,
                                           value_class=pm_types.LocationDetail,
                                           default_py_value=pm_types.LocationDetail(),
                                           is_optional=True)
    _props = ('LocationDetail',)

    def update_from_sdc_location(self, sdc_location):
        # pylint: disable=invalid-name
        self.LocationDetail.PoC = sdc_location.poc
        self.LocationDetail.Room = sdc_location.rm
        self.LocationDetail.Bed = sdc_location.bed
        self.LocationDetail.Facility = sdc_location.fac
        self.LocationDetail.Building = sdc_location.bld
        self.LocationDetail.Floor = sdc_location.flr
        self.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED

        extension_string = self._mk_extension_string(sdc_location)
        if not extension_string:
            # schema does not allow extension string of zero length
            extension_string = None
        self.Identification = [pm_types.InstanceIdentifier(root=sdc_location.root, extension_string=extension_string)]
        # pylint: enable=invalid-name

    @staticmethod
    def _mk_extension_string(sdc_location):
        return sdc_location.mk_extension_string()

    @classmethod
    def from_sdc_location(cls, descriptor_container, handle, sdc_location):
        obj = cls(descriptor_container)
        obj.Handle = handle
        obj.update_from_sdc_location(sdc_location)
        return obj


class PatientContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.PatientContextState
    CoreData = cp.SubElementProperty(pm.CoreData,
                                     value_class=pm_types.PatientDemographicsCoreData,
                                     default_py_value=pm_types.PatientDemographicsCoreData(),
                                     is_optional=True)
    _props = ('CoreData',)


class WorkflowContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.WorkflowContextState
    WorkflowDetail = cp.SubElementProperty(pm.WorkflowDetail, value_class=pm_types.WorkflowDetail)
    _props = ('WorkflowDetail',)


class OperatorContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.OperatorContextState
    OperatorDetails = cp.SubElementProperty(pm.OperatorDetails,
                                            value_class=pm_types.BaseDemographics,
                                            is_optional=True)
    _props = ('OperatorDetails',)


class MeansContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.MeansContextState
    # class has no own members


class EnsembleContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.EnsembleContextState
    # class has no own members


# mapping of states: xsi:type information to classes
# find all classes in this module that have a member "NODETYPE"
classes = inspect.getmembers(sys.modules[__name__],
                             lambda member: inspect.isclass(member) and member.__module__ == __name__)
classes_with_NODETYPE = [c[1] for c in classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself
# _state_lookup_by_type = dict([(c.NODETYPE, c) for c in classes_with_NODETYPE])
_state_lookup_by_type = {c.NODETYPE: c for c in classes_with_NODETYPE}


def get_container_class(type_qname):
    """ Returns class for given type
    :param type_qname: the QName of the expected NODETYPE
    """
    return _state_lookup_by_type.get(type_qname)
