import inspect
import sys
import time
import uuid

from . import containerproperties as cp
from .containerbase import ContainerBase
from .. import pmtypes
from .. import pm_qnames as pm


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

    Extension = cp.ExtensionNodeProperty()
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

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" StateVersion={self.StateVersion}'


class AbstractOperationStateContainer(AbstractStateContainer):
    NODETYPE = pm.AbstractOperationState
    is_operational_state = True
    OperatingMode = cp.EnumAttributeProperty('OperatingMode', default_py_value=pmtypes.OperatingMode.ENABLED,
                                             enum_cls=pmtypes.OperatingMode)
    _props = ('OperatingMode',)


class SetValueOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = pm.SetValueOperationState
    AllowedRange = cp.SubElementListProperty(pm.AllowedRange, value_class=pmtypes.Range)
    _props = ('AllowedRange',)


class T_AllowedValues(pmtypes.PropertyBasedPMType):  # pylint: disable=invalid-name
    Value = cp.SubElementTextListProperty(pm.Value)
    _props = ['Value']

    def __init__(self):
        pass

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


class AbstractMetricStateContainerBase(AbstractStateContainer):
    """
    This class is not in the xml schema hierarchy, it only helps to centrally implement functionality
    """
    is_metric_state = True

    @property
    def MetricValue(self):  # pylint: disable=invalid-name
        return self._metric_value

    @MetricValue.setter
    def MetricValue(self, metric_value_object):  # pylint: disable=invalid-name
        if metric_value_object is not None:
            assert isinstance(metric_value_object,
                              self.__class__._metric_value.value_class)  # pylint: disable=protected-access, no-member
        self._metric_value = metric_value_object

    def mk_metric_value(self):
        if self._metric_value is None:
            cls = self.__class__._metric_value.value_class  # pylint: disable=protected-access, no-member
            self._metric_value = cls()
            return self._metric_value
        raise ValueError(f'State (descriptor handle="{self.DescriptorHandle}") already has a metric value')


class AbstractMetricStateContainer(AbstractMetricStateContainerBase):
    BodySite = cp.SubElementListProperty(pm.BodySite, value_class=pmtypes.CodedValue)
    PhysicalConnector = cp.SubElementProperty(pm.PhysicalConnector,
                                              value_class=pmtypes.PhysicalConnectorInfo, is_optional=True)
    ActivationState = cp.EnumAttributeProperty('ActivationState', implied_py_value=pmtypes.ComponentActivation.ON,
                                               enum_cls=pmtypes.ComponentActivation)
    ActiveDeterminationPeriod = cp.DurationAttributeProperty('ActiveDeterminationPeriod')  # xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod')  # xsd:duration, optional
    _props = ('BodySite', 'PhysicalConnector', 'ActivationState', 'ActiveDeterminationPeriod', 'LifeTimePeriod')


class NumericMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.NumericMetricState
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pmtypes.NumericMetricValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pmtypes.Range)
    ActiveAveragingPeriod = cp.DurationAttributeProperty('ActiveAveragingPeriod')  # xsd:duration
    _props = ('_metric_value', 'PhysiologicalRange', 'ActiveAveragingPeriod')


class StringMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.StringMetricState
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pmtypes.StringMetricValue, is_optional=True)
    _props = ('_metric_value',)


class EnumStringMetricStateContainer(StringMetricStateContainer):
    NODETYPE = pm.EnumStringMetricState
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pmtypes.StringMetricValue, is_optional=True)


class RealTimeSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.RealTimeSampleArrayMetricState
    is_realtime_sample_array_metric_state = True
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pmtypes.SampleArrayValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pmtypes.Range)
    _props = ('_metric_value', 'PhysiologicalRange')
    MetricValue = _metric_value

    def __repr__(self):
        samples_count = 0
        if self._metric_value is not None and self._metric_value.Samples is not None:
            samples_count = len(self._metric_value.Samples)
        return f'{self.__class__.__name__} descriptorHandle="{self.DescriptorHandle}" ' \
               f'Activation="{self.ActivationState}" Samples={samples_count}'


class DistributionSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = pm.DistributionSampleArrayMetricState
    _metric_value = cp.SubElementProperty(pm.MetricValue, value_class=pmtypes.SampleArrayValue, is_optional=True)
    PhysiologicalRange = cp.SubElementListProperty(pm.PhysiologicalRange, value_class=pmtypes.Range)
    _props = ('_metric_value', 'PhysiologicalRange')


class AbstractDeviceComponentStateContainer(AbstractStateContainer):
    is_component_state = True
    CalibrationInfo = cp.SubElementProperty(pm.CalibrationInfo,
                                            value_class=pmtypes.CalibrationInfo,
                                            is_optional=True)
    NextCalibration = cp.SubElementProperty(pm.NextCalibration,
                                            value_class=pmtypes.CalibrationInfo,
                                            is_optional=True)
    PhysicalConnector = cp.SubElementProperty(pm.PhysicalConnector,
                                              value_class=pmtypes.PhysicalConnectorInfo,
                                              is_optional=True)

    ActivationState = cp.EnumAttributeProperty('ActivationState', enum_cls=pmtypes.ComponentActivation)
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
                                                  value_class=pmtypes.OperatingJurisdiction,
                                                  is_optional=True)
    OperatingMode = cp.EnumAttributeProperty('OperatingMode',
                                             default_py_value=pmtypes.MdsOperatingMode.NORMAL,
                                             enum_cls=pmtypes.MdsOperatingMode)
    Lang = cp.StringAttributeProperty('Lang', default_py_value='en')
    _props = ('OperatingJurisdiction', 'OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ScoState
    OperationGroup = cp.SubElementListProperty(pm.OperationGroup, value_class=pmtypes.OperationGroup)
    InvocationRequested = cp.OperationRefListAttributeProperty('InvocationRequested')
    InvocationRequired = cp.OperationRefListAttributeProperty('InvocationRequired')
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = pm.VmdState
    OperatingJurisdiction = cp.SubElementProperty(pm.OperatingJurisdiction,
                                                  value_class=pmtypes.OperatingJurisdiction,
                                                  is_optional=True)
    _props = ('OperatingJurisdiction',)


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ChannelState


class ClockStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = pm.ClockState
    ActiveSyncProtocol = cp.SubElementProperty(pm.ActiveSyncProtocol, value_class=pmtypes.CodedValue, is_optional=True)
    ReferenceSource = cp.SubElementListProperty(pm.ReferenceSource, value_class=pmtypes.ElementWithTextOnly)
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
    class ChargeStatusEnum(pmtypes.StringEnum):
        FULL = 'Ful'
        CHARGING = 'ChB'
        DISCHARGING = 'DisChB'
        EMPTY = 'DEB'

    NODETYPE = pm.BatteryState
    CapacityRemaining = cp.SubElementProperty(pm.CapacityRemaining,
                                              value_class=pmtypes.Measurement,
                                              is_optional=True)
    Voltage = cp.SubElementProperty(pm.Voltage, value_class=pmtypes.Measurement, is_optional=True)
    Current = cp.SubElementProperty(pm.Current, value_class=pmtypes.Measurement, is_optional=True)
    Temperature = cp.SubElementProperty(pm.Temperature, value_class=pmtypes.Measurement, is_optional=True)
    RemainingBatteryTime = cp.SubElementProperty(pm.RemainingBatteryTime,
                                                 value_class=pmtypes.Measurement,
                                                 is_optional=True)
    ChargeStatus = cp.EnumAttributeProperty('ChargeStatus', enum_cls=ChargeStatusEnum)
    ChargeCycles = cp.UnsignedIntAttributeProperty('ChargeCycles')  # Number of charge/discharge cycles.
    _props = (
        'CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus',
        'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    is_alert_state = True
    ActivationState = cp.EnumAttributeProperty('ActivationState',
                                               default_py_value=pmtypes.AlertActivation.ON,
                                               enum_cls=pmtypes.AlertActivation,
                                               is_optional=False)
    _props = ('ActivationState',)


class AlertSystemStateContainer(AbstractAlertStateContainer):
    NODETYPE = pm.AlertSystemState
    SystemSignalActivation = cp.SubElementListProperty(pm.SystemSignalActivation,
                                                       value_class=pmtypes.SystemSignalActivation)
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
    Presence = cp.EnumAttributeProperty('Presence', implied_py_value=pmtypes.AlertSignalPresence.OFF,
                                        enum_cls=pmtypes.AlertSignalPresence)
    Location = cp.EnumAttributeProperty('Location', implied_py_value=pmtypes.AlertSignalPrimaryLocation.LOCAL,
                                        enum_cls=pmtypes.AlertSignalPrimaryLocation)
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
    ActualPriority = cp.EnumAttributeProperty('ActualPriority', enum_cls=pmtypes.AlertConditionPriority)
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
    Limits = cp.SubElementProperty(pm.Limits, value_class=pmtypes.Range, default_py_value=pmtypes.Range())
    MonitoredAlertLimits = cp.EnumAttributeProperty('MonitoredAlertLimits',
                                                    default_py_value=pmtypes.AlertConditionMonitoredLimits.NONE,
                                                    enum_cls=pmtypes.AlertConditionMonitoredLimits,
                                                    is_optional=False)
    AutoLimitActivationState = cp.EnumAttributeProperty('AutoLimitActivationState',
                                                        enum_cls=pmtypes.AlertActivation)
    _props = ('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateContainer(AbstractStateContainer):
    is_multi_state = True
    Category = cp.SubElementProperty(pm.Category, value_class=pmtypes.CodedValue, is_optional=True)
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
    Validator = cp.SubElementListProperty(pm.Validator, value_class=pmtypes.InstanceIdentifier)
    Identification = cp.SubElementListProperty(pm.Identification, value_class=pmtypes.InstanceIdentifier)
    ContextAssociation = cp.EnumAttributeProperty('ContextAssociation',
                                                  enum_cls=pmtypes.ContextAssociation,
                                                  implied_py_value=pmtypes.ContextAssociation.NO_ASSOCIATION)
    BindingMdibVersion = cp.ReferencedVersionAttributeProperty('BindingMdibVersion')
    UnbindingMdibVersion = cp.ReferencedVersionAttributeProperty('UnbindingMdibVersion')
    BindingStartTime = cp.TimestampAttributeProperty('BindingStartTime')
    BindingEndTime = cp.TimestampAttributeProperty('BindingEndTime')
    _props = ('Validator', 'Identification', 'ContextAssociation', 'BindingMdibVersion', 'UnbindingMdibVersion',
              'BindingStartTime', 'BindingEndTime')


class LocationContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.LocationContextState
    LocationDetail = cp.SubElementProperty(pm.LocationDetail,
                                           value_class=pmtypes.LocationDetail,
                                           default_py_value=pmtypes.LocationDetail(),
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
        self.ContextAssociation = pmtypes.ContextAssociation.ASSOCIATED

        extension_string = self._mk_extension_string(sdc_location)
        if not extension_string:
            # schema does not allow extension string of zero length
            extension_string = None
        self.Identification = [pmtypes.InstanceIdentifier(root=sdc_location.root, extension_string=extension_string)]
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
                                     value_class=pmtypes.PatientDemographicsCoreData,
                                     default_py_value=pmtypes.PatientDemographicsCoreData(),
                                     is_optional=True)
    _props = ('CoreData',)


class WorkflowContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.WorkflowContextState
    WorkflowDetail = cp.SubElementProperty(pm.WorkflowDetail, value_class=pmtypes.WorkflowDetail)
    _props = ('WorkflowDetail',)


class OperatorContextStateContainer(AbstractContextStateContainer):
    NODETYPE = pm.OperatorContextState
    OperatorDetails = cp.SubElementProperty(pm.OperatorDetails,
                                            value_class=pmtypes.BaseDemographics,
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
