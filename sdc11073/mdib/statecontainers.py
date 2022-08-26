import inspect
import sys
import time
import uuid

from . import containerproperties as cp
from .containerbase import ContainerBase
from .. import pmtypes
from ..namespaces import domTag


class AbstractStateContainer(ContainerBase):
    # these class variables allow easy type-checking. Derived classes will set corresponding values to True
    isSystemContextState = False
    isRealtimeSampleArrayMetricState = False
    isMetricState = False
    isOperationalState = False
    isComponentState = False
    isAlertState = False
    isAlertSignal = False
    isAlertCondition = False
    isMultiState = False
    isContextState = False

    Extension = cp.ExtensionNodeProperty()
    DescriptorHandle = cp.HandleRefAttributeProperty('DescriptorHandle', is_optional=False)
    descriptorHandle = DescriptorHandle
    DescriptorVersion = cp.ReferencedVersionAttributeProperty('DescriptorVersion', default_py_value=0)
    StateVersion = cp.VersionCounterAttributeProperty('StateVersion', default_py_value=0)
    _props = ('Extension', 'DescriptorHandle', 'DescriptorVersion', 'StateVersion')

    stateVersion = StateVersion  # lower case for backwards compatibility

    def __init__(self, descriptor_container):
        super().__init__()
        self.descriptor_container = descriptor_container
        if descriptor_container is not None:
            # pylint: disable=invalid-name
            self.DescriptorHandle = descriptor_container.Handle
            self.DescriptorVersion = descriptor_container.DescriptorVersion
            # pylint: enable=invalid-name

    def set_node_member(self, nsmapper):
        self.node = self.mk_state_node(domTag('State'), nsmapper)

    def mk_state_node(self, tag, nsmapper, set_xsi_type=True):
        return super().mk_node(tag, nsmapper, set_xsi_type=set_xsi_type)

    def update_from_other_container(self, other, skipped_properties=None):
        if other.descriptorHandle != self.descriptorHandle:
            raise RuntimeError(
                f'Update from a node with different descriptor handle is not possible! '
                f'Have "{self.descriptorHandle}", got "{other.descriptorHandle}"')
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
            raise RuntimeError(f'State {self} has no descriptor_container')
        if self.descriptor_container.DescriptorVersion != self.DescriptorVersion:
            self.DescriptorVersion = self.descriptor_container.DescriptorVersion

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.descriptorHandle}" StateVersion={self.StateVersion}'


class AbstractOperationStateContainer(AbstractStateContainer):
    NODETYPE = domTag('AbstractOperationState')  # a QName
    isOperationalState = True
    OperatingMode = cp.EnumAttributeProperty('OperatingMode', default_py_value=pmtypes.OperatingMode.ENABLED,
                                             enum_cls=pmtypes.OperatingMode)
    _props = ('OperatingMode',)


class SetValueOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetValueOperationState')  # a QName
    AllowedRange = cp.SubElementListProperty(domTag('AllowedRange'), value_class=pmtypes.Range)
    _props = ('AllowedRange',)


class T_AllowedValues(pmtypes.PropertyBasedPMType):  # pylint: disable=invalid-name
    Value = cp.SubElementTextListProperty(domTag('Value'))
    _props = ['Value']

    def __init__(self):
        pass

    def is_empty(self):
        return self.Value is None or len(self.Value) == 0


class SetStringOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetStringOperationState')
    AllowedValues = cp.SubElementWithSubElementListProperty(domTag('AllowedValues'),
                                                            default_py_value=T_AllowedValues(),
                                                            value_class=T_AllowedValues)
    _props = ('AllowedValues',)


class ActivateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('ActivateOperationState')  # a QName


class SetContextStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetContextStateOperationState')  # a QName


class SetMetricStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetMetricStateOperationState')  # a QName


class SetComponentStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetComponentStateOperationState')  # a QName


class SetAlertStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetAlertStateOperationState')  # a QName


class AbstractMetricStateContainerBase(AbstractStateContainer):
    """
    This class is not in the xml schema hierarchy, it only helps to centrally implement functionality
    """
    isMetricState = True

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
        raise RuntimeError(f'State (descriptor handle="{self.descriptorHandle}") already has a metric value')


class AbstractMetricStateContainer(AbstractMetricStateContainerBase):
    BodySite = cp.SubElementListProperty(domTag('BodySite'), value_class=pmtypes.CodedValue)
    PhysicalConnector = cp.SubElementProperty(domTag('PhysicalConnector'),
                                              value_class=pmtypes.PhysicalConnectorInfo, is_optional=True)
    ActivationState = cp.EnumAttributeProperty('ActivationState', implied_py_value=pmtypes.ComponentActivation.ON,
                                               enum_cls=pmtypes.ComponentActivation)
    ActiveDeterminationPeriod = cp.DurationAttributeProperty('ActiveDeterminationPeriod')  # xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod')  # xsd:duration, optional
    _props = ('BodySite', 'PhysicalConnector', 'ActivationState', 'ActiveDeterminationPeriod', 'LifeTimePeriod')


class NumericMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('NumericMetricState')
    _metric_value = cp.SubElementProperty(domTag('MetricValue'), value_class=pmtypes.NumericMetricValue)
    PhysiologicalRange = cp.SubElementListProperty(domTag('PhysiologicalRange'), value_class=pmtypes.Range)
    ActiveAveragingPeriod = cp.DurationAttributeProperty('ActiveAveragingPeriod')  # xsd:duration
    _props = ('_metric_value', 'PhysiologicalRange', 'ActiveAveragingPeriod')


class StringMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('StringMetricState')
    _metric_value = cp.SubElementProperty(domTag('MetricValue'), value_class=pmtypes.StringMetricValue)
    _props = ('_metric_value',)


class EnumStringMetricStateContainer(StringMetricStateContainer):
    NODETYPE = domTag('EnumStringMetricState')
    _metric_value = cp.SubElementProperty(domTag('MetricValue'), value_class=pmtypes.StringMetricValue)


class RealTimeSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('RealTimeSampleArrayMetricState')
    isRealtimeSampleArrayMetricState = True
    _metric_value = cp.SubElementProperty(domTag('MetricValue'), value_class=pmtypes.SampleArrayValue)
    PhysiologicalRange = cp.SubElementListProperty(domTag('PhysiologicalRange'), value_class=pmtypes.Range)
    _props = ('_metric_value', 'PhysiologicalRange')
    MetricValue = _metric_value

    def __repr__(self):
        samples_count = 0
        if self._metric_value is not None and self._metric_value.Samples is not None:
            samples_count = len(self._metric_value.Samples)
        return f'{self.__class__.__name__} descriptorHandle="{self.descriptorHandle}" ' \
               f'Activation="{self.ActivationState}" Samples={samples_count}'


class DistributionSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('DistributionSampleArrayMetricState')
    _metric_value = cp.SubElementProperty(domTag('MetricValue'), value_class=pmtypes.SampleArrayValue)
    PhysiologicalRange = cp.SubElementListProperty(domTag('PhysiologicalRange'), value_class=pmtypes.Range)
    _props = ('_metric_value', 'PhysiologicalRange')


class AbstractDeviceComponentStateContainer(AbstractStateContainer):
    isComponentState = True
    CalibrationInfo = cp.SubElementProperty(domTag('CalibrationInfo'),
                                            value_class=pmtypes.CalibrationInfo,
                                            is_optional=True)
    NextCalibration = cp.SubElementProperty(domTag('NextCalibration'),
                                            value_class=pmtypes.CalibrationInfo,
                                            is_optional=True)
    PhysicalConnector = cp.SubElementProperty(domTag('PhysicalConnector'),
                                              value_class=pmtypes.PhysicalConnectorInfo)  # optional

    ActivationState = cp.EnumAttributeProperty('ActivationState', enum_cls=pmtypes.ComponentActivation)
    OperatingHours = cp.IntegerAttributeProperty('OperatingHours')  # optional, unsigned int
    OperatingCycles = cp.IntegerAttributeProperty('OperatingCycles')  # optional, unsigned int
    _props = (
        'CalibrationInfo', 'NextCalibration', 'PhysicalConnector', 'ActivationState', 'OperatingHours',
        'OperatingCycles')


class AbstractComplexDeviceComponentStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('AbstractComplexDeviceComponentState')


class MdsStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = domTag('MdsState')
    OperatingJurisdiction = cp.SubElementProperty(domTag('OperatingJurisdiction'),
                                                  value_class=pmtypes.OperatingJurisdiction,
                                                  is_optional=True)
    OperatingMode = cp.EnumAttributeProperty('OperatingMode',
                                             default_py_value=pmtypes.MdsOperatingMode.NORMAL,
                                             enum_cls=pmtypes.MdsOperatingMode)
    Lang = cp.StringAttributeProperty('Lang', default_py_value='en')
    _props = ('OperatingJurisdiction', 'OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ScoState')
    OperationGroup = cp.SubElementListProperty(domTag('OperationGroup'), value_class=pmtypes.OperationGroup)
    InvocationRequested = cp.OperationRefListAttributeProperty('InvocationRequested')
    InvocationRequired = cp.OperationRefListAttributeProperty('InvocationRequired')
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = domTag('VmdState')
    OperatingJurisdiction = cp.SubElementProperty(domTag('OperatingJurisdiction'),
                                                  value_class=pmtypes.OperatingJurisdiction,
                                                  is_optional=True)
    _props = ('OperatingJurisdiction',)


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ChannelState')


class ClockStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ClockState')
    ActiveSyncProtocol = cp.SubElementProperty(domTag('ActiveSyncProtocol'), value_class=pmtypes.CodedValue)
    ReferenceSource = cp.SubElementListProperty(domTag('ReferenceSource'), value_class=pmtypes.ElementWithTextOnly)
    DateAndTime = cp.CurrentTimestampAttributeProperty('DateAndTime')
    RemoteSync = cp.BooleanAttributeProperty('RemoteSync', default_py_value=True, is_optional=False)
    Accuracy = cp.DecimalAttributeProperty('Accuracy')
    LastSet = cp.TimestampAttributeProperty('LastSet')
    TimeZone = cp.TimeZoneAttributeProperty('TimeZone')  # a time zone string
    CriticalUse = cp.BooleanAttributeProperty('CriticalUse', implied_py_value=False)  # optional
    _props = ('ActiveSyncProtocol', 'ReferenceSource', 'DateAndTime', 'RemoteSync', 'Accuracy', 'LastSet', 'TimeZone',
              'CriticalUse')


class SystemContextStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('SystemContextState')


class BatteryStateContainer(AbstractDeviceComponentStateContainer):
    class ChargeStatusEnum(pmtypes.StringEnum):
        FULL = 'Ful'
        CHARGING = 'ChB'
        DISCHARGING = 'DisChB'
        EMPTY = 'DEB'

    NODETYPE = domTag('BatteryState')
    CapacityRemaining = cp.SubElementProperty(domTag('CapacityRemaining'),
                                              value_class=pmtypes.Measurement,
                                              is_optional=True)
    Voltage = cp.SubElementProperty(domTag('Voltage'), value_class=pmtypes.Measurement, is_optional=True)
    Current = cp.SubElementProperty(domTag('Current'), value_class=pmtypes.Measurement, is_optional=True)
    Temperature = cp.SubElementProperty(domTag('Temperature'), value_class=pmtypes.Measurement, is_optional=True)
    RemainingBatteryTime = cp.SubElementProperty(domTag('RemainingBatteryTime'),
                                                 value_class=pmtypes.Measurement,
                                                 is_optional=True)
    ChargeStatus = cp.EnumAttributeProperty('ChargeStatus', enum_cls=ChargeStatusEnum)
    ChargeCycles = cp.IntegerAttributeProperty('ChargeCycles')  # Number of charge/discharge cycles.
    _props = (
        'CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus',
        'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    isAlertState = True
    ActivationState = cp.EnumAttributeProperty('ActivationState',
                                               default_py_value=pmtypes.AlertActivation.ON,
                                               enum_cls=pmtypes.AlertActivation,
                                               is_optional=False)
    _props = ('ActivationState',)


class AlertSystemStateContainer(AbstractAlertStateContainer):
    NODETYPE = domTag('AlertSystemState')
    SystemSignalActivation = cp.SubElementListProperty(domTag('SystemSignalActivation'),
                                                       value_class=pmtypes.SystemSignalActivation)
    LastSelfCheck = cp.TimestampAttributeProperty('LastSelfCheck')
    SelfCheckCount = cp.IntegerAttributeProperty('SelfCheckCount')
    PresentPhysiologicalAlarmConditions = cp.AlertConditionRefListAttributeProperty(
        'PresentPhysiologicalAlarmConditions')
    PresentTechnicalAlarmConditions = cp.AlertConditionRefListAttributeProperty('PresentTechnicalAlarmConditions')
    _props = ('SystemSignalActivation', 'LastSelfCheck', 'SelfCheckCount', 'PresentPhysiologicalAlarmConditions',
              'PresentTechnicalAlarmConditions')

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.descriptorHandle}" ' \
               f'StateVersion={self.StateVersion} LastSelfCheck={self.LastSelfCheck} ' \
               f'SelfCheckCount={self.SelfCheckCount}'


class AlertSignalStateContainer(AbstractAlertStateContainer):
    isAlertSignal = True
    NODETYPE = domTag('AlertSignalState')
    ActualSignalGenerationDelay = cp.DurationAttributeProperty('ActualSignalGenerationDelay')
    Presence = cp.EnumAttributeProperty('Presence', implied_py_value=pmtypes.AlertSignalPresence.OFF,
                                        enum_cls=pmtypes.AlertSignalPresence)
    Location = cp.EnumAttributeProperty('Location', implied_py_value=pmtypes.AlertSignalPrimaryLocation.LOCAL,
                                        enum_cls=pmtypes.AlertSignalPrimaryLocation)
    Slot = cp.IntegerAttributeProperty('Slot')
    _props = ('ActualSignalGenerationDelay', 'Presence', 'Location', 'Slot')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_updated = time.time()

        if self.descriptor_container is not None and self.descriptor_container.SignalDelegationSupported:
            # Delegable signals should have location Remote according to BICEPS
            self.Location = pmtypes.AlertSignalPrimaryLocation.REMOTE  # pylint: disable=invalid-name


class AlertConditionStateContainer(AbstractAlertStateContainer):
    isAlertCondition = True
    NODETYPE = domTag('AlertConditionState')
    ActualConditionGenerationDelay = cp.DurationAttributeProperty('ActualConditionGenerationDelay')  # xsd:duration
    ActualPriority = cp.EnumAttributeProperty('ActualPriority', enum_cls=pmtypes.AlertConditionPriority)
    Rank = cp.IntegerAttributeProperty('Rank')  # Integer
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime')  # Integer
    Presence = cp.BooleanAttributeProperty('Presence', implied_py_value=False)
    _props = ('ActualConditionGenerationDelay', 'ActualPriority', 'Rank', 'DeterminationTime', 'Presence')


class LimitAlertConditionStateContainer(AlertConditionStateContainer):
    NODETYPE = domTag('LimitAlertConditionState')  # a QName
    Limits = cp.SubElementProperty(domTag('Limits'), value_class=pmtypes.Range,
                                   default_py_value=pmtypes.Range())  # required, pm:Range
    MonitoredAlertLimits = cp.EnumAttributeProperty('MonitoredAlertLimits',
                                                    default_py_value=pmtypes.AlertConditionMonitoredLimits.ALL_OFF,
                                                    enum_cls=pmtypes.AlertConditionMonitoredLimits,
                                                    is_optional=False)
    AutoLimitActivationState = cp.EnumAttributeProperty('AutoLimitActivationState',
                                                        enum_cls=pmtypes.AlertActivation)
    _props = ('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateContainer(AbstractStateContainer):
    isMultiState = True
    Handle = cp.HandleAttributeProperty('Handle', is_optional=False)
    _props = ('Handle',)

    def __init__(self, descriptor_container):
        super().__init__(descriptor_container)
        self.Handle = uuid.uuid4().hex  # pylint: disable=invalid-name
        self._handle_is_generated = True

    def update_from_other_container(self, other, skipped_properties=None):
        # Accept node only if descriptorHandle and Handle match
        if self._handle_is_generated:
            self.Handle = other.Handle
            self._handle_is_generated = False
        elif other.Handle != self.Handle:
            raise RuntimeError(
                f'Update from a node with different handle is not possible! Have "{self.Handle}", got "{other.Handle}"')
        super().update_from_other_container(other, skipped_properties)

    def mk_state_node(self, tag, nsmapper, set_xsi_type=True):
        if self.Handle is None:
            self.Handle = uuid.uuid4().hex
        return super().mk_state_node(tag, nsmapper, set_xsi_type)

    def __repr__(self):
        return f'{self.__class__.__name__} descriptorHandle="{self.descriptorHandle}" ' \
               f'Handle="{self.Handle}" type={self.NODETYPE}'


class AbstractContextStateContainer(AbstractMultiStateContainer):
    isContextState = True
    Validator = cp.SubElementListProperty(domTag('Validator'), value_class=pmtypes.InstanceIdentifier)
    Identification = cp.SubElementListProperty(domTag('Identification'), value_class=pmtypes.InstanceIdentifier)
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
    NODETYPE = domTag('LocationContextState')
    LocationDetail = cp.SubElementProperty(domTag('LocationDetail'),
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
    NODETYPE = domTag('PatientContextState')
    CoreData = cp.SubElementProperty(domTag('CoreData'),
                                     value_class=pmtypes.PatientDemographicsCoreData,
                                     default_py_value=pmtypes.PatientDemographicsCoreData(),
                                     is_optional=True)
    _props = ('CoreData',)


class WorkflowContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('WorkflowContextState')
    WorkflowDetail = cp.SubElementProperty(domTag('WorkflowDetail'), value_class=pmtypes.WorkflowDetail)
    _props = ('WorkflowDetail',)


class OperatorContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('OperatorContextState')
    OperatorDetails = cp.SubElementProperty(domTag('OperatorDetails'),
                                            value_class=pmtypes.BaseDemographics,
                                            is_optional=True)
    _props = ('OperatorDetails',)


class MeansContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('MeansContextState')
    # class has no own members


class EnsembleContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('EnsembleContextState')
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
    """
    :param type_qname: the QName of the expected NODETYPE
    """
    return _state_lookup_by_type.get(type_qname)
