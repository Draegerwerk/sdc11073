import time
import uuid
import sys
import inspect
from .containerbase import ContainerBase
from ..namespaces import domTag
from .. import pmtypes 
from . import containerproperties as cp


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

    ext_Extension = cp.ExtensionNodeProperty()
    DescriptorHandle = cp.StringAttributeProperty('DescriptorHandle', isOptional=False)
    descriptorHandle = DescriptorHandle
    DescriptorVersion = cp.IntegerAttributeProperty('DescriptorVersion', defaultPyValue=0) # an integer
    StateVersion = cp.IntegerAttributeProperty('StateVersion', defaultPyValue=0) # an integer
    _props=('ext_Extension', 'DescriptorHandle', 'DescriptorVersion', 'StateVersion')

    stateVersion = StateVersion   # lower case for backwards compatibility
    
    def __init__(self, nsmapper, descriptorContainer):
        super(AbstractStateContainer, self).__init__(nsmapper)
        self.descriptorContainer = descriptorContainer
        self.DescriptorHandle = descriptorContainer.handle
        self.DescriptorVersion = descriptorContainer.DescriptorVersion

    def updateNode(self):
        self.node = self.mkStateNode(domTag('State'))


    def mkStateNode(self, tag, updateDescriptorVersion=True):
        if updateDescriptorVersion:
            self.updateDescriptorVersion()
        node = super(AbstractStateContainer, self).mkNode(tag, setXsiType=True)
        node.set('DescriptorHandle', self.descriptorHandle)
        return node


    def update_from_other_container(self, other, skipped_properties=None):
        if other.descriptorHandle != self.descriptorHandle:
            raise RuntimeError('Update from a node with different descriptor handle is not possible! Have "{}", got "{}"'.format(self.descriptorHandle, other.descriptorHandle))
        self._update_from_other(other, skipped_properties)
        self.node = other.node

    updateFromOtherContainer = update_from_other_container  #


    def incrementState(self):
        if self.StateVersion is None:
            self.StateVersion = 1
        else:
            self.StateVersion += 1


    def updateDescriptorVersion(self):
        if self.descriptorContainer is None:
            raise RuntimeError('State {} has no descriptorContainer'.format(self))
        if self.descriptorContainer.DescriptorVersion != self.DescriptorVersion:
            self.DescriptorVersion = self.descriptorContainer.DescriptorVersion

    def __repr__(self):
        return '{} descriptorHandle="{}" StateVersion={}'.format(self.__class__.__name__, self.descriptorHandle, self.StateVersion)


class AbstractOperationStateContainer(AbstractStateContainer):
    NODETYPE = domTag('AbstractOperationState') # a QName
    isOperationalState = True
    OperatingMode = cp.EnumAttributeProperty('OperatingMode', defaultPyValue=pmtypes.OperatingMode.ENABLED,
                                             enum_cls=pmtypes.OperatingMode)
    _props=('OperatingMode',)    


class SetValueOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetValueOperationState') # a QName
    AllowedRange = cp.SubElementListProperty([domTag('AllowedRange')], cls=pmtypes.Range)
    _props=('AllowedRange',)


class SetStringOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetStringOperationState') # a QName
    AllowedValues = cp.SubElementTextListProperty([domTag('AllowedValues'), domTag('Value')])
    _props = ('AllowedValues',)


class ActivateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('ActivateOperationState') # a QName
    _props = tuple()


class SetContextStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetContextStateOperationState') # a QName
    _props = tuple()


class SetMetricStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetMetricStateOperationState') # a QName
    _props = tuple()


class SetComponentStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetComponentStateOperationState') # a QName
    _props = tuple()


class SetAlertStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetAlertStateOperationState') # a QName
    _props = tuple()



class _AbstractMetricStateContainer_Base(AbstractStateContainer):
    '''
    This class is not in the xml schema hierarchy, it only helps to centrally implement functionality
    '''
    isMetricState = True

    @property
    def metricValue(self):
        return self._MetricValue


    @metricValue.setter
    def metricValue(self, metricValueObject):
        if metricValueObject is not None:
            assert isinstance(metricValueObject, self.__class__._MetricValue.valueClass) #pylint: disable=protected-access
        self._MetricValue = metricValueObject


    def mkMetricValue(self):
        if self._MetricValue is None:
            self._MetricValue = self.__class__._MetricValue.valueClass() #pylint: disable=protected-access
            return self._MetricValue
        else:
            raise RuntimeError('State (handle="{}") already has a metric value'.format(self.handle))


class AbstractMetricStateContainer(_AbstractMetricStateContainer_Base):
    BodySite = cp.SubElementListProperty([domTag('BodySite')], cls=pmtypes.CodedValue)
    PhysicalConnector = cp.SubElementProperty([domTag('PhysicalConnector')],
                                              valueClass=pmtypes.PhysicalConnectorInfo, isOptional=True)
    ActivationState = cp.EnumAttributeProperty('ActivationState', impliedPyValue=pmtypes.ComponentActivation.ON,
                                               enum_cls=pmtypes.ComponentActivation)
    ActiveDeterminationPeriod = cp.DurationAttributeProperty('ActiveDeterminationPeriod') # xsd:duration
    LifeTimePeriod = cp.DurationAttributeProperty('LifeTimePeriod') # xsd:duration, optional
    _props=('BodySite', 'PhysicalConnector', 'ActivationState', 'ActiveDeterminationPeriod', 'LifeTimePeriod')


class NumericMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('NumericMetricState')
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.NumericMetricValue)
    PhysiologicalRange = cp.SubElementListProperty([domTag('PhysiologicalRange')], cls=pmtypes.Range)
    ActiveAveragingPeriod = cp.DurationAttributeProperty('ActiveAveragingPeriod')  # xsd:duration
    _props = ('_MetricValue', 'PhysiologicalRange', 'ActiveAveragingPeriod')


class StringMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('StringMetricState')
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.StringMetricValue)
    _props = ('_MetricValue',)



class EnumStringMetricStateContainer(StringMetricStateContainer):
    NODETYPE = domTag('EnumStringMetricState')
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.StringMetricValue)
    _props = tuple()



class RealTimeSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('RealTimeSampleArrayMetricState')
    isRealtimeSampleArrayMetricState = True
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.SampleArrayValue)
    PhysiologicalRange = cp.SubElementListProperty([domTag('PhysiologicalRange')], cls = pmtypes.Range)
    _props = ('_MetricValue', 'PhysiologicalRange')
    MetricValue = _MetricValue


    def __repr__(self):
        samplesCount = 0
        if self.metricValue is not None and self.metricValue.Samples is not None:
            samplesCount = len(self.metricValue.Samples)
        return '{} descriptorHandle="{}" Activation="{}" Samples={}'.format(self.__class__.__name__,
                                                                            self.descriptorHandle, self.ActivationState,
                                                                            samplesCount)


class DistributionSampleArrayMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('DistributionSampleArrayMetricState')
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.SampleArrayValue)
    PhysiologicalRange = cp.SubElementListProperty([domTag('PhysiologicalRange')], cls = pmtypes.Range)
    _props = ('_MetricValue', 'PhysiologicalRange')



class AbstractDeviceComponentStateContainer(AbstractStateContainer):
    isComponentState = True
    CalibrationInfo = cp.NotImplementedProperty('CalibrationInfo', None)  # optional, CalibrationInfo type
    NextCalibration = cp.NotImplementedProperty('NextCalibration', None)  # optional, CalibrationInfo type
    PhysicalConnector = cp.SubElementProperty([domTag('PhysicalConnector')], valueClass=pmtypes.PhysicalConnectorInfo) #optional

    ActivationState = cp.EnumAttributeProperty('ActivationState', enum_cls=pmtypes.ComponentActivation)
    OperatingHours = cp.IntegerAttributeProperty('OperatingHours')  # optional, unsigned int
    OperatingCycles = cp.IntegerAttributeProperty('OperatingCycles')  # optional, unsigned int
    _props = ('CalibrationInfo', 'NextCalibration', 'PhysicalConnector', 'ActivationState', 'OperatingHours', 'OperatingCycles')


class AbstractComplexDeviceComponentStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('AbstractComplexDeviceComponentState')
    _props = tuple()


class MdsStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = domTag('MdsState')
    OperatingMode = cp.EnumAttributeProperty('OperatingMode',
                                             defaultPyValue=pmtypes.MdsOperatingMode.NORMAL,
                                             enum_cls=pmtypes.MdsOperatingMode)
    Lang = cp.StringAttributeProperty('Lang', defaultPyValue='en')
    _props = ('OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ScoState')
    OperationGroup = cp.SubElementListProperty([domTag('OperationGroup')], cls=pmtypes.OperationGroup)
    InvocationRequested = cp.OperationRefListAttributeProperty('InvocationRequested')
    InvocationRequired = cp.OperationRefListAttributeProperty('InvocationRequired')
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractComplexDeviceComponentStateContainer):
    NODETYPE = domTag('VmdState')
    _props = tuple()


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ChannelState')
    _props = tuple()


class ClockStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ClockState')
    ActiveSyncProtocol = cp.SubElementProperty([domTag('ActiveSyncProtocol')], valueClass=pmtypes.CodedValue)
    ReferenceSource = cp.SubElementListProperty([domTag('ReferenceSource')], cls=pmtypes.ElementWithTextOnly)
    DateAndTime = cp.CurrentTimestampAttributeProperty('DateAndTime')
    RemoteSync = cp.BooleanAttributeProperty('RemoteSync', defaultPyValue=True, isOptional=False)
    Accuracy = cp.DecimalAttributeProperty('Accuracy')
    LastSet = cp.TimestampAttributeProperty('LastSet')
    TimeZone = cp.StringAttributeProperty('TimeZone') # a time zone string
    CriticalUse = cp.BooleanAttributeProperty('CriticalUse', impliedPyValue=False) # optional
    _props = ('ActiveSyncProtocol', 'ReferenceSource', 'DateAndTime', 'RemoteSync', 'Accuracy', 'LastSet', 'TimeZone', 'CriticalUse')


class SystemContextStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('SystemContextState')
    _props = tuple()


class BatteryStateContainer(AbstractDeviceComponentStateContainer):
    class T_ChargeStatus(pmtypes.StringEnum):
        FULL = 'Ful'
        CHARGING = 'ChB'
        DISCHARGING = 'DisChB'
        EMPTY = 'DEB'

    NODETYPE = domTag('BatteryState')
    CapacityRemaining = cp.SubElementProperty([domTag('CapacityRemaining')],
                                              valueClass=pmtypes.Measurement,
                                              isOptional=True)
    Voltage = cp.SubElementProperty([domTag('Voltage')], valueClass=pmtypes.Measurement, isOptional=True)
    Current = cp.SubElementProperty([domTag('Current')], valueClass=pmtypes.Measurement, isOptional=True)
    Temperature = cp.SubElementProperty([domTag('Temperature')], valueClass=pmtypes.Measurement, isOptional=True)
    RemainingBatteryTime = cp.SubElementProperty([domTag('RemainingBatteryTime')],
                                                 valueClass=pmtypes.Measurement,
                                                 isOptional=True)
    ChargeStatus = cp.EnumAttributeProperty('ChargeStatus', enum_cls=T_ChargeStatus)
    ChargeCycles = cp.IntegerAttributeProperty('ChargeCycles') # Number of charge/discharge cycles.
    _props = ('CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus', 'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    isAlertState = True
    ActivationState = cp.EnumAttributeProperty('ActivationState',
                                               defaultPyValue=pmtypes.AlertActivation.ON,
                                               enum_cls=pmtypes.AlertActivation,
                                               isOptional=False)
    _props=('ActivationState', )


class AlertSystemStateContainer(AbstractAlertStateContainer):
    NODETYPE = domTag('AlertSystemState')
    SystemSignalActivation = cp.SubElementListProperty([domTag('SystemSignalActivation')],
                                                       cls=pmtypes.SystemSignalActivation)
    LastSelfCheck = cp.TimestampAttributeProperty('LastSelfCheck')
    SelfCheckCount = cp.IntegerAttributeProperty('SelfCheckCount')
    PresentPhysiologicalAlarmConditions = cp.AlertConditionRefListAttributeProperty(
        'PresentPhysiologicalAlarmConditions')
    PresentTechnicalAlarmConditions = cp.AlertConditionRefListAttributeProperty('PresentTechnicalAlarmConditions')
    _props=('SystemSignalActivation', 'LastSelfCheck', 'SelfCheckCount', 'PresentPhysiologicalAlarmConditions',
            'PresentTechnicalAlarmConditions')

    def __repr__(self):
        return '{} descriptorHandle="{}" StateVersion={} LastSelfCheck={} SelfCheckCount={}'.format(
            self.__class__.__name__, self.descriptorHandle, self.StateVersion, self.LastSelfCheck, self.SelfCheckCount)


class AlertSignalStateContainer(AbstractAlertStateContainer):
    isAlertSignal = True
    NODETYPE = domTag('AlertSignalState')
    ActualSignalGenerationDelay = cp.DurationAttributeProperty('ActualSignalGenerationDelay')
    Presence = cp.EnumAttributeProperty('Presence', impliedPyValue=pmtypes.AlertSignalPresence.OFF,
                                        enum_cls=pmtypes.AlertSignalPresence)
    Location = cp.EnumAttributeProperty('Location', impliedPyValue=pmtypes.AlertSignalPrimaryLocation.LOCAL,
                                        enum_cls=pmtypes.AlertSignalPrimaryLocation)
    Slot = cp.IntegerAttributeProperty('Slot')
    _props = ('ActualSignalGenerationDelay', 'Presence', 'Location', 'Slot')

    def __init__(self, *args, **kwargs):
        super(AlertSignalStateContainer, self).__init__(*args, **kwargs)
        self.lastUpdated = time.time()

        if self.descriptorContainer.SignalDelegationSupported:
            # Delegable signals should have location Remote according to BICEPS
            self.Location = pmtypes.AlertSignalPrimaryLocation.REMOTE


class AlertConditionStateContainer(AbstractAlertStateContainer):
    isAlertCondition = True
    NODETYPE = domTag('AlertConditionState')
    ActualConditionGenerationDelay = cp.DurationAttributeProperty('ActualConditionGenerationDelay')# xsd:duration
    ActualPriority = cp.EnumAttributeProperty('ActualPriority', enum_cls=pmtypes.AlertConditionPriority)
    Rank = cp.IntegerAttributeProperty('Rank') # Integer
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime') # Integer
    Presence = cp.BooleanAttributeProperty('Presence', impliedPyValue=False)
    _props=('ActualConditionGenerationDelay', 'ActualPriority', 'Rank', 'DeterminationTime', 'Presence')


class LimitAlertConditionStateContainer(AlertConditionStateContainer):
    NODETYPE = domTag('LimitAlertConditionState') # a QName
    Limits = cp.SubElementProperty([domTag('Limits')], valueClass=pmtypes.Range, defaultPyValue=pmtypes.Range())# required, pm:Range
    MonitoredAlertLimits = cp.EnumAttributeProperty('MonitoredAlertLimits',
                                                    defaultPyValue=pmtypes.AlertConditionMonitoredLimits.ALL_OFF,
                                                    enum_cls=pmtypes.AlertConditionMonitoredLimits,
                                                    isOptional=False)
    AutoLimitActivationState = cp.EnumAttributeProperty('AutoLimitActivationState',
                                                        enum_cls=pmtypes.AlertActivation)
    _props=('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateContainer(AbstractStateContainer):
    isMultiState = True
    Handle = cp.StringAttributeProperty('Handle', isOptional=False)
    _props = ('Handle', )    

    def __init__(self, nsmapper, descriptorContainer):
        super(AbstractMultiStateContainer, self).__init__(nsmapper, descriptorContainer)
        self.Handle = uuid.uuid4().hex  # might be preliminary
        self._handle_is_generated = True

    def update_from_other_container(self, other, skipped_properties=None):
        #     Accept node only if descriptorHandle and Handle match
        if self._handle_is_generated:
            self.Handle = other.Handle
            self._handle_is_generated = False
        elif other.Handle != self.Handle:
            raise RuntimeError(
                   'Update from a node with different handle is not possible! Have "{}", got "{}"'.format(
                    self.Handle, other.Handle))
        super().update_from_other_container(other, skipped_properties)

    def mkStateNode(self, tag, updateDescriptorVersion=True):
        if self.Handle is None:
            self.Handle = uuid.uuid4().hex
        return super().mkStateNode(tag, updateDescriptorVersion)

    def __repr__(self):
        return '{} descriptorHandle="{}" handle="{}" type={}'.format(self.__class__.__name__, self.descriptorHandle, self.Handle, self.NODETYPE)


class AbstractContextStateContainer(AbstractMultiStateContainer):
    isContextState = True
    Validator = cp.SubElementListProperty([domTag('Validator')], cls = pmtypes.InstanceIdentifier)
    Identification = cp.SubElementListProperty([domTag('Identification')], cls = pmtypes.InstanceIdentifier)
    ContextAssociation = cp.EnumAttributeProperty('ContextAssociation',
                                                  enum_cls=pmtypes.ContextAssociation,
                                                  impliedPyValue=pmtypes.ContextAssociation.NO_ASSOCIATION)
    BindingMdibVersion = cp.IntegerAttributeProperty('BindingMdibVersion') 
    UnbindingMdibVersion = cp.IntegerAttributeProperty('UnbindingMdibVersion') 
    BindingStartTime = cp.TimestampAttributeProperty('BindingStartTime') # time.time() value (float)
    BindingEndTime = cp.TimestampAttributeProperty('BindingEndTime') # time.time() value (float)
    _props = ('Validator', 'Identification', 'ContextAssociation', 'BindingMdibVersion', 'UnbindingMdibVersion', 'BindingStartTime', 'BindingEndTime')    


class LocationContextStateContainer(AbstractContextStateContainer):
    class T_Location_Detail(pmtypes.PropertyBasedPMType):
        PoC = cp.StringAttributeProperty('PoC')
        Room = cp.StringAttributeProperty('Room')
        Bed = cp.StringAttributeProperty('Bed')
        Facility = cp.StringAttributeProperty('Facility')
        Building = cp.StringAttributeProperty('Building')
        Floor = cp.StringAttributeProperty('Floor')
        _props = ('PoC', 'Room', 'Bed', 'Facility', 'Building', 'Floor')

    NODETYPE = domTag('LocationContextState')
    LocationDetail = cp.SubElementProperty([domTag('LocationDetail')],
                                           valueClass=T_Location_Detail,
                                           defaultPyValue=T_Location_Detail(),
                                           isOptional=True)
    _props = ('LocationDetail', )

    def updateFromSdcLocation(self, sdc_location, bicepsSchema):
        self.LocationDetail.PoC = sdc_location.poc
        self.LocationDetail.Room = sdc_location.rm
        self.LocationDetail.Bed = sdc_location.bed
        self.LocationDetail.Facility = sdc_location.fac
        self.LocationDetail.Building = sdc_location.bld
        self.LocationDetail.Floor = sdc_location.flr
        self.ContextAssociation = pmtypes.ContextAssociation.ASSOCIATED

        extensionString = self._mkExtensionstring(sdc_location)
        if not extensionString:
            # schema does not allow extension string of zero length
            extensionString = None
        self.Identification = [pmtypes.InstanceIdentifier(root=sdc_location.root, extensionString=extensionString)]
        self.updateNode()

    def _mkExtensionstring(self, sdcLocation):
        return sdcLocation.mkExtensionStringSdc()

    @classmethod
    def fromSdcLocation(cls, nsmapper, descriptorContainer, handle, sdc_location, bicepsSchema):
        obj = cls(nsmapper, descriptorContainer)
        obj.Handle = handle
        obj.updateFromSdcLocation(sdc_location, bicepsSchema)
        return obj


class PatientContextStateContainer(AbstractContextStateContainer):

    NODETYPE = domTag('PatientContextState')
    CoreData = cp.SubElementProperty([domTag('CoreData')],
                                           valueClass=pmtypes.PatientDemographicsCoreData,
                                           defaultPyValue=pmtypes.PatientDemographicsCoreData(),
                                           isOptional=True)
    _props = ('CoreData', )


class WorkflowContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('WorkflowContextState')
    WorkflowDetail = cp.SubElementProperty([domTag('WorkflowDetail')], valueClass=pmtypes.WorkflowDetail)
    _props = ('WorkflowDetail',)


class OperatorContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('OperatorContextState')
    OperatorDetails = cp.SubElementProperty([domTag('OperatorDetails')], valueClass=pmtypes.BaseDemographics) #optional
    _props = ('OperatorDetails',)


class MeansContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('MeansContextState')
    # class has no own members


class EnsembleContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('EnsembleContextState')
    # class has no own members


# mapping of states: xsi:type information to classes
# find all classes in this module that have a member "NODETYPE"
classes = inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__ )
classes_with_NODETYPE = [c[1] for c in classes if hasattr(c[1], 'NODETYPE') and c[1].NODETYPE is not None]
# make a dictionary from found classes: (Key is NODETYPE, value is the class itself
_state_lookup_by_type = dict([(c.NODETYPE, c) for c in classes_with_NODETYPE])


def getContainerClass(qNameType):
    '''
    @param qNameType: a QName instance
    '''
    return _state_lookup_by_type.get(qNameType)

    
