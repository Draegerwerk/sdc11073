import time
import uuid
import sys
import inspect
import copy
from .containerbase import ContainerBase
from ..namespaces import domTag
from .. import pmtypes 
from . import containerproperties as cp


class AbstractStateContainer(ContainerBase):
    NODENAME = domTag('State')

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
    DescriptorVersion = cp.IntegerAttributeProperty('DescriptorVersion', defaultPyValue=0) # an integer
    StateVersion = cp.IntegerAttributeProperty('StateVersion', defaultPyValue=0) # an integer
    _props=('ext_Extension', 'DescriptorVersion', 'StateVersion')

    stateVersion = StateVersion   # lower case for backwards compatibility
    
    def __init__(self, nsmapper, descriptorContainer, node=None):
        self.descriptorContainer = descriptorContainer
        self.descriptorHandle = descriptorContainer.handle
        super(AbstractStateContainer, self).__init__(nsmapper, node)

        if node is None:
            self.DescriptorVersion = descriptorContainer.DescriptorVersion

    @property
    def nodeName(self):
        return self.NODENAME

    def updateNode(self):
        self.node = self.mkStateNode()


    def mkStateNode(self, tag=None, updateDescriptorVersion=True):
        if updateDescriptorVersion:
            self.updateDescriptorVersion()
        node = super(AbstractStateContainer, self).mkNode(tag, setXsiType=True)
        node.set('DescriptorHandle', self.descriptorHandle)
        return node


    def updateFromNode(self, node):
        ''' update self.node with node, and set members.
        Accept node only if descriptorHandle matches'''
        descriptorHandle = node.get('DescriptorHandle')
        if self.descriptorHandle is not None and descriptorHandle != self.descriptorHandle:
            raise RuntimeError(
                'Update from a node with different descriptor handle is not possible! Have "{}", got "{}"'.format(
                    self.descriptorHandle, descriptorHandle))
        super(AbstractStateContainer, self)._updateFromNode(node)
        self.node = node

    def updateFromOtherContainer(self, other, skippedProperties=None):
        if other.__class__ != self.__class__:
            raise RuntimeError('Update from a node with different type is not possible! Have "{}", got "{}"'.format(self.__class__.__name__, other.__class__.__name__))
        if other.descriptorHandle != self.descriptorHandle:
            raise RuntimeError('Update from a node with different descriptor handle is not possible! Have "{}", got "{}"'.format(self.descriptorHandle, other.descriptorHandle))

        # update all ContainerProperties
        if skippedProperties is None:
            skippedProperties = []
        self.node = other.node
        for prop_name, _ in self._sortedContainerProperties():
            if prop_name not in skippedProperties:
                new_value = getattr(other, prop_name)
                setattr(self, prop_name, new_value)

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
    OperatingMode = cp.NodeAttributeProperty('OperatingMode', defaultPyValue=pmtypes.OperatingMode.ENABLED)
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


class SetContextStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetContextStateOperationState') # a QName


class SetMetricStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetMetricStateOperationState') # a QName


class SetComponentStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetComponentStateOperationState') # a QName


class SetAlertStateOperationStateContainer(AbstractOperationStateContainer):
    NODETYPE = domTag('SetAlertStateOperationState') # a QName



class AbstractMetricStateContainer_Base(AbstractStateContainer):
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
            self._MetricValue = self.__class__._MetricValue.valueClass(self.nsmapper) #pylint: disable=protected-access
            return self._MetricValue
        else:
            raise RuntimeError('State (handle="{}") already has a metric value'.format(self.handle))

    def mkCopy(self, copy_node=True):
        copied = super().mkCopy(copy_node)
        copied._MetricValue = copy.deepcopy(self._MetricValue)
        return copied


class AbstractMetricStateContainer(AbstractMetricStateContainer_Base):
    BodySite = cp.SubElementListProperty([domTag('BodySite')], cls=pmtypes.CodedValue)
    PhysicalConnector = cp.SubElementProperty([domTag('PhysicalConnector')], valueClass=pmtypes.PhysicalConnectorInfo) # optional
    ActivationState = cp.NodeAttributeProperty('ActivationState', impliedPyValue=pmtypes.ComponentActivation.ON)
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



class EnumStringMetricStateContainer(AbstractMetricStateContainer):
    NODETYPE = domTag('EnumStringMetricState')
    _MetricValue = cp.SubElementProperty([domTag('MetricValue')], valueClass=pmtypes.StringMetricValue)
    _props = ('_MetricValue',)



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
        return '{} Version={} descriptorHandle="{}" Activation="{}" Samples={}'.format(
            self.__class__.__name__, self.StateVersion, self.descriptorHandle, self.ActivationState, samplesCount)


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

    ActivationState = cp.NodeAttributeProperty('ActivationState')  # pmtypes.ComponentActivation
    OperatingHours = cp.IntegerAttributeProperty('OperatingHours')  # optional, unsigned int
    OperatingCycles = cp.IntegerAttributeProperty('OperatingCycles')  # optional, unsigned int
    _props = ('CalibrationInfo', 'NextCalibration', 'PhysicalConnector', 'ActivationState', 'OperatingHours', 'OperatingCycles')


class MdsStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('MdsState')
    OperatingMode = cp.NodeAttributeProperty('OperatingMode',
                                             defaultPyValue=pmtypes.MdsOperatingMode.NORMAL)  # pmtypes.MdsOperatingMode
    Lang = cp.NodeAttributeProperty('Lang', impliedPyValue='en')
    _props = ('OperatingMode', 'Lang')


class ScoStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ScoState')
    OperationGroup = cp.SubElementListProperty([domTag('OperationGroup')], cls=pmtypes.OperationGroup)
    InvocationRequested = cp.NodeAttributeListProperty('InvocationRequested')  # pm:OperationRef
    InvocationRequired = cp.NodeAttributeListProperty('InvocationRequired')  # pm:OperationRef
    _props = ('OperationGroup', 'InvocationRequested', 'InvocationRequired')


class VmdStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('VmdState')


class ChannelStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ChannelState')


class ClockStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('ClockState')
    ActiveSyncProtocol = cp.SubElementProperty([domTag('ActiveSyncProtocol')], valueClass=pmtypes.CodedValue)
    ReferenceSource = cp.SubElementListProperty([domTag('ReferenceSource')], cls=pmtypes.ElementWithTextOnly)
    DateAndTime = cp.CurrentTimestampAttributeProperty('DateAndTime')
    RemoteSync = cp.BooleanAttributeProperty('RemoteSync', defaultPyValue=True)
    Accuracy = cp.DecimalAttributeProperty('Accuracy')
    LastSet = cp.TimestampAttributeProperty('LastSet')
    TimeZone = cp.NodeAttributeProperty('TimeZone') # optional, a time zone string
    CriticalUse = cp.BooleanAttributeProperty('CriticalUse', impliedPyValue=False) # optional
    _props = ('ActiveSyncProtocol', 'ReferenceSource', 'DateAndTime', 'RemoteSync', 'Accuracy', 'LastSet', 'TimeZone', 'CriticalUse')

    def diff(self, other):
        """ compares all properties EXCEPT DateAndTime.
        BICEPS says:
        "As the current date/time changes at a high frequency, a change of this value SHALL NOT cause
        an update of the state version unless it has been synchronized either remotely or manually."
        returns a list of strings that describe differences"""
        return super().diff(other, ignore_property_names=['DateAndTime'])


class SystemContextStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('SystemContextState')


class BatteryStateContainer(AbstractDeviceComponentStateContainer):
    NODETYPE = domTag('BatteryState')
    CapacityRemaining = cp.SubElementProperty([domTag('CapacityRemaining')], valueClass=pmtypes.Measurement) #optional
    Voltage = cp.SubElementProperty([domTag('Voltage')], valueClass=pmtypes.Measurement) #optional
    Current = cp.SubElementProperty([domTag('Current')], valueClass=pmtypes.Measurement) #optional
    Temperature = cp.SubElementProperty([domTag('Temperature')], valueClass=pmtypes.Measurement) #optional
    RemainingBatteryTime = cp.SubElementProperty([domTag('RemainingBatteryTime')], valueClass=pmtypes.Measurement) #optional
    ChargeStatus = cp.NodeAttributeProperty('ChargeStatus') # Ful, ChB, DisChB, DEB
    ChargeCycles = cp.IntegerAttributeProperty('ChargeCycles') # Number of charge/discharge cycles.
    _props = ('CapacityRemaining', 'Voltage', 'Current', 'Temperature', 'RemainingBatteryTime', 'ChargeStatus', 'ChargeCycles')


class AbstractAlertStateContainer(AbstractStateContainer):
    isAlertState = True
    ActivationState = cp.NodeAttributeProperty('ActivationState', defaultPyValue=pmtypes.AlertActivation.ON)
    _props=('ActivationState', )


class AlertSystemStateContainer(AbstractAlertStateContainer):
    NODETYPE = domTag('AlertSystemState')
    SystemSignalActivation = cp.SubElementListProperty([domTag('SystemSignalActivation')],
                                                       cls=pmtypes.SystemSignalActivation)
    LastSelfCheck = cp.TimestampAttributeProperty('LastSelfCheck')
    SelfCheckCount = cp.IntegerAttributeProperty('SelfCheckCount')
    PresentPhysiologicalAlarmConditions = cp.NodeAttributeListProperty('PresentPhysiologicalAlarmConditions')# pm:AlertConditionReference, List of HANDLE references
    PresentTechnicalAlarmConditions = cp.NodeAttributeListProperty('PresentTechnicalAlarmConditions')# pm:AlertConditionReference, List of HANDLE references
    _props=('SystemSignalActivation', 'LastSelfCheck', 'SelfCheckCount', 'PresentPhysiologicalAlarmConditions', 'PresentTechnicalAlarmConditions')

    def __repr__(self):
        return '{} descriptorHandle="{}" StateVersion={} LastSelfCheck={} SelfCheckCount={}'.format(self.__class__.__name__,
                                                                           self.descriptorHandle,
                                                                           self.StateVersion,
                                                                           self.LastSelfCheck,
                                                                           self.SelfCheckCount)


class AlertSignalStateContainer(AbstractAlertStateContainer):
    isAlertSignal = True
    NODETYPE = domTag('AlertSignalState')
    Presence = cp.NodeAttributeProperty('Presence', impliedPyValue=pmtypes.AlertSignalPresence.OFF)
    Location = cp.NodeAttributeProperty('Location', impliedPyValue='Loc') # 'Loc', 'Rem'
    Slot = cp.IntegerAttributeProperty('Slot')             # unsigned int
    ActualSignalGenerationDelay = cp.DurationAttributeProperty('ActualSignalGenerationDelay') # xsd:duration
    _props = ('Presence', 'Location', 'Slot', 'ActualSignalGenerationDelay')

    def __init__(self, *args, **kwargs):
        super(AlertSignalStateContainer, self).__init__(*args, **kwargs)
        self.lastUpdated = time.time()

        if self.descriptorContainer.SignalDelegationSupported:
            # Delegable signals should have location Remote according to BICEPS
            self.Location = 'Rem'


class AlertConditionStateContainer(AbstractAlertStateContainer):
    isAlertCondition = True
    NODETYPE = domTag('AlertConditionState')
    ActualConditionGenerationDelay = cp.DurationAttributeProperty('ActualConditionGenerationDelay')# xsd:duration
    ActualPriority = cp.NodeAttributeProperty('ActualPriority') # optional, pmtypes.AlertConditionPriority ('Lo', 'Me', 'Hi', 'None')
    Rank = cp.NodeAttributeProperty('Rank', valueConverter=cp.IntegerConverter) # Integer
    DeterminationTime = cp.TimestampAttributeProperty('DeterminationTime') # Integer
    Presence = cp.NodeAttributeProperty('Presence', valueConverter=cp.BooleanConverter, impliedPyValue=False)
    _props=('ActualConditionGenerationDelay', 'ActualPriority', 'Rank', 'DeterminationTime', 'Presence')


class LimitAlertConditionStateContainer(AlertConditionStateContainer):
    NODETYPE = domTag('LimitAlertConditionState') # a QName
    Limits = cp.SubElementProperty([domTag('Limits')], valueClass=pmtypes.Range, defaultPyValue=pmtypes.Range())# required, pm:Range
    MonitoredAlertLimits = cp.NodeAttributeProperty('MonitoredAlertLimits', defaultPyValue=pmtypes.AlertConditionMonitoredLimits.ALL_OFF) # required, pm:AlertConditionMonitoredLimits
    AutoLimitActivationState = cp.NodeAttributeProperty('AutoLimitActivationState') # optional, pm:AlertActivation
    _props=('Limits', 'MonitoredAlertLimits', 'AutoLimitActivationState')


class AbstractMultiStateContainer(AbstractStateContainer):
    isMultiState = True
    Handle = cp.NodeAttributeProperty('Handle') # required
    _props = ('Handle', )    

    def __init__(self, nsmapper, descriptorContainer, node=None):
        super(AbstractMultiStateContainer, self).__init__(nsmapper, descriptorContainer, node)
        if node is None:
            # auto- generate a handle
            self.Handle = uuid.uuid4().hex

    def updateFromNode(self, node):
        ''' update self.node with node, and set members.
        Accept node only if descriptorHandle and Handle match'''
        if self.Handle is not None: # if self.handle is None, this is an initial init from node, no check for equality.
            handle = node.get('Handle')

            if handle != self.Handle:
                raise RuntimeError(
                    'Update from a node with different handle is not possible! Have "{}", got "{}"'.format(
                        self.Handle, handle))
        super(AbstractMultiStateContainer, self).updateFromNode(node)

    def __repr__(self):
        return '{} descriptorHandle="{}" handle="{}" type={}'.format(self.__class__.__name__, self.descriptorHandle, self.Handle, self.NODETYPE)


class AbstractContextStateContainer(AbstractMultiStateContainer):
    isContextState = True
    Validator = cp.SubElementListProperty([domTag('Validator')], cls = pmtypes.InstanceIdentifier)
    Identification = cp.SubElementListProperty([domTag('Identification')], cls = pmtypes.InstanceIdentifier)
    ContextAssociation = cp.NodeAttributeProperty('ContextAssociation', impliedPyValue=pmtypes.ContextAssociation.NO_ASSOCIATION)
    BindingMdibVersion = cp.IntegerAttributeProperty('BindingMdibVersion') 
    UnbindingMdibVersion = cp.IntegerAttributeProperty('UnbindingMdibVersion') 
    BindingStartTime = cp.TimestampAttributeProperty('BindingStartTime') # time.time() value (float)
    BindingEndTime = cp.TimestampAttributeProperty('BindingEndTime') # time.time() value (float)
    _props = ('Validator', 'Identification', 'ContextAssociation', 'BindingMdibVersion', 'UnbindingMdibVersion', 'BindingStartTime', 'BindingEndTime')    


class LocationContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('LocationContextState')
    lc = domTag('LocationDetail')
    PoC = cp.NodeAttributeProperty('PoC', [lc])
    Room = cp.NodeAttributeProperty('Room', [lc])
    Bed = cp.NodeAttributeProperty('Bed', [lc])
    Facility = cp.NodeAttributeProperty('Facility', [lc])
    Building = cp.NodeAttributeProperty('Building', [lc])
    Floor = cp.NodeAttributeProperty('Floor', [lc])
    _props = ('PoC', 'Room', 'Bed', 'Facility', 'Building', 'Floor')

    def updateFromSdcLocation(self, sdc_location):
        self.PoC = sdc_location.poc
        self.Room = sdc_location.rm
        self.Bed = sdc_location.bed
        self.Facility = sdc_location.fac
        self.Building = sdc_location.bld
        self.Floor = sdc_location.flr
        self.ContextAssociation = 'Assoc'

        extensionString = self._mkExtensionstring(sdc_location)
        if not extensionString:
            # schema does not allow extension string of zero length
            extensionString = None
        self.Identification = [pmtypes.InstanceIdentifier(root=sdc_location.root, extensionString=extensionString)]
        self.updateNode()

    def _mkExtensionstring(self, sdcLocation):
        return sdcLocation.mkExtensionStringSdc()

    @classmethod
    def fromSdcLocation(cls, nsmapper, descriptorContainer, handle, sdc_location):
        obj = cls(nsmapper, descriptorContainer)
        obj.Handle = handle
        obj.updateFromSdcLocation(sdc_location)
        return obj


class PatientContextStateContainer(AbstractContextStateContainer):
    NODETYPE = domTag('PatientContextState')
    cd = domTag('CoreData') # a shortcut
    Givenname = cp.NodeTextProperty([cd, domTag('Givenname')])
    Middlename = cp.NodeTextProperty([cd, domTag('Middlename')])
    Familyname = cp.NodeTextProperty([cd, domTag('Familyname')])
    Birthname = cp.NodeTextProperty([cd, domTag('Birthname')])
    Title = cp.NodeTextProperty([cd, domTag('Title')])
    Sex = cp.NodeTextProperty([cd, domTag('Sex')])
    PatientType = cp.NodeTextProperty([cd, domTag('PatientType')])
    DateOfBirth = cp.DateOfBirthProperty([cd, domTag('DateOfBirth')])
    Height = cp.SubElementProperty([cd, domTag('Height')], valueClass=pmtypes.Measurement)
    Weight = cp.SubElementProperty([cd, domTag('Weight')], valueClass=pmtypes.Measurement)
    Race = cp.SubElementProperty([cd, domTag('Race')], valueClass=pmtypes.CodedValue)
    _props = ('Givenname', 'Middlename', 'Familyname', 'Birthname', 'Title', 'Sex', 'PatientType', 'DateOfBirth', 'Height', 'Weight', 'Race')

    def setBirthdate(self, dateTimeOfBirth_string):
        ''' this method accepts a string, format acc. to XML Schema: xsd:dateTime, xsd:date, xsd:gYearMonth or xsd:gYear
        Internally it holds it as a datetime object, so specific formatting of the dateTimeOfBirth_string will be lost.'''
        if not dateTimeOfBirth_string:
            self.DateOfBirth = None
        else:
            datetime = cp.DateOfBirthProperty.mk_value_object(dateTimeOfBirth_string)
            self.DateOfBirth = datetime


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

    
