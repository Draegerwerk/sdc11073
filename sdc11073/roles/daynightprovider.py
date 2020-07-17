from .. import namespaces
from .. import sdcdevice
from .. import pmtypes
from . import providerbase


class GenericSDCDayNightProvider(providerbase.ProviderRole):
    """ A Provider for testing purpose.
    Creates a DN_VMD/DN_CHAN/DN_METRIC entry in mdib, and adds a SetStringOperation with handle= "DN_SET"
    """
    def makeOperationInstance(self, operationDescriptorContainer):
        return None

    def makeMissingOperations(self):
        mdsContainer = self._mdib.descriptions.NODETYPE.getOne(namespaces.domTag('MdsDescriptor'))
        return self._addDayNightHandling(mdsContainer)

    def _addDayNightHandling(self, mdsContainer, safetyClassification=pmtypes.SafetyClassification.INF):
        ops = []
        ''' create DN_VMD/AP_CHANNEL/AP_METRIC Metrics and add operation to set it'''
        self._mdib.rmDescriptorHandleAll('DN_VMD')  # just to be safe: delete it if it already exists
        vmdContainer = self._mdib.createVmdDescriptorContainer(handle='DN_VMD',
                                                               parentHandle=mdsContainer.handle,
                                                               codedValue=pmtypes.PrivateCodedValue('DN_VMD'),
                                                               safetyClassification=safetyClassification)
        channelContainer = self._mdib.createChannelDescriptorContainer(handle='DN_CHAN',
                                                                       parentHandle=vmdContainer.handle,
                                                                       codedValue=pmtypes.PrivateCodedValue('DN_CHAN'),
                                                                       safetyClassification=safetyClassification)
        metricContainer = self._mdib.createEnumStringMetricDescriptorContainer(handle='DN_METRIC',
                                                                               parentHandle=channelContainer.handle,
                                                                               codedValue=pmtypes.PrivateCodedValue(
                                                                                   'DN_METRIC'),
                                                                               safetyClassification=safetyClassification,
                                                                               metricAvailability=pmtypes.MetricAvailability.INTERMITTENT,
                                                                               metricCategory=pmtypes.MetricCategory.SETTING,
                                                                               unit=pmtypes.Units.UnitLess,
                                                                               allowedValues=[
                                                                                   pmtypes.AllowedValue('Day'),
                                                                                   pmtypes.AllowedValue('Night'),
                                                                                   pmtypes.AllowedValue('DayDark')])
        dn_operation = self._mkOperation(sdcdevice.sco.SetStringOperation,
                                         handle='DN_SET',
                                         operationTargetHandle=metricContainer.handle,
                                         codedValue=pmtypes.PrivateCodedValue('DN_SET'),
                                         currentArgumentHandler=self._setString)
        ops.append(dn_operation)
        return ops
