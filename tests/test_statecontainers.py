# coding: utf-8
from __future__ import print_function
from __future__ import absolute_import
import unittest
import datetime
from lxml import etree as etree_
import copy
import sdc11073.mdib.statecontainers as statecontainers
import sdc11073.mdib.descriptorcontainers as descriptorcontainers
import sdc11073.namespaces as namespaces
import sdc11073.pmtypes as pmtypes
import sdc11073.xmlparsing as xmlparsing
import sdc11073.mdib.containerproperties as containerproperties
from sdc11073.location import SdcLocation
from sdc11073.definitions_sdc import SDC_v1_Definitions

#pylint: disable=protected-access

class TestStateContainers(unittest.TestCase):

    def setUp(self):
        self.nsmapper = namespaces.DocNamespaceHelper()
        self.dc = descriptorcontainers.AbstractDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456')
        self.dc.DescriptorVersion = 42


    def test_AbstractStateContainer(self):
        sc = statecontainers.AbstractStateContainer(nsmapper=self.nsmapper, 
                                                    descriptorContainer=self.dc, 
                                                    node=None)
        # constructor shall create a node
        self.assertEqual(sc.nodeName, namespaces.domTag('State'))

        #initially the state version shall be 0, and DescriptorVersion shall be set
        self.assertEqual(sc.StateVersion, 0)
        self.assertEqual(sc.DescriptorVersion, self.dc.DescriptorVersion)
        
        # verify that mkStateNode() also updates changed Descriptorversion
        self.dc.DescriptorVersion +=1
        sc.mkStateNode()
        self.assertEqual(sc.DescriptorVersion, self.dc.DescriptorVersion)

        # verify incrementState works ay expected
        sc.incrementState()
        self.assertEqual(sc.StateVersion, 1)
        node = sc.mkStateNode()
        self.assertEqual(node.get('StateVersion'), '1')
        
        # test updateFromNode
        node = etree_.Element(namespaces.domTag('State'),
                              attrib={'StateVersion':'2',
                                      'DescriptorHandle': self.dc.handle})
        sc.updateFromNode(node)
        self.assertEqual(sc.StateVersion, 2)
        self.assertEqual(sc.node.get('StateVersion'), '2')
        
        node = etree_.Element(namespaces.domTag('State'),
                              attrib={'StateVersion':'3',
                                      'DescriptorHandle':'something_completely_different'})
        self.assertRaises(RuntimeError, sc.updateFromNode, node)

        #test creation from node
        sc2 = statecontainers.AbstractStateContainer(nsmapper=self.nsmapper, 
                                                     descriptorContainer=self.dc, 
                                                     node=copy.deepcopy(sc.node)) # sc2 might change node, therfore give it a deep copy
        self._verifyAbstractStateContainerDataEqual(sc, sc2)
        
        # test update from Node
        sc.DescriptorVersion += 1 
        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)


    def _verifyAbstractStateContainerDataEqual(self, sc1, sc2):
        self.assertEqual(sc1.DescriptorVersion, sc2.DescriptorVersion)
        self.assertEqual(sc1.StateVersion, sc2.StateVersion)
        self.assertEqual(sc1.nodeName, sc2.nodeName)


    def test_AbstractOperationStateContainer(self):
        sc = statecontainers.AbstractOperationStateContainer(nsmapper=self.nsmapper, 
                                                    descriptorContainer=self.dc, 
                                                    node=None)
        self.assertIsNotNone(sc.OperatingMode) # this is a required attribute
        
        #test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.AbstractOperationStateContainer(nsmapper=self.nsmapper, 
                                                          descriptorContainer=self.dc, 
                                                          node=node)
        self.assertIsNotNone(sc2.OperatingMode)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

        #test update from node
        sc.OperatingMode = pmtypes.OperatingMode.NA
        self.assertEqual(sc.OperatingMode, pmtypes.OperatingMode.NA)
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self.assertEqual(sc2.OperatingMode, pmtypes.OperatingMode.NA)



    def test_AbstractMetricStateContainer_Final(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456')
        sc = statecontainers.NumericMetricStateContainer(nsmapper=self.nsmapper,
                                                            descriptorContainer=dc,
                                                            node=None)

        self.assertEqual(sc.ActivationState, 'On')
        for value in ('foo', 'bar'):
            sc.ActivationState = value
            self.assertEqual(sc.ActivationState, value)
            node = sc.mkStateNode()
            self.assertEqual(node.get('ActivationState'), value)

        self.assertEqual(sc.ActiveDeterminationPeriod, None)
        for value in (21, 42):
            sc.ActiveDeterminationPeriod = value
            self.assertEqual(sc.ActiveDeterminationPeriod, value)
            node = sc.mkStateNode()
            self.assertEqual(node.get('ActiveDeterminationPeriod'),
                             containerproperties.DurationConverter.toXML(value))
        sc.BodySite = [pmtypes.CodedValue('ABC')]
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)

        # test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.NumericMetricStateContainer(nsmapper=self.nsmapper,
                                                          descriptorContainer=dc,
                                                          node=node)
        self.assertEqual(sc.ActivationState, sc2.ActivationState)
        self.assertEqual(sc.ActiveDeterminationPeriod, sc2.ActiveDeterminationPeriod)
        self.assertEqual(sc.BodySite, sc2.BodySite)
        self.assertEqual(sc.PhysicalConnector, sc2.PhysicalConnector)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

        # test update from Node
        sc.ActivationState = 'something else'
        sc.ActiveDeterminationPeriod += 1
        sc.BodySite = [pmtypes.CodedValue('DEF')]
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('DEF')], 2)
        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self.assertEqual(sc.ActivationState, sc2.ActivationState)
        self.assertEqual(sc.BodySite, sc2.BodySite)
        self.assertEqual(sc.PhysicalConnector, sc2.PhysicalConnector)
        self.assertEqual(sc.ActiveDeterminationPeriod, sc2.ActiveDeterminationPeriod)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)


    def test_NumericMetricStateContainer(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456')
        sc = statecontainers.NumericMetricStateContainer(nsmapper=self.nsmapper,
                                                    descriptorContainer=dc,
                                                    node=None)
        sc.mkMetricValue()
        self.assertTrue(isinstance(sc.metricValue, pmtypes.NumericMetricValue))
        sc.metricValue.Value = 42.21
        sc.metricValue.StartTime = 1234567.21
        sc.metricValue.StopTime = sc.metricValue.StartTime +10
        sc.metricValue.DeterminationTime = sc.metricValue.StartTime +20
        sc.metricValue.Validity = 'Vld'
        sc.ActiveAveragingPeriod = 42
        sc.PhysiologicalRange = [pmtypes.Range(1, 2, 3, 4, 5), pmtypes.Range(10, 20, 30, 40, 50)]
        node = sc.mkStateNode()
        
        #test creation from node
        sc2 = statecontainers.NumericMetricStateContainer(nsmapper=self.nsmapper, 
                                                          descriptorContainer=dc,
                                                          node=node)
        # verify also that mkStateNode on receiving sc does not change anything
        for dummy in range(1):
            self.assertEqual(sc.metricValue.Value, sc2.metricValue.Value)
            self.assertEqual(sc.metricValue.StartTime, sc2.metricValue.StartTime)
            self.assertEqual(sc.metricValue.StopTime, sc2.metricValue.StopTime)
            self.assertEqual(sc.metricValue.DeterminationTime, sc2.metricValue.DeterminationTime)
            self.assertEqual(sc.metricValue.Validity, sc2.metricValue.Validity)
            self.assertEqual(sc.ActiveAveragingPeriod, sc2.ActiveAveragingPeriod)
            self.assertEqual(sc.PhysiologicalRange, sc2.PhysiologicalRange)
            
            self._verifyAbstractStateContainerDataEqual(sc, sc2)
            sc.mkStateNode()

        #test update from node
        sc.metricValue.Value += 1
        sc.incrementState()
        sc.ActiveAveragingPeriod = 24
        sc.PhysiologicalRange[1].Lower =100
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self.assertEqual(sc.metricValue.Value, sc2.metricValue.Value)
        self.assertEqual(sc.ActiveAveragingPeriod, sc2.ActiveAveragingPeriod)
        self.assertEqual(sc.PhysiologicalRange, sc2.PhysiologicalRange)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)


    def test_StringMetricStateContainer(self):
        dc = descriptorcontainers.StringMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456')
        sc = statecontainers.StringMetricStateContainer(nsmapper=self.nsmapper,
                                                    descriptorContainer=dc,
                                                    node=None)
        sc.mkMetricValue()
        self.assertTrue(isinstance(sc.metricValue, pmtypes.StringMetricValue))


    def test_EnumStringMetricStateContainer(self):
        dc = descriptorcontainers.EnumStringMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                   nodeName='MyDescriptor',
                                                                   handle='123',
                                                                   parentHandle='456')

        sc = statecontainers.EnumStringMetricStateContainer(nsmapper=self.nsmapper,
                                                            descriptorContainer=dc,
                                                            node=None)
        sc.mkMetricValue()
        self.assertTrue(isinstance(sc.metricValue, pmtypes.StringMetricValue))


    def test_RealTimeSampleArrayMetricStateContainer(self):
        dc = descriptorcontainers.RealTimeSampleArrayMetricDescriptorContainer(nsmapper=self.nsmapper,
                                                                               nodeName='MyDescriptor',
                                                                               handle='123',
                                                                               parentHandle='456')

        def verifyEqual(origin, copied):
            self.assertEqual(copied.metricValue.Samples, origin.metricValue.Samples)
            self.assertEqual(copied.metricValue.DeterminationTime, origin.metricValue.DeterminationTime)
            self.assertEqual(copied.metricValue.Annotation, origin.metricValue.Annotation)
            self.assertEqual(copied.metricValue.ApplyAnnotations, origin.metricValue.ApplyAnnotations)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
        
        sc = statecontainers.RealTimeSampleArrayMetricStateContainer(nsmapper=self.nsmapper, 
                                                                     descriptorContainer=dc,
                                                                     node=None)
        sc.mkMetricValue()
        self.assertEqual(sc.nodeName, namespaces.domTag('State'))
        self.assertTrue(isinstance(sc.metricValue, pmtypes.SampleArrayValue))
        
        sc.metricValue.Samples = [1,2,3,4,5.5]
        sc.metricValue.DeterminationTime = 1234567
        sc.metricValue.Annotations = []
        sc.metricValue.ApplyAnnotations = []
        sc.ActivationState = 'act'

        #test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.RealTimeSampleArrayMetricStateContainer(nsmapper=self.nsmapper, 
                                                                     descriptorContainer=dc,
                                                                     node=node)
        verifyEqual(sc, sc2)
        
        #test update from node
        sc.metricValue.Samples = [5.5, 6.6]
        sc.metricValue.DeterminationTime = 2345678
        sc.metricValue.Annotations = [pmtypes.Annotation(pmtypes.CodedValue('a','b'))]
        sc.metricValue.ApplyAnnotations = [pmtypes.ApplyAnnotation(1,2)]

        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_AbstractDeviceComponentStateContainer_Final(self):

        def verifyEqual(origin, copied):
            self.assertEqual(copied.ActivationState, origin.ActivationState)
            self.assertEqual(copied.OperatingHours, origin.OperatingHours)
            self.assertEqual(copied.OperatingCycles, origin.OperatingCycles)
            self.assertEqual(copied.PhysicalConnector, origin.PhysicalConnector)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.AbstractDeviceComponentStateContainer(nsmapper=self.nsmapper,
                                                                   descriptorContainer=self.dc,
                                                                   node=None)
        self.assertEqual(sc.ActivationState, None)
        self.assertEqual(sc.OperatingHours, None)
        self.assertEqual(sc.OperatingCycles, None)
        self.assertEqual(sc.PhysicalConnector, None)

        sc.ActivationState = 'On'
        sc.OperatingHours = 2.3
        sc.OperatingHours = 4
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)

        # test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.AbstractDeviceComponentStateContainer(nsmapper=self.nsmapper,
                                                                    descriptorContainer=self.dc,
                                                                    node=node)
        verifyEqual(sc, sc2)

        sc.ActivationState = 'Off'
        sc.OperatingHours += 1
        sc.OperatingHours += 1
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('DEF')], 2)
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_MdsStateContainer(self):
        pass


    def test_VmdStateContainer(self):
        pass


    def test_ChannelStateContainer(self):
        pass


    def test_ClockStateContainer(self):
        pass


    def test_AbstractAlertStateContainer(self):
        pass



    def test_AlertSystemStateContainer_final(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.SystemSignalActivation, origin.SystemSignalActivation)
            self.assertEqual(copied.LastSelfCheck, origin.LastSelfCheck)
            self.assertEqual(copied.SelfCheckCount, origin.SelfCheckCount)
            self.assertEqual(copied.PresentPhysiologicalAlarmConditions, origin.PresentPhysiologicalAlarmConditions)
            self.assertEqual(copied.PresentTechnicalAlarmConditions, origin.PresentTechnicalAlarmConditions)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.AlertSystemStateContainer(nsmapper=self.nsmapper,
                                                                   descriptorContainer=self.dc,
                                                                   node=None)
        self.assertEqual(sc.SystemSignalActivation, [])
        self.assertEqual(sc.LastSelfCheck, None)
        self.assertEqual(sc.SelfCheckCount, None)
        self.assertEqual(sc.PresentPhysiologicalAlarmConditions, [])
        self.assertEqual(sc.PresentTechnicalAlarmConditions, [])

        #test creation from node
        sc.SystemSignalActivation = [pmtypes.SystemSignalActivation(manifestation=pmtypes.AlertSignalManifestation.AUD,
                                                                    state=pmtypes.AlertActivation.ON),
                                     pmtypes.SystemSignalActivation(manifestation=pmtypes.AlertSignalManifestation.VIS,
                                                                    state=pmtypes.AlertActivation.ON)
                                     ]
        sc.LastSelfCheck = 1234567
        sc.SelfCheckCount = 3
        sc.PresentPhysiologicalAlarmConditions = ["handle1", "handle2", "handle3"]
        sc.incrementState()
        node = sc.mkStateNode()
        sc2 = statecontainers.AlertSystemStateContainer(nsmapper=self.nsmapper,
                                                                    descriptorContainer=self.dc,
                                                                    node=node)
        verifyEqual(sc, sc2)

        #test update from node
        sc.LastSelfCheck = 12345678
        sc.SelfCheckCount = 4
        sc.PresentPhysiologicalAlarmConditions = ["handle2", "handle3", "handle4"]
        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_AlertConditionStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.ActualPriority, origin.ActualPriority)
            self.assertEqual(copied.Rank, origin.Rank)
            self.assertEqual(copied.DeterminationTime, origin.DeterminationTime)
            self.assertEqual(copied.Presence, origin.Presence)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
            
        sc = statecontainers.AlertConditionStateContainer(nsmapper=self.nsmapper, 
                                                          descriptorContainer=self.dc, 
                                                          node=None)
        self.assertEqual(sc.ActualPriority, None)
        self.assertEqual(sc.Rank, None)
        self.assertEqual(sc.DeterminationTime, None)
        self.assertEqual(sc.Presence, False)

        #test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.AlertConditionStateContainer(nsmapper=self.nsmapper, 
                                                           descriptorContainer=self.dc, 
                                                           node=node)
        verifyEqual(sc, sc2)

        #test update from node
        sc.ActualPriority = 'lo'
        sc.Rank = 3
        sc.DeterminationTime = 1234567
        sc.Presence = True
        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_LimitAlertConditionStateContainer_Final(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Limits, origin.Limits)
            self.assertEqual(copied.MonitoredAlertLimits, origin.MonitoredAlertLimits)
            self.assertEqual(copied.AutoLimitActivationState, origin.AutoLimitActivationState)
            self._verifyAbstractStateContainerDataEqual(copied, origin)


        sc = statecontainers.LimitAlertConditionStateContainer(nsmapper=self.nsmapper,
                                                            descriptorContainer=self.dc,
                                                            node=None)
        self.assertEqual(sc.MonitoredAlertLimits, pmtypes.AlertConditionMonitoredLimits.ALL_OFF)
        self.assertEqual(sc.AutoLimitActivationState, None)

        # test creation from node
        node = sc.mkStateNode()
        sc2 = statecontainers.LimitAlertConditionStateContainer(nsmapper=self.nsmapper,
                                                           descriptorContainer=self.dc,
                                                           node=node)
        verifyEqual(sc, sc2)

        # test update from node
        sc.Limits = pmtypes.Range(lower=5, upper=9, stepWidth=0.1, relativeAccuracy=0.01, absoluteAccuracy=0.001)
        sc.Rank = 3
        sc.DeterminationTime = 1234567
        sc.Presence = True
        sc.incrementState()
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)



    def test_SetStringOperationStateContainer_Final(self):
        sc = statecontainers.SetStringOperationStateContainer(nsmapper=self.nsmapper,
                                                                   descriptorContainer=self.dc,
                                                                   node=None)
        # verify that initial pyValue is an empty list, and that no AllowedValues node is created
        self.assertEqual(sc.AllowedValues, [])
        node = sc.mkStateNode()
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)

        sc2 = statecontainers.SetStringOperationStateContainer(nsmapper=self.nsmapper,
                                                           descriptorContainer=self.dc,
                                                           node=copy.deepcopy(sc.node))
        self.assertEqual(sc2.AllowedValues, [])

        # verify that setting to None is identical to empty list
        sc.AllowedValues = None
        node = sc.mkStateNode()
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)

        # verify that non-empty list creates values in xml and that same list appears in container created from that xml
        sc.AllowedValues = ['a', 'b', 'c']
        node = sc.mkStateNode()
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 1)
        valuesNodes = node.xpath('//dom:Value', namespaces=namespaces.nsmap)
        self.assertEqual(len(valuesNodes), 3)
        sc2 = statecontainers.SetStringOperationStateContainer(nsmapper=self.nsmapper,
                                                           descriptorContainer=self.dc,
                                                           node=node)
        self.assertEqual(sc2.AllowedValues, ['a', 'b', 'c'])

        # verify that setting it back to None clears all data
        sc.AllowedValues = None
        node = sc.mkStateNode()
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)
        sc2 = statecontainers.SetStringOperationStateContainer(nsmapper=self.nsmapper,
                                                           descriptorContainer=self.dc,
                                                           node=node)
        self.assertEqual(sc2.AllowedValues, [])


    def test_AbstractMultiStateContainer(self):
        pass


    def test_AbstractContextStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.ContextAssociation, origin.ContextAssociation)
            self.assertEqual(copied.BindingMdibVersion, origin.BindingMdibVersion)
            self.assertEqual(copied.UnbindingMdibVersion, origin.UnbindingMdibVersion)
            self.assertEqual(copied.BindingStartTime, origin.BindingStartTime)
            self.assertEqual(copied.BindingEndTime, origin.BindingEndTime)
            self.assertEqual(copied.Validator, origin.Validator)
            self.assertEqual(copied.Identification, origin.Identification)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
        
        sc = statecontainers.AbstractContextStateContainer(nsmapper=self.nsmapper, 
                                                           descriptorContainer=self.dc, 
                                                           node=None)
        self.assertEqual(sc.ContextAssociation, 'No')
        self.assertEqual(sc.BindingMdibVersion, None)
        self.assertEqual(sc.UnbindingMdibVersion, None)
        self.assertEqual(sc.BindingStartTime, None)
        self.assertEqual(sc.BindingEndTime, None)
        self.assertEqual(sc.Validator, [])
        self.assertEqual(sc.Identification, [])

        idents = [pmtypes.InstanceIdentifier(root='abc', 
                                             type_codedValue=pmtypes.CodedValue('abc','def'), 
                                             identifierNames=[pmtypes.LocalizedText('ABC')], 
                                             extensionString='123')]
        sc.Identification = idents
        self.assertEqual(sc.Identification, idents)

        validators = [pmtypes.InstanceIdentifier(root='ABC', 
                                                 type_codedValue=pmtypes.CodedValue('123','456'), 
                                                 identifierNames=[pmtypes.LocalizedText('DEF')], 
                                                 extensionString='321')]
        sc.Validator = validators
        self.assertEqual(sc.Validator, validators)
        
        for value in ('assoc', 'disassoc'):
            sc.ContextAssociation = value
            node = sc.mkStateNode()
            self.assertEqual(node.get('ContextAssociation'), value)
            
        for value in (12345.123, 67890.987):
            sc.BindingStartTime = value
            sc.BindingEndTime = value+1
            node = sc.mkStateNode()
            self.assertEqual(node.get('BindingStartTime'), containerproperties.TimestampConverter.toXML(value))
            self.assertEqual(node.get('BindingEndTime'), containerproperties.TimestampConverter.toXML(value+1))

        for value in (0, 42, 123):
            sc.BindingMdibVersion = value
            sc.UnbindingMdibVersion = value+1
            node = sc.mkStateNode()
            self.assertEqual(node.get('BindingMdibVersion'), containerproperties.IntegerConverter.toXML(value))
            self.assertEqual(node.get('UnbindingMdibVersion'), containerproperties.IntegerConverter.toXML(value+1))

        node = etree_.Element(namespaces.domTag('State'),
                              attrib={'StateVersion':'2',
                                      'DescriptorHandle':'123',
                                      'BindingStartTime':'1234567',
                                      'BindingEndTime':'2345678',
                                      'Handle':sc.Handle})
        sc.updateFromNode(node)
        self.assertEqual(sc.BindingStartTime, 1234.567)
        self.assertEqual(sc.BindingEndTime, 2345.678)
        self.assertEqual(sc.node.get('BindingStartTime'), '1234567')
        self.assertEqual(sc.node.get('BindingEndTime'), '2345678')
        self.assertEqual(sc.Identification, [])
        self.assertEqual(sc.Validator, [])
        
        #test creation from node
        sc.Identification = idents
        sc.Validator = validators
        node = sc.mkStateNode()
        sc2 = statecontainers.AbstractContextStateContainer(nsmapper=self.nsmapper,
                                                                     descriptorContainer=self.dc, 
                                                                     node=node)
        verifyEqual(sc, sc2)


    def test_LocationContextStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Handle, origin.Handle)
            self.assertEqual(copied.PoC, origin.PoC)
            self.assertEqual(copied.Room, origin.Room)
            self.assertEqual(copied.Bed, origin.Bed)
            self.assertEqual(copied.Facility, origin.Facility)
            self.assertEqual(copied.Building, origin.Building)
            self.assertEqual(copied.Floor, origin.Floor)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
            
        sc = statecontainers.LocationContextStateContainer(nsmapper=self.nsmapper, 
                                                           descriptorContainer=self.dc, 
                                                           node=None)
        
        self.assertTrue(sc.Handle is not None)
        self.assertEqual(sc.PoC, None)
        self.assertEqual(sc.Room, None)
        self.assertEqual(sc.Bed, None)
        self.assertEqual(sc.Facility, None)
        self.assertEqual(sc.Building, None)
        self.assertEqual(sc.Floor, None)

        #test creation from empty node
        node = sc.mkStateNode()
        self.assertEqual(node.get('Handle'), sc.Handle)
        print (etree_.tostring(node, pretty_print=True))
        sc2 = statecontainers.LocationContextStateContainer(nsmapper=self.nsmapper, 
                                                            descriptorContainer=self.dc, 
                                                            node=node)
        verifyEqual(sc, sc2)
        
        sc.Handle = 'xyz'
        sc.PoC = 'a'
        sc.Room = 'b'
        sc.Bed = 'c'
        sc.Facility = 'd'
        sc.Building = 'e'
        sc.Floor = 'f'
        
        #test creation from non-empty node
        node = sc.mkStateNode()
        sc2 = statecontainers.LocationContextStateContainer(nsmapper=self.nsmapper, 
                                                                     descriptorContainer=self.dc, 
                                                                     node=node)
        verifyEqual(sc, sc2)
        sc.PoC = 'aa'
        sc.Room = 'bb'
        sc.Bed = 'cc'
        sc.Facility = 'dd'
        sc.Building = 'ee'
        sc.Floor = 'ff'
        
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)

        dr = SdcLocation(fac='a', poc='b', bed='c', bld='d', flr='e', rm='f', root='g')
        bicepsSchema = xmlparsing.BicepsSchema(SDC_v1_Definitions)
        sc = statecontainers.LocationContextStateContainer.fromDrLocation(nsmapper=self.nsmapper, 
                                                                           descriptorContainer=self.dc, 
                                                                           handle='abc', 
                                                                           draegerLocation=dr,
                                                                           bicepsSchema=bicepsSchema)
        self.assertEqual(sc.Handle, 'abc')
        self.assertEqual(sc.PoC, 'b')
        self.assertEqual(sc.Room, 'f')
        self.assertEqual(sc.Bed, 'c')
        self.assertEqual(sc.Facility, 'a')
        self.assertEqual(sc.Building, 'd')
        self.assertEqual(sc.Floor, 'e')

        #test creation from non-empty node
        node = sc.mkStateNode()
        sc2 = statecontainers.LocationContextStateContainer(nsmapper=self.nsmapper, 
                                                            descriptorContainer=self.dc, 
                                                            node=node)
        verifyEqual(sc, sc2)
        
        #Umlaut test
        dr = SdcLocation(fac=u'Dräger', poc=u'b', bed=u'c', bld=u'd', flr=u'e', rm=u'f', root=u'g')
        sc3 = statecontainers.LocationContextStateContainer.fromDrLocation(nsmapper=self.nsmapper, 
                                                                           descriptorContainer=self.dc, 
                                                                           handle='abc', 
                                                                           draegerLocation=dr,
                                                                           bicepsSchema=bicepsSchema)
        sc2.updateFromDraegerLocation(dr, bicepsSchema)
        verifyEqual(sc3, sc2)


    def test_PatientContextStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Handle, origin.Handle)
            self.assertEqual(copied.Givenname, origin.Givenname)
            self.assertEqual(copied.Middlename, origin.Middlename)
            self.assertEqual(copied.Familyname, origin.Familyname)
            self.assertEqual(copied.DateOfBirth, origin.DateOfBirth)
            self.assertEqual(copied.Height, origin.Height)
            self.assertEqual(copied.Weight, origin.Weight)
            self.assertEqual(copied.Race, origin.Race)
            self.assertEqual(copied.Identification, origin.Identification)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
        
        sc = statecontainers.PatientContextStateContainer(nsmapper=self.nsmapper, 
                                                           descriptorContainer=self.dc, 
                                                           node=None)
        sc.Identification.append(pmtypes.InstanceIdentifier('abc', pmtypes.CodedValue('123'), [pmtypes.LocalizedText('Peter', 'en'),
                                                                                               pmtypes.LocalizedText('Paul'),
                                                                                               pmtypes.LocalizedText('Mary')]))
        sc.Identification.append(pmtypes.InstanceIdentifier('def', pmtypes.CodedValue('456'), [pmtypes.LocalizedText('John'),
                                                                                               pmtypes.LocalizedText('Jim'),
                                                                                               pmtypes.LocalizedText('Jane')]))
        self.assertTrue(sc.Handle is not None)
        sc.Givenname = 'Karl'
        sc.Middlename = 'M.'
        sc.Familyname = 'Klammer'
        sc.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
        sc.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
        sc.Race = pmtypes.CodedValue('123', 'def')

        sc.DateOfBirth = datetime.date(2001, 3, 12)
        print (sc.DateOfBirth)
        
        #test creation from node
        node = sc.mkStateNode()
        print (etree_.tostring(node, pretty_print=True).decode('utf-8'))
        sc2 = statecontainers.PatientContextStateContainer(nsmapper=self.nsmapper, 
                                                            descriptorContainer=self.dc, 
                                                            node=node)
        verifyEqual(sc, sc2)

        #test update from node
        sc.Middlename = 'K.'
        sc.DateOfBirth = datetime.datetime(2001, 3, 12, 14, 30, 1)
        sc.incrementState()
        sc.Height._value =42
        sc.Weight._value =420
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_PatientContextStateContainer_final(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Handle, origin.Handle)
            self.assertEqual(copied.Givenname, origin.Givenname)
            self.assertEqual(copied.Middlename, origin.Middlename)
            self.assertEqual(copied.Familyname, origin.Familyname)
            self.assertEqual(copied.DateOfBirth, origin.DateOfBirth)
            self.assertEqual(copied.Height, origin.Height)
            self.assertEqual(copied.Weight, origin.Weight)
            self.assertEqual(copied.Race, origin.Race)
            self.assertEqual(copied.Identification, origin.Identification)
            self._verifyAbstractStateContainerDataEqual(copied, origin)
        
        sc = statecontainers.PatientContextStateContainer(nsmapper=self.nsmapper,
                                                                        descriptorContainer=self.dc,
                                                                        node=None)

        self.assertTrue(sc.Handle is not None)

        sc.Givenname = 'Karl'
        sc.Middlename = 'M.'
        sc.Familyname = 'Klammer'
        sc.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
        sc.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
        sc.Race = pmtypes.CodedValue('123', 'def')

        sc.DateOfBirth = datetime.date(2001, 3, 12)
        print (sc.DateOfBirth)

        sc.Identification.append(pmtypes.InstanceIdentifier('abc', pmtypes.CodedValue('123'), [pmtypes.LocalizedText('Peter', 'en'),
                                                                                               pmtypes.LocalizedText('Paul'),
                                                                                               pmtypes.LocalizedText('Mary')]))
        sc.Identification.append(pmtypes.InstanceIdentifier('def', pmtypes.CodedValue('456'), [pmtypes.LocalizedText('John'),
                                                                                               pmtypes.LocalizedText('Jim'),
                                                                                               pmtypes.LocalizedText('Jane')]))

        #test creation from node
        node = sc.mkStateNode()
        print (etree_.tostring(node, pretty_print=True))
        sc2 = statecontainers.PatientContextStateContainer(nsmapper=self.nsmapper,
                                                                         descriptorContainer=self.dc,
                                                                         node=node)
        verifyEqual(sc, sc2)

        #test update from node
        sc.Middlename = 'K.'
        sc.DateOfBirth = datetime.datetime(2001, 3, 12, 14, 30, 1)
        sc.incrementState()
        sc.Height._value = 42
        sc.Weight._value = 420
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        verifyEqual(sc, sc2)


    def test_SetValueOperationStateContainer(self):
        sc = statecontainers.SetValueOperationStateContainer(nsmapper=self.nsmapper, 
                                                             descriptorContainer=self.dc, 
                                                             node=None)
        
        self.assertEqual(sc.AllowedRange, [])
        sc.AllowedRange.append(pmtypes.Range(1,2,3,4,5))
        node = sc.mkStateNode()
        sc2 = statecontainers.SetValueOperationStateContainer(nsmapper=self.nsmapper, 
                                                             descriptorContainer=self.dc, 
                                                             node=node)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)

        sc.AllowedRange[0].Lower = 42
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)

        sc.AllowedRange.append(pmtypes.Range(3,4,5,6,7))
        node = sc.mkStateNode()
        sc2.updateFromNode(node)
        self.assertEqual(len(sc2.AllowedRange), 2)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestStateContainers)
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_statecontainers.TestStateContainers.test_SetValueOperationStateContainer'))


if __name__ == '__main__':
#    unittest.TextTestRunner(verbosity=2).run(suite())
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_statecontainers.TestStateContainers.test_RealTimeSampleArrayMetricStateContainer'))
    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_statecontainers.TestStateContainers.test_AbstractContextStateContainer'))
