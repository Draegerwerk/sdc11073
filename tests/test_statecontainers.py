# coding: utf-8
import datetime
import unittest
from math import isclose
from decimal import Decimal
from lxml import etree as etree_

import sdc11073.mdib.containerproperties as containerproperties
import sdc11073.mdib.descriptorcontainers as descriptorcontainers
import sdc11073.mdib.statecontainers as statecontainers
import sdc11073.namespaces as namespaces
import sdc11073.pmtypes as pmtypes
from sdc11073.location import SdcLocation
from sdc11073 import pm_qnames as pm
from tests.mockstuff import dec_list
# pylint: disable=protected-access
_my_tag = pm.State


class TestStateContainers(unittest.TestCase):

    def setUp(self):
        self.nsmapper = namespaces.DocNamespaceHelper()
        self.dc = descriptorcontainers.AbstractDescriptorContainer(handle='123', parent_handle='456')
        self.dc.DescriptorVersion = 42

    def test_AbstractStateContainer(self):
        sc = statecontainers.AbstractStateContainer(descriptor_container=self.dc)

        # initially the state version shall be 0, and DescriptorVersion shall be set
        self.assertEqual(sc.StateVersion, 0)
        self.assertEqual(sc.DescriptorVersion, self.dc.DescriptorVersion)

        # verify incrementState works as expected
        sc.increment_state_version()
        self.assertEqual(sc.StateVersion, 1)
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        self.assertEqual(node.get('StateVersion'), '1')

        # test creation from other container
        sc2 = statecontainers.AbstractStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)

        self._verifyAbstractStateContainerDataEqual(sc, sc2)

        # test update from other container
        sc.DescriptorVersion += 1
        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

    def _verifyAbstractStateContainerDataEqual(self, sc1, sc2):
        self.assertEqual(sc1.DescriptorVersion, sc2.DescriptorVersion)
        self.assertEqual(sc1.StateVersion, sc2.StateVersion)

    def test_AbstractOperationStateContainer(self):
        sc = statecontainers.AbstractOperationStateContainer(descriptor_container=self.dc)
        self.assertIsNotNone(sc.OperatingMode)  # this is a required attribute

        sc2 = statecontainers.AbstractOperationStateContainer(descriptor_container=self.dc)
        self.assertIsNotNone(sc2.OperatingMode)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

        # test update_from_other_container
        sc.OperatingMode = pmtypes.OperatingMode.NA
        self.assertEqual(sc.OperatingMode, pmtypes.OperatingMode.NA)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc2.OperatingMode, pmtypes.OperatingMode.NA)

    def test_AbstractMetricStateContainer(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(handle='123', parent_handle='456')
        sc = statecontainers.NumericMetricStateContainer(descriptor_container=dc)

        self.assertEqual(sc.ActivationState, 'On')
        for value in list(pmtypes.ComponentActivation):
            sc.ActivationState = value
            self.assertEqual(sc.ActivationState, value)
            node = sc.mk_state_node(_my_tag, self.nsmapper)
            self.assertEqual(node.get('ActivationState'), value)

        self.assertEqual(sc.ActiveDeterminationPeriod, None)
        for value in (21, 42):
            sc.ActiveDeterminationPeriod = value
            self.assertEqual(sc.ActiveDeterminationPeriod, value)
            node = sc.mk_state_node(_my_tag, self.nsmapper)
            self.assertEqual(node.get('ActiveDeterminationPeriod'),
                             containerproperties.DurationConverter.to_xml(value))
        sc.BodySite = [pmtypes.CodedValue('ABC')]
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)

        # test creation from other container
        sc2 = statecontainers.NumericMetricStateContainer(descriptor_container=dc)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc.ActivationState, sc2.ActivationState)
        self.assertEqual(sc.ActiveDeterminationPeriod, sc2.ActiveDeterminationPeriod)
        self.assertEqual(sc.BodySite, sc2.BodySite)
        self.assertEqual(sc.PhysicalConnector, sc2.PhysicalConnector)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

        # test update from other container
        sc.ActivationState = pmtypes.ComponentActivation.NOT_READY
        sc.ActiveDeterminationPeriod += 1
        sc.BodySite = [pmtypes.CodedValue('DEF')]
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('DEF')], 2)
        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        self.assertEqual(sc.ActivationState, sc2.ActivationState)
        self.assertEqual(sc.BodySite, sc2.BodySite)
        self.assertEqual(sc.PhysicalConnector, sc2.PhysicalConnector)
        self.assertEqual(sc.ActiveDeterminationPeriod, sc2.ActiveDeterminationPeriod)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

    def test_NumericMetricStateContainer(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(handle='123', parent_handle='456')
        sc = statecontainers.NumericMetricStateContainer(descriptor_container=dc)
        sc.mk_metric_value()
        self.assertTrue(isinstance(sc.MetricValue, pmtypes.NumericMetricValue))
        sc.MetricValue.Value = Decimal(42.21)
        sc.MetricValue.StartTime = 1234567.21
        sc.MetricValue.StopTime = sc.MetricValue.StartTime + 10
        sc.MetricValue.DeterminationTime = sc.MetricValue.StartTime + 20
        sc.MetricValue.Validity = 'Vld'
        sc.ActiveAveragingPeriod = 42
        sc.PhysiologicalRange = [pmtypes.Range(*dec_list(1, 2, 3, 4, 5)), pmtypes.Range(*dec_list(10, 20, 30, 40, 50))]

        sc2 = statecontainers.NumericMetricStateContainer(descriptor_container=dc)
        sc2.update_from_other_container(sc)
        # verify also that mkStateNode on receiving sc does not change anything
        for dummy in range(1):
            self.assertTrue(isclose(sc.MetricValue.Value, sc2.MetricValue.Value))
            self.assertEqual(sc.MetricValue.StartTime, sc2.MetricValue.StartTime)
            self.assertEqual(sc.MetricValue.StopTime, sc2.MetricValue.StopTime)
            self.assertEqual(sc.MetricValue.DeterminationTime, sc2.MetricValue.DeterminationTime)
            self.assertEqual(sc.MetricValue.Validity, sc2.MetricValue.Validity)
            self.assertEqual(sc.ActiveAveragingPeriod, sc2.ActiveAveragingPeriod)
            self.assertEqual(sc.PhysiologicalRange, sc2.PhysiologicalRange)

            self._verifyAbstractStateContainerDataEqual(sc, sc2)
            sc.mk_state_node(_my_tag, self.nsmapper)

        sc.MetricValue.Value += 1
        sc.increment_state_version()
        sc.ActiveAveragingPeriod = 24
        sc.PhysiologicalRange[1].Lower = Decimal(100)
        sc2.update_from_other_container(sc)
        self.assertTrue(isclose(sc.MetricValue.Value, sc2.MetricValue.Value))
        self.assertEqual(sc.ActiveAveragingPeriod, sc2.ActiveAveragingPeriod)
        self.assertEqual(sc.PhysiologicalRange, sc2.PhysiologicalRange)
        self._verifyAbstractStateContainerDataEqual(sc, sc2)

    def test_StringMetricStateContainer(self):
        dc = descriptorcontainers.StringMetricDescriptorContainer(handle='123',parent_handle='456')
        sc = statecontainers.StringMetricStateContainer(descriptor_container=dc)
        sc.mk_metric_value()
        self.assertTrue(isinstance(sc.MetricValue, pmtypes.StringMetricValue))

    def test_EnumStringMetricStateContainer(self):
        dc = descriptorcontainers.EnumStringMetricDescriptorContainer(handle='123', parent_handle='456')
        sc = statecontainers.EnumStringMetricStateContainer(descriptor_container=dc)
        sc.mk_metric_value()
        self.assertTrue(isinstance(sc.MetricValue, pmtypes.StringMetricValue))

    def test_RealTimeSampleArrayMetricStateContainer(self):
        dc = descriptorcontainers.RealTimeSampleArrayMetricDescriptorContainer(handle='123', parent_handle='456')

        def verifyEqual(origin, copied):
            self.assertEqual(len(copied.MetricValue.Samples), len(origin.MetricValue.Samples))
            for c, o in zip(copied.MetricValue.Samples, origin.MetricValue.Samples):
                self.assertTrue((isclose(c, o)))
            self.assertEqual(copied.MetricValue.DeterminationTime, origin.MetricValue.DeterminationTime)
            self.assertEqual(copied.MetricValue.Annotation, origin.MetricValue.Annotation)
            self.assertEqual(copied.MetricValue.ApplyAnnotations, origin.MetricValue.ApplyAnnotations)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.RealTimeSampleArrayMetricStateContainer(descriptor_container=dc)
        sc.mk_metric_value()
        self.assertTrue(isinstance(sc.MetricValue, pmtypes.SampleArrayValue))

        sc.MetricValue.Samples = dec_list(1, 2, 3, 4, 5.5)
        sc.MetricValue.DeterminationTime = 1234567
        sc.MetricValue.Annotations = []
        sc.MetricValue.ApplyAnnotations = []
        sc.ActivationState = pmtypes.ComponentActivation.FAILURE

        # test creation from other container
        sc2 = statecontainers.RealTimeSampleArrayMetricStateContainer(descriptor_container=dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        sc.MetricValue.Samples = dec_list(5.5, 6.6)
        sc.MetricValue.DeterminationTime = 2345678
        sc.MetricValue.Annotations = [pmtypes.Annotation(pmtypes.CodedValue('a', 'b'))]
        sc.MetricValue.ApplyAnnotations = [pmtypes.ApplyAnnotation(1, 2)]

        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_AbstractDeviceComponentStateContainer(self):

        def verifyEqual(origin, copied):
            self.assertEqual(copied.ActivationState, origin.ActivationState)
            self.assertEqual(copied.OperatingHours, origin.OperatingHours)
            self.assertEqual(copied.OperatingCycles, origin.OperatingCycles)
            self.assertEqual(copied.PhysicalConnector, origin.PhysicalConnector)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.AbstractDeviceComponentStateContainer(descriptor_container=self.dc, )
        self.assertEqual(sc.ActivationState, None)
        self.assertEqual(sc.OperatingHours, None)
        self.assertEqual(sc.OperatingCycles, None)
        self.assertEqual(sc.PhysicalConnector, None)

        sc.ActivationState = pmtypes.ComponentActivation.ON
        sc.OperatingHours = 4
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)

        sc2 = statecontainers.AbstractDeviceComponentStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        sc.ActivationState = pmtypes.ComponentActivation.OFF
        sc.OperatingHours += 1
        sc.OperatingHours += 1
        sc.PhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('DEF')], 2)
        sc2.update_from_other_container(sc)
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

    def test_AlertSystemStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.SystemSignalActivation, origin.SystemSignalActivation)
            self.assertEqual(copied.LastSelfCheck, origin.LastSelfCheck)
            self.assertEqual(copied.SelfCheckCount, origin.SelfCheckCount)
            self.assertEqual(copied.PresentPhysiologicalAlarmConditions, origin.PresentPhysiologicalAlarmConditions)
            self.assertEqual(copied.PresentTechnicalAlarmConditions, origin.PresentTechnicalAlarmConditions)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.AlertSystemStateContainer(descriptor_container=self.dc)
        self.assertEqual(sc.SystemSignalActivation, [])
        self.assertEqual(sc.LastSelfCheck, None)
        self.assertEqual(sc.SelfCheckCount, None)
        self.assertEqual(sc.PresentPhysiologicalAlarmConditions, [])
        self.assertEqual(sc.PresentTechnicalAlarmConditions, [])

        # test creation from other container
        sc.SystemSignalActivation = [pmtypes.SystemSignalActivation(manifestation=pmtypes.AlertSignalManifestation.AUD,
                                                                    state=pmtypes.AlertActivation.ON),
                                     pmtypes.SystemSignalActivation(manifestation=pmtypes.AlertSignalManifestation.VIS,
                                                                    state=pmtypes.AlertActivation.ON)
                                     ]
        sc.LastSelfCheck = 1234567
        sc.SelfCheckCount = 3
        sc.PresentPhysiologicalAlarmConditions = ["handle1", "handle2", "handle3"]
        sc.increment_state_version()
        sc2 = statecontainers.AlertSystemStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        # test update from other container
        sc.LastSelfCheck = 12345678
        sc.SelfCheckCount = 4
        sc.PresentPhysiologicalAlarmConditions = ["handle2", "handle3", "handle4"]
        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_AlertConditionStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.ActualPriority, origin.ActualPriority)
            self.assertEqual(copied.Rank, origin.Rank)
            self.assertEqual(copied.DeterminationTime, origin.DeterminationTime)
            self.assertEqual(copied.Presence, origin.Presence)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.AlertConditionStateContainer(descriptor_container=self.dc)
        self.assertEqual(sc.ActualPriority, None)
        self.assertEqual(sc.Rank, None)
        self.assertEqual(sc.DeterminationTime, None)
        self.assertEqual(sc.Presence, False)

        sc2 = statecontainers.AlertConditionStateContainer(descriptor_container=self.dc)
        verifyEqual(sc, sc2)

        # test update from other container
        sc.ActualPriority = pmtypes.AlertConditionPriority.LOW
        sc.Rank = 3
        sc.DeterminationTime = 1234567
        sc.Presence = True
        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_LimitAlertConditionStateContainer_Final(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Limits, origin.Limits)
            self.assertEqual(copied.MonitoredAlertLimits, origin.MonitoredAlertLimits)
            self.assertEqual(copied.AutoLimitActivationState, origin.AutoLimitActivationState)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.LimitAlertConditionStateContainer(descriptor_container=self.dc)
        self.assertEqual(sc.MonitoredAlertLimits, pmtypes.AlertConditionMonitoredLimits.ALL_OFF)
        self.assertEqual(sc.AutoLimitActivationState, None)

        sc2 = statecontainers.LimitAlertConditionStateContainer(descriptor_container=self.dc)
        verifyEqual(sc, sc2)

        # test update from other container
        sc.Limits = pmtypes.Range(*dec_list(5, 9, '0.1', '0.01', '0.001'))
        sc.Rank = 3
        sc.DeterminationTime = 1234567
        sc.Presence = True
        sc.increment_state_version()
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_SetStringOperationStateContainer(self):
        sc = statecontainers.SetStringOperationStateContainer(descriptor_container=self.dc)
        # verify that initial pyValue is empty, and that no AllowedValues node is created
        self.assertEqual(sc.AllowedValues.Value, [])
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)

        sc2 = statecontainers.SetStringOperationStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc2.AllowedValues.Value, [])

        # verify that setting to None is identical to empty list
        sc.AllowedValues.Value = []
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)

        # verify that non-empty list creates values in xml and that same list appears in container created from that xml
        sc.AllowedValues.Value = ['a', 'b', 'c']
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 1)
        valuesNodes = node.xpath('//dom:Value', namespaces=namespaces.nsmap)
        self.assertEqual(len(valuesNodes), 3)
        sc2 = statecontainers.SetStringOperationStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc2.AllowedValues.Value, ['a', 'b', 'c'])

        # verify that setting it back to None clears all data
        sc.AllowedValues.Value = None
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=namespaces.nsmap)
        self.assertEqual(len(allowedValuesNodes), 0)
        sc2 = statecontainers.SetStringOperationStateContainer(descriptor_container=self.dc)
        self.assertEqual(sc2.AllowedValues.Value, [])

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

        sc = statecontainers.AbstractContextStateContainer(descriptor_container=self.dc)
        self.assertEqual(sc.ContextAssociation, 'No')
        self.assertEqual(sc.BindingMdibVersion, None)
        self.assertEqual(sc.UnbindingMdibVersion, None)
        self.assertEqual(sc.BindingStartTime, None)
        self.assertEqual(sc.BindingEndTime, None)
        self.assertEqual(sc.Validator, [])
        self.assertEqual(sc.Identification, [])

        idents = [pmtypes.InstanceIdentifier(root='abc',
                                             type_coded_value=pmtypes.CodedValue('abc', 'def'),
                                             identifier_names=[pmtypes.LocalizedText('ABC')],
                                             extension_string='123')]
        sc.Identification = idents
        self.assertEqual(sc.Identification, idents)

        validators = [pmtypes.InstanceIdentifier(root='ABC',
                                                 type_coded_value=pmtypes.CodedValue('123', '456'),
                                                 identifier_names=[pmtypes.LocalizedText('DEF')],
                                                 extension_string='321')]
        sc.Validator = validators
        self.assertEqual(sc.Validator, validators)

        for value in list(pmtypes.ContextAssociation):
            sc.ContextAssociation = value
            node = sc.mk_state_node(_my_tag, self.nsmapper)
            self.assertEqual(node.get('ContextAssociation'), value)

        for value in (12345.123, 67890.987):
            sc.BindingStartTime = value
            sc.BindingEndTime = value + 1
            node = sc.mk_state_node(_my_tag, self.nsmapper)
            self.assertEqual(node.get('BindingStartTime'), containerproperties.TimestampConverter.to_xml(value))
            self.assertEqual(node.get('BindingEndTime'), containerproperties.TimestampConverter.to_xml(value + 1))

        for value in (0, 42, 123):
            sc.BindingMdibVersion = value
            sc.UnbindingMdibVersion = value + 1
            node = sc.mk_state_node(_my_tag, self.nsmapper)
            self.assertEqual(node.get('BindingMdibVersion'), containerproperties.IntegerConverter.to_xml(value))
            self.assertEqual(node.get('UnbindingMdibVersion'), containerproperties.IntegerConverter.to_xml(value + 1))

        # test creation from other container
        sc.Identification = idents
        sc.Validator = validators
        sc2 = statecontainers.AbstractContextStateContainer(descriptor_container=self.dc, )
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_LocationContextStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Handle, origin.Handle)
            self.assertEqual(copied.LocationDetail.PoC, origin.LocationDetail.PoC)
            self.assertEqual(copied.LocationDetail.Room, origin.LocationDetail.Room)
            self.assertEqual(copied.LocationDetail.Bed, origin.LocationDetail.Bed)
            self.assertEqual(copied.LocationDetail.Facility, origin.LocationDetail.Facility)
            self.assertEqual(copied.LocationDetail.Building, origin.LocationDetail.Building)
            self.assertEqual(copied.LocationDetail.Floor, origin.LocationDetail.Floor)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.LocationContextStateContainer(descriptor_container=self.dc)

        self.assertEqual(sc.LocationDetail.PoC, None)
        self.assertEqual(sc.LocationDetail.Room, None)
        self.assertEqual(sc.LocationDetail.Bed, None)
        self.assertEqual(sc.LocationDetail.Facility, None)
        self.assertEqual(sc.LocationDetail.Building, None)
        self.assertEqual(sc.LocationDetail.Floor, None)

        # test creation from empty node
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        self.assertEqual(node.get('Handle'), sc.Handle)
        print(etree_.tostring(node, pretty_print=True))
        sc2 = statecontainers.LocationContextStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        sc.Handle = 'xyz'
        sc.LocationDetail.PoC = 'a'
        sc.LocationDetail.Room = 'b'
        sc.LocationDetail.Bed = 'c'
        sc.LocationDetail.Facility = 'd'
        sc.LocationDetail.Building = 'e'
        sc.LocationDetail.Floor = 'f'

        sc2 = statecontainers.LocationContextStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)
        sc.LocationDetail.PoC = 'aa'
        sc.LocationDetail.Room = 'bb'
        sc.LocationDetail.Bed = 'cc'
        sc.LocationDetail.Facility = 'dd'
        sc.LocationDetail.Building = 'ee'
        sc.LocationDetail.Floor = 'ff'

        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        loc = SdcLocation(fac='a', poc='b', bed='c', bld='d', flr='e', rm='f', root='g')
        sc = statecontainers.LocationContextStateContainer.from_sdc_location(descriptor_container=self.dc,
                                                                             handle='abc',
                                                                             sdc_location=loc)
        self.assertEqual(sc.Handle, 'abc')
        self.assertEqual(sc.LocationDetail.PoC, 'b')
        self.assertEqual(sc.LocationDetail.Room, 'f')
        self.assertEqual(sc.LocationDetail.Bed, 'c')
        self.assertEqual(sc.LocationDetail.Facility, 'a')
        self.assertEqual(sc.LocationDetail.Building, 'd')
        self.assertEqual(sc.LocationDetail.Floor, 'e')

        sc2 = statecontainers.LocationContextStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        sc3 = statecontainers.LocationContextStateContainer.from_sdc_location(descriptor_container=self.dc,
                                                                              handle='abc',
                                                                              sdc_location=loc)
        sc2.update_from_sdc_location(loc)
        verifyEqual(sc3, sc2)

    def test_PatientContextStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Handle, origin.Handle)
            self.assertEqual(copied.CoreData.Givenname, origin.CoreData.Givenname)
            self.assertEqual(copied.CoreData.Middlename, origin.CoreData.Middlename)
            self.assertEqual(copied.CoreData.Familyname, origin.CoreData.Familyname)
            self.assertEqual(copied.CoreData.DateOfBirth, origin.CoreData.DateOfBirth)
            self.assertEqual(copied.CoreData.Height, origin.CoreData.Height)
            self.assertEqual(copied.CoreData.Weight, origin.CoreData.Weight)
            self.assertEqual(copied.CoreData.Race, origin.CoreData.Race)
            self.assertEqual(copied.Identification, origin.Identification)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        sc = statecontainers.PatientContextStateContainer(descriptor_container=self.dc)

        sc.CoreData.Givenname = 'Karl'
        sc.CoreData.Middlename = ['M.']
        sc.CoreData.Familyname = 'Klammer'
        sc.CoreData.Height = pmtypes.Measurement(Decimal('88.2'), pmtypes.CodedValue('abc', 'def'))
        sc.CoreData.Weight = pmtypes.Measurement(Decimal('68.2'), pmtypes.CodedValue('abc'))
        sc.CoreData.Race = pmtypes.CodedValue('123', 'def')

        sc.CoreData.DateOfBirth = datetime.date(2001, 3, 12)

        sc.Identification.append(
            pmtypes.InstanceIdentifier('abc', pmtypes.CodedValue('123'), [pmtypes.LocalizedText('Peter', 'en'),
                                                                          pmtypes.LocalizedText('Paul'),
                                                                          pmtypes.LocalizedText('Mary')]))
        sc.Identification.append(
            pmtypes.InstanceIdentifier('def', pmtypes.CodedValue('456'), [pmtypes.LocalizedText('John'),
                                                                          pmtypes.LocalizedText('Jim'),
                                                                          pmtypes.LocalizedText('Jane')]))

        node = sc.mk_state_node(_my_tag, self.nsmapper)
        print(etree_.tostring(node, pretty_print=True))
        sc2 = statecontainers.PatientContextStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

        sc.CoreData.Middlename = ['K.']
        sc.CoreData.DateOfBirth = datetime.datetime(2001, 3, 12, 14, 30, 1)
        sc.increment_state_version()
        sc.CoreData.Height._value = 42
        sc.CoreData.Weight._value = 420
        sc2.update_from_other_container(sc)
        verifyEqual(sc, sc2)

    def test_PatientContextStateContainerNeo(self):
        """Test if a pmtypes class derived from the value_class of a property is handled correctly.
         In this test:
          - sc.Core becomes a NeonatalPatientDemographicsCoreData instead of PatientDemographicsCoreData.
          - sc.Core.Mother becomes a PersonParticipation instead of PersonReference"""
        sc = statecontainers.PatientContextStateContainer(descriptor_container=self.dc)
        sc.CoreData = pmtypes.NeonatalPatientDemographicsCoreData(given_name='Otto',
                                                                  family_name='Smith')
        sc.CoreData.BirthLength = pmtypes.Measurement(Decimal('57.6'), pmtypes.CodedValue('abc', 'def'))
        sc.CoreData.Mother = pmtypes.PersonParticipation(identifications=[pmtypes.InstanceIdentifier('root')],
                                                         name=pmtypes.BaseDemographics(given_name='Charly'))
        sc2 = statecontainers.PatientContextStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        self.assertTrue(isinstance(sc2.CoreData, pmtypes.NeonatalPatientDemographicsCoreData))
        # also check update via xml node
        node = sc.mk_state_node(_my_tag, self.nsmapper)
        print(etree_.tostring(node, pretty_print=True).decode('utf-8'))
        sc3 = statecontainers.PatientContextStateContainer(descriptor_container=self.dc)
        sc3.update_from_node(node)
        self.assertEqual(sc3.CoreData.__class__, pmtypes.NeonatalPatientDemographicsCoreData)
        self.assertEqual(sc3.CoreData.Mother.__class__, pmtypes.PersonParticipation)

    def test_SetValueOperationStateContainer(self):
        sc = statecontainers.SetValueOperationStateContainer(descriptor_container=self.dc)

        self.assertEqual(sc.AllowedRange, [])
        sc.AllowedRange.append(pmtypes.Range(*dec_list(1, 2, 3, 4, 5)))
        sc2 = statecontainers.SetValueOperationStateContainer(descriptor_container=self.dc)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)

        sc.AllowedRange[0].Lower = Decimal(42)
        sc2.update_from_other_container(sc)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)

        sc.AllowedRange.append(pmtypes.Range(*dec_list(3, 4, 5, 6, 7)))
        sc2.update_from_other_container(sc)
        self.assertEqual(len(sc2.AllowedRange), 2)
        self.assertEqual(sc.AllowedRange, sc2.AllowedRange)
