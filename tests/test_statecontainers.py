import datetime
import unittest
from decimal import Decimal
from math import isclose

import sdc11073.xml_types.xml_structure as cp
import sdc11073.mdib.descriptorcontainers as dc
import sdc11073.mdib.statecontainers as sc
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types import pm_types, pm_qnames as pm
from sdc11073.location import SdcLocation
from tests.mockstuff import dec_list

# pylint: disable=protected-access
_my_tag = pm.State


class TestStateContainers(unittest.TestCase):

    def setUp(self):
        self.ns_mapper = ns_hlp
        self.descr = dc.AbstractDescriptorContainer(handle='123', parent_handle='456')
        self.descr.DescriptorVersion = 42

    def test_AbstractStateContainer(self):
        state = sc.AbstractStateContainer(descriptor_container=self.descr)

        # initially the state version shall be 0, and DescriptorVersion shall be set
        self.assertEqual(state.StateVersion, 0)
        self.assertEqual(state.DescriptorVersion, self.descr.DescriptorVersion)

        # verify incrementState works as expected
        state.increment_state_version()
        self.assertEqual(state.StateVersion, 1)
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        self.assertEqual(node.get('StateVersion'), '1')

        # test creation from other container
        state2 = sc.AbstractStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)

        self._verifyAbstractStateContainerDataEqual(state, state2)

        # test update from other container
        state.DescriptorVersion += 1
        state.increment_state_version()
        state2.update_from_other_container(state)
        self._verifyAbstractStateContainerDataEqual(state, state2)

        # also check update via xml node
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        state3 = sc.AbstractStateContainer(descriptor_container=self.descr)
        state3.update_from_node(node)
        self.assertIsNotNone(state3.node)


    def _verifyAbstractStateContainerDataEqual(self, state1, state2):
        self.assertEqual(state1.DescriptorVersion, state2.DescriptorVersion)
        self.assertEqual(state1.StateVersion, state2.StateVersion)

    def test_AbstractOperationStateContainer(self):
        state = sc.AbstractOperationStateContainer(descriptor_container=self.descr)
        self.assertIsNotNone(state.OperatingMode)  # this is a required attribute

        state2 = sc.AbstractOperationStateContainer(descriptor_container=self.descr)
        self.assertIsNotNone(state2.OperatingMode)
        self._verifyAbstractStateContainerDataEqual(state, state2)

        # test update_from_other_container
        state.OperatingMode = pm_types.OperatingMode.NA
        self.assertEqual(state.OperatingMode, pm_types.OperatingMode.NA)
        state2.update_from_other_container(state)
        self.assertEqual(state2.OperatingMode, pm_types.OperatingMode.NA)

    def test_AbstractMetricStateContainer(self):
        descr = dc.NumericMetricDescriptorContainer(handle='123', parent_handle='456')
        state = sc.NumericMetricStateContainer(descriptor_container=descr)

        self.assertEqual(state.ActivationState, 'On')
        for value in list(pm_types.ComponentActivation):
            state.ActivationState = value
            self.assertEqual(state.ActivationState, value)
            node = state.mk_state_node(_my_tag, self.ns_mapper)
            self.assertEqual(node.get('ActivationState'), value)

        self.assertEqual(state.ActiveDeterminationPeriod, None)
        for value in (21, 42):
            state.ActiveDeterminationPeriod = value
            self.assertEqual(state.ActiveDeterminationPeriod, value)
            node = state.mk_state_node(_my_tag, self.ns_mapper)
            self.assertEqual(node.get('ActiveDeterminationPeriod'),
                             cp.DurationConverter.to_xml(value))
        state.BodySite = [pm_types.CodedValue('ABC')]
        state.PhysicalConnector = pm_types.PhysicalConnectorInfo([pm_types.LocalizedText('ABC')], 1)

        # test creation from other container
        state2 = sc.NumericMetricStateContainer(descriptor_container=descr)
        state2.update_from_other_container(state)
        self.assertEqual(state.ActivationState, state2.ActivationState)
        self.assertEqual(state.ActiveDeterminationPeriod, state2.ActiveDeterminationPeriod)
        self.assertEqual(state.BodySite, state2.BodySite)
        self.assertEqual(state.PhysicalConnector, state2.PhysicalConnector)
        self._verifyAbstractStateContainerDataEqual(state, state2)

        # test update from other container
        state.ActivationState = pm_types.ComponentActivation.NOT_READY
        state.ActiveDeterminationPeriod += 1
        state.BodySite = [pm_types.CodedValue('DEF')]
        state.PhysicalConnector = pm_types.PhysicalConnectorInfo([pm_types.LocalizedText('DEF')], 2)
        state.increment_state_version()
        state2.update_from_other_container(state)
        self.assertEqual(state.ActivationState, state2.ActivationState)
        self.assertEqual(state.BodySite, state2.BodySite)
        self.assertEqual(state.PhysicalConnector, state2.PhysicalConnector)
        self.assertEqual(state.ActiveDeterminationPeriod, state2.ActiveDeterminationPeriod)
        self._verifyAbstractStateContainerDataEqual(state, state2)

    def test_NumericMetricStateContainer(self):
        descr = dc.NumericMetricDescriptorContainer(handle='123', parent_handle='456')
        state = sc.NumericMetricStateContainer(descriptor_container=descr)
        state.mk_metric_value()
        self.assertTrue(isinstance(state.MetricValue, pm_types.NumericMetricValue))
        state.MetricValue.Value = Decimal(42.21)
        state.MetricValue.StartTime = 1234567.21
        state.MetricValue.StopTime = state.MetricValue.StartTime + 10
        state.MetricValue.DeterminationTime = state.MetricValue.StartTime + 20
        state.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID
        state.ActiveAveragingPeriod = 42
        state.PhysiologicalRange = [pm_types.Range(*dec_list(1, 2, 3, 4, 5)),
                                    pm_types.Range(*dec_list(10, 20, 30, 40, 50))]

        state2 = sc.NumericMetricStateContainer(descriptor_container=descr)
        state2.update_from_other_container(state)
        # verify also that mkStateNode on receiving sc does not change anything
        for dummy in range(1):
            self.assertTrue(isclose(state.MetricValue.Value, state2.MetricValue.Value))
            self.assertEqual(state.MetricValue.StartTime, state2.MetricValue.StartTime)
            self.assertEqual(state.MetricValue.StopTime, state2.MetricValue.StopTime)
            self.assertEqual(state.MetricValue.DeterminationTime, state2.MetricValue.DeterminationTime)
            self.assertEqual(state.MetricValue.MetricQuality.Validity, state2.MetricValue.MetricQuality.Validity)
            self.assertEqual(state.ActiveAveragingPeriod, state2.ActiveAveragingPeriod)
            self.assertEqual(state.PhysiologicalRange, state2.PhysiologicalRange)

            self._verifyAbstractStateContainerDataEqual(state, state2)
            state.mk_state_node(_my_tag, self.ns_mapper)

        state.MetricValue.Value += 1
        state.increment_state_version()
        state.ActiveAveragingPeriod = 24
        state.PhysiologicalRange[1].Lower = Decimal(100)
        state2.update_from_other_container(state)
        self.assertTrue(isclose(state.MetricValue.Value, state2.MetricValue.Value))
        self.assertEqual(state.ActiveAveragingPeriod, state2.ActiveAveragingPeriod)
        self.assertEqual(state.PhysiologicalRange, state2.PhysiologicalRange)
        self._verifyAbstractStateContainerDataEqual(state, state2)

    def test_StringMetricStateContainer(self):
        descr = dc.StringMetricDescriptorContainer(handle='123', parent_handle='456')
        state = sc.StringMetricStateContainer(descriptor_container=descr)
        state.mk_metric_value()
        self.assertTrue(isinstance(state.MetricValue, pm_types.StringMetricValue))

    def test_EnumStringMetricStateContainer(self):
        descr = dc.EnumStringMetricDescriptorContainer(handle='123', parent_handle='456')
        state = sc.EnumStringMetricStateContainer(descriptor_container=descr)
        state.mk_metric_value()
        self.assertTrue(isinstance(state.MetricValue, pm_types.StringMetricValue))

    def test_RealTimeSampleArrayMetricStateContainer(self):
        descr = dc.RealTimeSampleArrayMetricDescriptorContainer(handle='123', parent_handle='456')

        def verifyEqual(origin, copied):
            self.assertEqual(len(copied.MetricValue.Samples), len(origin.MetricValue.Samples))
            for c, o in zip(copied.MetricValue.Samples, origin.MetricValue.Samples):
                self.assertTrue(isclose(c, o))
            self.assertEqual(copied.MetricValue.DeterminationTime, origin.MetricValue.DeterminationTime)
            self.assertEqual(copied.MetricValue.Annotation, origin.MetricValue.Annotation)
            self.assertEqual(copied.MetricValue.ApplyAnnotations, origin.MetricValue.ApplyAnnotations)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        state = sc.RealTimeSampleArrayMetricStateContainer(descriptor_container=descr)
        state.mk_metric_value()
        self.assertTrue(isinstance(state.MetricValue, pm_types.SampleArrayValue))

        state.MetricValue.Samples = dec_list(1, 2, 3, 4, 5.5)
        state.MetricValue.DeterminationTime = 1234567
        state.MetricValue.Annotations = []
        state.MetricValue.ApplyAnnotations = []
        state.ActivationState = pm_types.ComponentActivation.FAILURE

        # test creation from other container
        state2 = sc.RealTimeSampleArrayMetricStateContainer(descriptor_container=descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        state.MetricValue.Samples = dec_list(5.5, 6.6)
        state.MetricValue.DeterminationTime = 2345678
        state.MetricValue.Annotations = [pm_types.Annotation(pm_types.CodedValue('a', 'b'))]
        state.MetricValue.ApplyAnnotations = [pm_types.ApplyAnnotation(1, 2)]

        state.increment_state_version()
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

    def test_AbstractDeviceComponentStateContainer(self):

        def verifyEqual(origin, copied):
            self.assertEqual(copied.CalibrationInfo, origin.CalibrationInfo)
            self.assertEqual(copied.NextCalibration, origin.NextCalibration)
            self.assertEqual(copied.PhysicalConnector, origin.PhysicalConnector)
            self.assertEqual(copied.ActivationState, origin.ActivationState)
            self.assertEqual(copied.OperatingHours, origin.OperatingHours)
            self.assertEqual(copied.OperatingCycles, origin.OperatingCycles)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        state = sc.AbstractDeviceComponentStateContainer(descriptor_container=self.descr)
        self.assertEqual(state.ActivationState, pm_types.ComponentActivation.ON)
        self.assertEqual(state.OperatingHours, None)
        self.assertEqual(state.OperatingCycles, None)
        self.assertEqual(state.PhysicalConnector, None)
        self.assertEqual(state.CalibrationInfo, None)
        self.assertEqual(state.NextCalibration, None)

        state.ActivationState = pm_types.ComponentActivation.ON
        state.OperatingHours = 4
        state.PhysicalConnector = pm_types.PhysicalConnectorInfo([pm_types.LocalizedText('ABC')], 1)

        calibration_result = pm_types.CalibrationResult()
        calibration_result.Code = pm_types.CodedValue("42")
        calibration_result.Value = pm_types.Measurement(Decimal(50), pm_types.CodedValue("10"))
        calibration_documentation = pm_types.CalibrationDocumentation()
        calibration_documentation.Documentation.append(pm_types.LocalizedText('documentation result'))
        calibration_documentation.CalibrationResult.append(calibration_result)

        calib_info = pm_types.CalibrationInfo()
        self.assertEqual(calib_info.Type, pm_types.CalibrationType.UNSPEC)
        calib_info.CalibrationDocumentation = [calibration_documentation]
        calib_info.ComponentCalibrationState = pm_types.CalibrationState.CALIBRATED
        calib_info.Time = 3782495
        calib_info.Type = pm_types.CalibrationType.TWO_POINT_CALIBRATION
        state.CalibrationInfo = calib_info

        state2 = sc.AbstractDeviceComponentStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        state.ActivationState = pm_types.ComponentActivation.OFF
        state.OperatingHours += 1
        state.OperatingHours += 1
        state.PhysicalConnector = pm_types.PhysicalConnectorInfo([pm_types.LocalizedText('DEF')], 2)
        state.CalibrationInfo.CalibrationDocumentation[0].CalibrationResult[0].Code = pm_types.CodedValue("1000")
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

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

        state = sc.AlertSystemStateContainer(descriptor_container=self.descr)
        self.assertEqual(state.SystemSignalActivation, [])
        self.assertEqual(state.LastSelfCheck, None)
        self.assertEqual(state.SelfCheckCount, None)
        self.assertEqual(state.PresentPhysiologicalAlarmConditions, [])
        self.assertEqual(state.PresentTechnicalAlarmConditions, [])

        # test creation from other container
        state.SystemSignalActivation = [
            pm_types.SystemSignalActivation(manifestation=pm_types.AlertSignalManifestation.AUD,
                                           state=pm_types.AlertActivation.ON),
            pm_types.SystemSignalActivation(manifestation=pm_types.AlertSignalManifestation.VIS,
                                           state=pm_types.AlertActivation.ON)
        ]
        state.LastSelfCheck = 1234567
        state.SelfCheckCount = 3
        state.PresentPhysiologicalAlarmConditions = ["handle1", "handle2", "handle3"]
        state.increment_state_version()
        state2 = sc.AlertSystemStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        # test update from other container
        state.LastSelfCheck = 12345678
        state.SelfCheckCount = 4
        state.PresentPhysiologicalAlarmConditions = ["handle2", "handle3", "handle4"]
        state.increment_state_version()
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

    def test_AlertConditionStateContainer(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.ActualPriority, origin.ActualPriority)
            self.assertEqual(copied.Rank, origin.Rank)
            self.assertEqual(copied.DeterminationTime, origin.DeterminationTime)
            self.assertEqual(copied.Presence, origin.Presence)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        state = sc.AlertConditionStateContainer(descriptor_container=self.descr)
        self.assertEqual(state.ActualPriority, None)
        self.assertEqual(state.Rank, None)
        self.assertEqual(state.DeterminationTime, None)
        self.assertEqual(state.Presence, False)

        state2 = sc.AlertConditionStateContainer(descriptor_container=self.descr)
        verifyEqual(state, state2)

        # test update from other container
        state.ActualPriority = pm_types.AlertConditionPriority.LOW
        state.Rank = 3
        state.DeterminationTime = 1234567
        state.Presence = True
        state.increment_state_version()
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

    def test_LimitAlertConditionStateContainer_Final(self):
        def verifyEqual(origin, copied):
            self.assertEqual(copied.Limits, origin.Limits)
            self.assertEqual(copied.MonitoredAlertLimits, origin.MonitoredAlertLimits)
            self.assertEqual(copied.AutoLimitActivationState, origin.AutoLimitActivationState)
            self._verifyAbstractStateContainerDataEqual(copied, origin)

        state = sc.LimitAlertConditionStateContainer(descriptor_container=self.descr)
        self.assertEqual(state.MonitoredAlertLimits, pm_types.AlertConditionMonitoredLimits.NONE)
        self.assertEqual(state.AutoLimitActivationState, None)

        state2 = sc.LimitAlertConditionStateContainer(descriptor_container=self.descr)
        verifyEqual(state, state2)

        # test update from other container
        state.Limits = pm_types.Range(*dec_list(5, 9, '0.1', '0.01', '0.001'))
        state.Rank = 3
        state.DeterminationTime = 1234567
        state.Presence = True
        state.increment_state_version()
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

    def test_SetStringOperationStateContainer(self):
        state = sc.SetStringOperationStateContainer(descriptor_container=self.descr)
        # verify that initial pyValue is empty, and that no AllowedValues node is created
        self.assertEqual(state.AllowedValues.Value, [])
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=ns_hlp.ns_map)
        self.assertEqual(len(allowedValuesNodes), 0)

        state2 = sc.SetStringOperationStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        self.assertEqual(state2.AllowedValues.Value, [])

        # verify that setting to None is identical to empty list
        state.AllowedValues.Value = []
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=ns_hlp.ns_map)
        self.assertEqual(len(allowedValuesNodes), 0)

        # verify that non-empty list creates values in xml and that same list appears in container created from that xml
        state.AllowedValues.Value = ['a', 'b', 'c']
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=ns_hlp.ns_map)
        self.assertEqual(len(allowedValuesNodes), 1)
        valuesNodes = node.xpath('//dom:Value', namespaces=ns_hlp.ns_map)
        self.assertEqual(len(valuesNodes), 3)
        state2 = sc.SetStringOperationStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        self.assertEqual(state2.AllowedValues.Value, ['a', 'b', 'c'])

        # verify that setting it back to None clears all data
        state.AllowedValues.Value = None
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        allowedValuesNodes = node.xpath('//dom:AllowedValues', namespaces=ns_hlp.ns_map)
        self.assertEqual(len(allowedValuesNodes), 0)
        state2 = sc.SetStringOperationStateContainer(descriptor_container=self.descr)
        self.assertEqual(state2.AllowedValues.Value, [])

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

        state = sc.AbstractContextStateContainer(descriptor_container=self.descr)
        self.assertEqual(state.ContextAssociation, 'No')
        self.assertEqual(state.BindingMdibVersion, None)
        self.assertEqual(state.UnbindingMdibVersion, None)
        self.assertEqual(state.BindingStartTime, None)
        self.assertEqual(state.BindingEndTime, None)
        self.assertEqual(state.Validator, [])
        self.assertEqual(state.Identification, [])

        idents = [pm_types.InstanceIdentifier(root='abc',
                                             type_coded_value=pm_types.CodedValue('abc', 'def'),
                                             identifier_names=[pm_types.LocalizedText('ABC')],
                                             extension_string='123')]
        state.Identification = idents
        self.assertEqual(state.Identification, idents)

        validators = [pm_types.InstanceIdentifier(root='ABC',
                                                 type_coded_value=pm_types.CodedValue('123', '456'),
                                                 identifier_names=[pm_types.LocalizedText('DEF')],
                                                 extension_string='321')]
        state.Validator = validators
        self.assertEqual(state.Validator, validators)

        for value in list(pm_types.ContextAssociation):
            state.ContextAssociation = value
            node = state.mk_state_node(_my_tag, self.ns_mapper)
            self.assertEqual(node.get('ContextAssociation'), value)

        for value in (12345.123, 67890.987):
            state.BindingStartTime = value
            state.BindingEndTime = value + 1
            node = state.mk_state_node(_my_tag, self.ns_mapper)
            self.assertEqual(node.get('BindingStartTime'), cp.TimestampConverter.to_xml(value))
            self.assertEqual(node.get('BindingEndTime'), cp.TimestampConverter.to_xml(value + 1))

        for value in (0, 42, 123):
            state.BindingMdibVersion = value
            state.UnbindingMdibVersion = value + 1
            node = state.mk_state_node(_my_tag, self.ns_mapper)
            self.assertEqual(node.get('BindingMdibVersion'), cp.IntegerConverter.to_xml(value))
            self.assertEqual(node.get('UnbindingMdibVersion'), cp.IntegerConverter.to_xml(value + 1))

        # test creation from other container
        state.Identification = idents
        state.Validator = validators
        state2 = sc.AbstractContextStateContainer(descriptor_container=self.descr, )
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

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

        state = sc.LocationContextStateContainer(descriptor_container=self.descr)

        self.assertEqual(state.LocationDetail.PoC, None)
        self.assertEqual(state.LocationDetail.Room, None)
        self.assertEqual(state.LocationDetail.Bed, None)
        self.assertEqual(state.LocationDetail.Facility, None)
        self.assertEqual(state.LocationDetail.Building, None)
        self.assertEqual(state.LocationDetail.Floor, None)

        # test creation from empty node
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        self.assertEqual(node.get('Handle'), state.Handle)
        state2 = sc.LocationContextStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        state.Handle = 'xyz'
        state.LocationDetail.PoC = 'a'
        state.LocationDetail.Room = 'b'
        state.LocationDetail.Bed = 'c'
        state.LocationDetail.Facility = 'd'
        state.LocationDetail.Building = 'e'
        state.LocationDetail.Floor = 'f'

        state2 = sc.LocationContextStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)
        state.LocationDetail.PoC = 'aa'
        state.LocationDetail.Room = 'bb'
        state.LocationDetail.Bed = 'cc'
        state.LocationDetail.Facility = 'dd'
        state.LocationDetail.Building = 'ee'
        state.LocationDetail.Floor = 'ff'

        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        loc = SdcLocation(fac='a', poc='b', bed='c', bldng='d', flr='e', rm='f', root='g')
        state = sc.LocationContextStateContainer.from_sdc_location(descriptor_container=self.descr,
                                                                   handle='abc',
                                                                   sdc_location=loc)
        self.assertEqual(state.Handle, 'abc')
        self.assertEqual(state.LocationDetail.PoC, 'b')
        self.assertEqual(state.LocationDetail.Room, 'f')
        self.assertEqual(state.LocationDetail.Bed, 'c')
        self.assertEqual(state.LocationDetail.Facility, 'a')
        self.assertEqual(state.LocationDetail.Building, 'd')
        self.assertEqual(state.LocationDetail.Floor, 'e')

        state2 = sc.LocationContextStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        state3 = sc.LocationContextStateContainer.from_sdc_location(descriptor_container=self.descr,
                                                                    handle='abc',
                                                                    sdc_location=loc)
        state2.update_from_sdc_location(loc)
        verifyEqual(state3, state2)

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

        state = sc.PatientContextStateContainer(descriptor_container=self.descr)

        state.CoreData.Givenname = 'Karl'
        state.CoreData.Middlename = ['M.']
        state.CoreData.Familyname = 'Klammer'
        state.CoreData.Height = pm_types.Measurement(Decimal('88.2'), pm_types.CodedValue('abc', 'def'))
        state.CoreData.Weight = pm_types.Measurement(Decimal('68.2'), pm_types.CodedValue('abc'))
        state.CoreData.Race = pm_types.CodedValue('123', 'def')

        state.CoreData.DateOfBirth = datetime.date(2001, 3, 12)

        state.Identification.append(
            pm_types.InstanceIdentifier('abc', pm_types.CodedValue('123'), [pm_types.LocalizedText('Peter', 'en'),
                                                                          pm_types.LocalizedText('Paul'),
                                                                          pm_types.LocalizedText('Mary')]))
        state.Identification.append(
            pm_types.InstanceIdentifier('def', pm_types.CodedValue('456'), [pm_types.LocalizedText('John'),
                                                                          pm_types.LocalizedText('Jim'),
                                                                          pm_types.LocalizedText('Jane')]))

        _ = state.mk_state_node(_my_tag, self.ns_mapper)
        state2 = sc.PatientContextStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        verifyEqual(state, state2)

        state.CoreData.Middlename = ['K.']
        state.CoreData.DateOfBirth = datetime.datetime(2001, 3, 12, 14, 30, 1)
        state.increment_state_version()
        # state.CoreData.Height._value = 42
        # state.CoreData.Weight._value = 420
        state.CoreData.Height = pm_types.Measurement(Decimal('42'), unit=pm_types.CodedValue('44444'))
        state.CoreData.Weight = pm_types.Measurement(Decimal('420'), unit=pm_types.CodedValue('44444'))

        state2.update_from_other_container(state)
        verifyEqual(state, state2)

    def test_PatientContextStateContainerNeo(self):
        """Test if a pm_types class derived from the value_class of a property is handled correctly.
         In this test:
          - state.Core becomes a NeonatalPatientDemographicsCoreData instead of PatientDemographicsCoreData.
          - state.Core.Mother becomes a PersonParticipation instead of PersonReference"""
        state = sc.PatientContextStateContainer(descriptor_container=self.descr)
        state.CoreData = pm_types.NeonatalPatientDemographicsCoreData(given_name='Otto',
                                                                     family_name='Smith')
        state.CoreData.BirthLength = pm_types.Measurement(Decimal('57.6'), pm_types.CodedValue('abc', 'def'))
        state.CoreData.Mother = pm_types.PersonParticipation(identifications=[pm_types.InstanceIdentifier('root')],
                                                            name=pm_types.BaseDemographics(given_name='Charly'))
        state2 = sc.PatientContextStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        self.assertTrue(isinstance(state2.CoreData, pm_types.NeonatalPatientDemographicsCoreData))
        # also check update via xml node
        node = state.mk_state_node(_my_tag, self.ns_mapper)
        state3 = sc.PatientContextStateContainer(descriptor_container=self.descr)
        state3.update_from_node(node)
        self.assertEqual(state3.CoreData.__class__, pm_types.NeonatalPatientDemographicsCoreData)
        self.assertEqual(state3.CoreData.Mother.__class__, pm_types.PersonParticipation)

    def test_SetValueOperationStateContainer(self):
        state = sc.SetValueOperationStateContainer(descriptor_container=self.descr)

        self.assertEqual(state.AllowedRange, [])
        state.AllowedRange.append(pm_types.Range(*dec_list(1, 2, 3, 4, 5)))
        state2 = sc.SetValueOperationStateContainer(descriptor_container=self.descr)
        state2.update_from_other_container(state)
        self.assertEqual(state.AllowedRange, state2.AllowedRange)

        state.AllowedRange[0].Lower = Decimal(42)
        state2.update_from_other_container(state)
        self.assertEqual(state.AllowedRange, state2.AllowedRange)

        state.AllowedRange.append(pm_types.Range(*dec_list(3, 4, 5, 6, 7)))
        state2.update_from_other_container(state)
        self.assertEqual(len(state2.AllowedRange), 2)
        self.assertEqual(state.AllowedRange, state2.AllowedRange)
