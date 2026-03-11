"""Unit tests for descriptor containers."""

import unittest

from lxml import etree
from tutorial.codedvaluecomparator import _coded_value_comparator, _list_of_codes_equal

from sdc11073.mdib import descriptorcontainers
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types import msg_qnames as msg
from sdc11073.xml_types import pm_types
from tests.mockstuff import dec_list
from tests.utils import container_diff

test_tag = ns_hlp.PM.tag('MyDescriptor')


class TestDescriptorContainers(unittest.TestCase):
    def setUp(self):
        self.ns_mapper = ns_hlp

    def test_abstract_descriptor_container(self):
        dc = descriptorcontainers.AbstractDescriptorContainer(handle='123', parent_handle='456')

        self.assertEqual(dc.DescriptorVersion, 0)
        self.assertEqual(dc.SafetyClassification, 'Inf')
        self.assertEqual(dc.get_actual_value('SafetyClassification'), None)
        self.assertEqual(dc.Type, None)
        self.assertEqual(len(dc.Extension), 0)

        # test creation from node
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.AbstractDescriptorContainer.from_node(node=node, parent_handle='467')
        self.assertEqual(dc2.DescriptorVersion, 0)
        self.assertEqual(dc2.SafetyClassification, 'Inf')
        self.assertEqual(dc.Type, None)
        self.assertEqual(len(dc.Extension), 0)

        # test update from node
        dc.DescriptorVersion = 42
        dc.SafetyClassification = pm_types.SafetyClassification.MED_A
        dc.Type = pm_types.CodedValue('abc', 'def', 'ghi')

        ext_node = etree.Element(ns_hlp.MSG.tag('Whatever'))
        etree.SubElement(ext_node, 'foo', attrib={'some_attr': 'some_value'})
        etree.SubElement(ext_node, 'bar', attrib={'another_attr': 'different_value'})
        dc.Extension.append(ext_node)
        retrievability = pm_types.Retrievability(
            [
                pm_types.RetrievabilityInfo(pm_types.RetrievabilityMethod.GET),
                pm_types.RetrievabilityInfo(pm_types.RetrievabilityMethod.PERIODIC, update_period=42.0),
            ],
        )
        dc.Extension.append(retrievability.as_etree_node(msg.Retrievability, {}))

        dc2.update_from_other_container(dc)
        self.assertEqual(dc2.DescriptorVersion, 42)
        self.assertEqual(dc2.SafetyClassification, 'MedA')
        self.assertEqual(dc2.Type.Code, dc.Type.Code)
        self.assertEqual(dc2.Type.CodingSystem, dc.Type.CodingSystem)
        self.assertEqual(dc2.Type.CodingSystemVersion, dc.Type.CodingSystemVersion)
        self.assertEqual(dc2.Extension[0], ext_node)
        self.assertEqual(dc2.get_retrievability(), [retrievability])

        node = dc.mk_node(test_tag, self.ns_mapper)
        dc3 = descriptorcontainers.AbstractDescriptorContainer.from_node(node=node, parent_handle='467')
        self.assertIsNotNone(dc3.node)
        self.assertEqual(dc3.DescriptorVersion, 42)
        self.assertEqual(dc3.SafetyClassification, 'MedA')
        self.assertEqual(dc3.Type.Code, dc.Type.Code)
        self.assertEqual(dc3.Type.CodingSystem, dc.Type.CodingSystem)
        self.assertEqual(dc3.Type.CodingSystemVersion, dc.Type.CodingSystemVersion)
        self.assertEqual(dc3.Extension[0].tag, ext_node.tag)
        self.assertEqual(dc3.get_retrievability(), [retrievability])

    def test_abstract_metric_descriptor_container(self):
        dc = descriptorcontainers.AbstractMetricDescriptorContainer(handle='123', parent_handle='456')
        self.assertEqual(dc.MetricAvailability, pm_types.MetricAvailability.CONTINUOUS)  # the default value
        self.assertEqual(dc.MetricCategory, pm_types.MetricCategory.UNSPECIFIED)  # the default value
        self.assertEqual(dc.DeterminationPeriod, None)
        self.assertEqual(dc.MaxMeasurementTime, None)
        self.assertEqual(dc.MaxDelayTime, None)
        dc.Unit = pm_types.CodedValue('abc', 'def')

        # test creation from node
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.AbstractMetricDescriptorContainer.from_node(node=node, parent_handle='467')
        self.assertEqual(dc2.MetricAvailability, pm_types.MetricAvailability.CONTINUOUS)
        self.assertEqual(dc2.MetricCategory, pm_types.MetricCategory.UNSPECIFIED)
        self.assertEqual(dc2.DeterminationPeriod, None)
        self.assertEqual(dc2.MaxMeasurementTime, None)
        self.assertEqual(dc2.MaxDelayTime, None)

        # test update from node
        dc.MetricAvailability = pm_types.MetricAvailability.INTERMITTENT
        dc.MetricCategory = pm_types.MetricCategory.MEASUREMENT

        dc.DeterminationPeriod = 3.5
        dc.MaxMeasurementTime = 2.1
        dc.MaxDelayTime = 4
        dc.BodySite.append(pm_types.CodedValue('ABC', 'DEF'))
        dc.BodySite.append(pm_types.CodedValue('GHI', 'JKL'))
        dc2.update_from_other_container(dc)

        self.assertEqual(dc2.MetricAvailability, pm_types.MetricAvailability.INTERMITTENT)
        self.assertEqual(dc2.MetricCategory, pm_types.MetricCategory.MEASUREMENT)
        self.assertEqual(dc2.DeterminationPeriod, 3.5)
        self.assertEqual(dc2.MaxMeasurementTime, 2.1)
        self.assertEqual(dc2.MaxDelayTime, 4)
        self.assertTrue(_coded_value_comparator(dc2.Unit, dc.Unit))
        self.assertTrue(_list_of_codes_equal(dc2.BodySite, dc.BodySite))
        self.assertTrue(
            _list_of_codes_equal(dc2.BodySite, [pm_types.CodedValue('ABC', 'DEF'), pm_types.CodedValue('GHI', 'JKL')]),
        )

    def test_numeric_metric_descriptor_container(self):
        dc = descriptorcontainers.NumericMetricDescriptorContainer(handle='123', parent_handle='456')
        self.assertEqual(dc.Resolution, None)
        self.assertEqual(dc.AveragingPeriod, None)

    def test_enum_string_metric_descriptor_container(self):
        dc = descriptorcontainers.EnumStringMetricDescriptorContainer(handle='123', parent_handle='456')
        dc.AllowedValue = [pm_types.AllowedValue('abc')]
        dc.Unit = pm_types.CodedValue('abc', 'def')

        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.EnumStringMetricDescriptorContainer.from_node(node=node, parent_handle='467')
        self.assertEqual(dc.AllowedValue, dc2.AllowedValue)

    def _cmp_alert_condition_descriptor_container(
        self,
        dc: descriptorcontainers.AlertConditionDescriptorContainer,
        dc2: descriptorcontainers.AlertConditionDescriptorContainer,
    ):
        self.assertEqual(dc.Source, dc2.Source)
        self.assertEqual(dc.CauseInfo, dc2.CauseInfo)
        self.assertEqual(dc.Kind, dc2.Kind)
        self.assertEqual(dc.Priority, dc2.Priority)

    def test_alert_condition_descriptor_container(self):
        dc = descriptorcontainers.AlertConditionDescriptorContainer(handle='123', parent_handle='456')
        # create copy with default values
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.AlertConditionDescriptorContainer.from_node(node=node, parent_handle='467')
        self._cmp_alert_condition_descriptor_container(dc, dc2)

        # set values, test updateFromNode
        dc.Source = ['A', 'B']
        dc.Cause = [
            pm_types.CauseInfo(
                remedy_info=pm_types.RemedyInfo([pm_types.LocalizedText('abc'), pm_types.LocalizedText('def')]),
                descriptions=[pm_types.LocalizedText('descr1'), pm_types.LocalizedText('descr2')],
            ),
            pm_types.CauseInfo(
                remedy_info=pm_types.RemedyInfo([pm_types.LocalizedText('123'), pm_types.LocalizedText('456')]),
                descriptions=[pm_types.LocalizedText('descr1'), pm_types.LocalizedText('descr2')],
            ),
        ]
        dc.Kind = pm_types.AlertConditionKind.TECHNICAL
        dc.Priority = pm_types.AlertConditionPriority.HIGH
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2.update_from_other_container(dc)
        self._cmp_alert_condition_descriptor_container(dc, dc2)

        # create copy with values set
        dc2 = descriptorcontainers.AlertConditionDescriptorContainer.from_node(node=node, parent_handle='467')
        self._cmp_alert_condition_descriptor_container(dc, dc2)

    def _cmp_limit_alert_condition_descriptor_container(
        self,
        dc: descriptorcontainers.LimitAlertConditionDescriptorContainer,
        dc2: descriptorcontainers.LimitAlertConditionDescriptorContainer,
    ):
        self.assertEqual(dc.MaxLimits, dc2.MaxLimits)
        self.assertEqual(dc.AutoLimitSupported, dc2.AutoLimitSupported)
        self._cmp_alert_condition_descriptor_container(dc, dc2)

    def test_limit_alert_condition_descriptor_container(self):
        dc = descriptorcontainers.LimitAlertConditionDescriptorContainer(handle='123', parent_handle='456')
        # create copy with default values
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.LimitAlertConditionDescriptorContainer.from_node(node=node, parent_handle='467')
        self._cmp_limit_alert_condition_descriptor_container(dc, dc2)

        # set values, test updateFromNode
        dc.MaxLimits = pm_types.Range(*dec_list(0, 100, 1, '0.1', '0.2'))
        dc.AutoLimitSupported = True
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2.update_from_other_container(dc)
        self._cmp_limit_alert_condition_descriptor_container(dc, dc2)

        # create copy with values set
        dc2 = descriptorcontainers.LimitAlertConditionDescriptorContainer.from_node(node=node, parent_handle='467')
        self._cmp_limit_alert_condition_descriptor_container(dc, dc2)

    def test_activate_operation_descriptor_container(self):
        def cmp_activate_operation_descriptor_container(
            _dc: descriptorcontainers.ActivateOperationDescriptorContainer,
            _dc2: descriptorcontainers.ActivateOperationDescriptorContainer,
        ):
            self.assertIsNone(container_diff(_dc, _dc2))
            self.assertEqual(_dc.Argument, _dc2.Argument)
            self.assertEqual(_dc.Retriggerable, _dc2.Retriggerable)

        dc = descriptorcontainers.ActivateOperationDescriptorContainer(handle='123', parent_handle='456')
        dc.OperationTarget = 'my_handle'
        # create copy with default values
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.ActivateOperationDescriptorContainer.from_node(node=node, parent_handle='456')
        cmp_activate_operation_descriptor_container(dc, dc2)

        dc.Argument = [
            pm_types.ActivateOperationDescriptorArgument(
                arg_name=pm_types.CodedValue('abc', 'def'),
                arg=ns_hlp.PM.tag('foo'),
            ),
        ]
        dc2.update_from_other_container(dc)
        cmp_activate_operation_descriptor_container(dc, dc2)

    def test_clock_descriptor_container(self):
        dc = descriptorcontainers.ClockDescriptorContainer(handle='123', parent_handle='456')
        # create copy with default values
        node = dc.mk_node(test_tag, self.ns_mapper)
        dc2 = descriptorcontainers.ClockDescriptorContainer.from_node(node=node, parent_handle='467')
        self.assertEqual(dc.TimeProtocol, dc2.TimeProtocol)
        self.assertEqual(dc.Resolution, dc2.Resolution)

        dc.TimeProtocol = [pm_types.CodedValue('abc', 'def'), pm_types.CodedValue('123', '456')]
        dc.Resolution = 3.14
        dc2.update_from_other_container(dc)
        self.assertEqual(dc.TimeProtocol, dc2.TimeProtocol)
        self.assertEqual(dc.Resolution, dc2.Resolution)
