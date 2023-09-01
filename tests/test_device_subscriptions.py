import os
import time
import unittest
from decimal import Decimal

from sdc11073.xml_types import pm_types, msg_types, pm_qnames as pm
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.mdib import ProviderMdib
from sdc11073.mdib.mdibbase import MdibVersionGroup
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.provider import waveforms, SdcProvider
from sdc11073.provider.components import default_sdc_provider_components_sync
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_types, msg_types, pm_qnames as pm
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from tests import mockstuff

mdib_folder = os.path.dirname(__file__)

# pylint: disable=protected-access

CLIENT_VALIDATE = True

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}


class TestDeviceSubscriptions(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()
        self.logger = get_logger_adapter('sdc.test')
        self.mdib = ProviderMdib.from_mdib_file(os.path.join(mdib_folder, '70041_MDIB_Final.xml'))

        this_model = ThisModelType(manufacturer='ABCDEFG GmbH',
                                   manufacturer_url='www.abcdefg.com',
                                   model_name='Foobar',
                                   model_number='1.0',
                                   model_url='www.abcdefg.com/foobar/model',
                                   presentation_url='www.abcdefg.com/foobar/presentation')
        this_device = ThisDeviceType(friendly_name='Big Bang Practice',
                                     firmware_version='0.99',
                                     serial_number='123serial')

        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_device = SdcProvider(self.wsd, this_model, this_device, self.mdib,
                                      default_components=default_sdc_provider_components_sync)
        self.sdc_device.start_all(periodic_reports_interval=1.0)
        self.logger.info('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        self.logger.info('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.wsd.stop()
        self.sdc_device.stop_all()

    def _verify_proper_namespaces(self, report):
        """We want some namespaces declared only once for small report sizes."""
        import re
        xml_string = self.sdc_device.msg_factory.serialize_message(report).decode('utf-8')
        for ns in (ns_hlp.PM.namespace,
                   ns_hlp.MSG.namespace,
                   ns_hlp.EXT.namespace,
                   ns_hlp.XSI.namespace,):
            occurrences = [i.start() for i in re.finditer(ns, xml_string)]
            self.assertLessEqual(len(occurrences), 1)

    def test_waveformSubscription(self):
        test_subscription = mockstuff.TestDevSubscription([self.sdc_device.mdib.sdc_definitions.Actions.Waveform],
                                                          self.sdc_device._soap_client_pool,
                                                          self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.state_event_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)

        waveform_provider = self.sdc_device.mdib.xtra.waveform_provider

        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.01)

        waveform_provider.register_waveform_generator(HANDLES[0], tr)
        waveform_provider.register_waveform_generator(HANDLES[1], st)
        waveform_provider.register_waveform_generator(HANDLES[2], si)

        time.sleep(3)
        self.assertGreater(len(test_subscription.reports), 20)
        report = test_subscription.reports[-1]
        self._verify_proper_namespaces(report)
        # simulate data transfer from device to client
        xml_bytes = self.sdc_device.msg_factory.serialize_message(report)
        received_response_message = self.sdc_device.msg_reader.read_received_message(xml_bytes)
        expected_action = self.sdc_device.mdib.sdc_definitions.Actions.Waveform
        self.assertEqual(received_response_message.action, expected_action)

    def test_episodicMetricReportEvent(self):
        """ verify that an event message is sent to subscriber and that message is valid"""
        # directly inject a subscription event, this test is not about starting subscriptions
        test_subscription = mockstuff.TestDevSubscription(
            [self.sdc_device.mdib.sdc_definitions.Actions.EpisodicMetricReport],
            self.sdc_device._soap_client_pool,
            self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.state_event_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)

        descriptor_handle = '0x34F00100'  # '0x34F04380'
        first_value = Decimal(12)
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.get_state(descriptor_handle)
            if st.MetricValue is None:
                st.mk_metric_value()
            st.MetricValue.Value = first_value
            st.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID
        self.assertEqual(len(test_subscription.reports), 1)
        response = test_subscription.reports[0]
        self._verify_proper_namespaces(response)

        # simulate data transfer from device to client
        xml_bytes = self.sdc_device.msg_factory.serialize_message(response)
        _ = self.sdc_device.msg_reader.read_received_message(xml_bytes)
        # verify that header contains the identifier of client subscription

    def test_episodicContextReportEvent(self):
        """ verify that an event message is sent to subscriber and that message is valid"""
        # directly inject a subscription event, this test is not about starting subscriptions
        test_subscription = mockstuff.TestDevSubscription(
            [self.sdc_device.mdib.sdc_definitions.Actions.EpisodicContextReport],
            self.sdc_device._soap_client_pool,
            self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.context_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)
        patient_context_descriptor = self.sdc_device.mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)
        descriptor_handle = patient_context_descriptor.Handle
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.mk_context_state(descriptor_handle)
            st.CoreData.PatientType = pm_types.PatientType.ADULT
        self.assertEqual(len(test_subscription.reports), 1)
        response = test_subscription.reports[0]
        self._verify_proper_namespaces(response)

    def test_notifyOperation(self):
        test_subscription = mockstuff.TestDevSubscription(
            [self.sdc_device.mdib.sdc_definitions.Actions.OperationInvokedReport],
            self.sdc_device._soap_client_pool,
            self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.set_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)

        class DummyOperation:
            pass

        dummy_operation = DummyOperation()
        dummy_operation.handle = 'something'
        port_type_impl = self.sdc_device.hosted_services.set_service
        port_type_impl.notify_operation(dummy_operation,
                                        123,
                                        msg_types.InvocationState.FINISHED,
                                        mdib_version_group=MdibVersionGroup(1234,
                                                                            'urn:uuid:abc',
                                                                            None),
                                        error=msg_types.InvocationError.UNSPECIFIED,
                                        error_message='')
        self.assertEqual(len(test_subscription.reports), 1)
