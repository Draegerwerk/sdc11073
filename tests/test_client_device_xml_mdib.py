import copy
import datetime
import logging
import pathlib
import socket
import ssl
import sys
import time
import traceback
import unittest
import unittest.mock
import uuid
from decimal import Decimal
from itertools import product
from http.client import NotConnected
from threading import Event
from lxml import etree as etree_

import sdc11073.certloader
import sdc11073.definitions_sdc
from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073 import observableproperties
from sdc11073.dispatch import RequestDispatcher
from sdc11073.httpserver import compression
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.xml_mdib.xml_consumermdib import XmlConsumerMdib
from sdc11073.xml_mdib.xml_mdibbase import Entity, MultiStateEntity, XmlEntity, XmlMultiStateEntity

from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.pysoap.msgreader import MdibVersionGroupReader
from sdc11073.pysoap.soapclient import HTTPReturnCodeError
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.pysoap.soapenvelope import Soap12Envelope, faultcodeEnum
from sdc11073.xml_types import pm_types, msg_types, msg_qnames as msg, pm_qnames as pm
from sdc11073.xml_types.actions import periodic_actions
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types import pm_qnames
from sdc11073.consumer import SdcConsumer
from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.consumer.subscription import ClientSubscriptionManagerReferenceParams
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.provider.components import (SdcProviderComponents,
                                          default_sdc_provider_components_async,
                                          default_sdc_provider_components_sync)
from sdc11073.provider.subscriptionmgr_async import SubscriptionsManagerReferenceParamAsync
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.namespaces import default_ns_helper
from tests import utils
from tests.mockstuff import SomeDevice, dec_list

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    comm_logger = commlog.DirectoryLogger(log_folder=r'c:\temp\sdc_commlog',
                                          log_out=True,
                                          log_in=True,
                                          broadcast_ip_filter=None)
    comm_logger.start()

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value

# mdib_70041 = '70041_MDIB_Final.xml'
mdib_70041 = '70041_MDIB_multi.xml'


def provide_realtime_data(sdc_device):
    waveform_provider = sdc_device.waveform_provider
    if waveform_provider is None:
        return
    paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveform_period=1.1, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

    flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveform_period=1.2, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

    co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveform_period=1.0, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05506',
                                                  co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

    # make SinusGenerator (0x34F05501) the annotator source
    waveform_provider.add_annotation_generator(pm_types.CodedValue('a', 'b'),
                                               trigger_handle='0x34F05501',
                                               annotated_handles=['0x34F05500', '0x34F05501', '0x34F05506']
                                               )

def runtest_basic_connect(unit_test, sdc_client):
    # simply check that correct top node is returned
    cl_get_service = sdc_client.client('Get')
    get_result = cl_get_service.get_mdib()
    descriptor_containers, state_containers = get_result.result
    unit_test.assertGreater(len(descriptor_containers), 0)
    unit_test.assertGreater(len(state_containers), 0)

    get_result = cl_get_service.get_md_description()
    unit_test.assertGreater(len(get_result.result.MdDescription.Mds), 0)

    get_result = cl_get_service.get_md_state()
    unit_test.assertGreater(len(get_result.result.MdState.State), 0)

    context_service = sdc_client.client('Context')
    get_result = context_service.get_context_states()
    unit_test.assertGreater(len(get_result.result.ContextState), 0)


class Test_Client_SomeDevice(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        self.logger = get_logger_adapter('sdc.test')
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        self.logger.info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_provider = SomeDevice.from_mdib_file(self.wsd, None, mdib_70041,
                                                      default_components=default_sdc_provider_components_async,
                                                      max_subscription_duration=10)  # shorter duration for faster tests
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        self.sdc_provider.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_provider.set_location(utils.random_location(), self._loc_validators)
        provide_realtime_data(self.sdc_provider)

        time.sleep(0.5)  # allow init of devices to complete
        # no deferred action handling for easier debugging
        specific_components = SdcConsumerComponents(
            action_dispatcher_class=RequestDispatcher
        )

        x_addr = self.sdc_provider.get_xaddrs()
        self.sdc_consumer = SdcConsumer(x_addr[0],
                                        sdc_definitions=self.sdc_provider.mdib.sdc_definitions,
                                        ssl_context_container=None,
                                        validate=CLIENT_VALIDATE,
                                        specific_components=specific_components)
        self.sdc_consumer.start_all()  # with periodic reports and system error report
        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        self.logger.info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        try:
            if self.sdc_provider:
                self.sdc_provider.stop_all()
            if self.sdc_consumer:
                self.sdc_consumer.stop_all(unsubscribe=False)
            self.wsd.stop()
        except:
            sys.stderr.write(traceback.format_exc())
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_consumer_xml_mdib(self):
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.descriptions.objects), len(consumer_mdib._entities))

        for handle, xml_entity in consumer_mdib._entities.items():
            self.assertIsInstance(xml_entity, (XmlEntity, XmlMultiStateEntity))
            self.assertIsInstance(xml_entity.node_type, etree_.QName)
            self.assertIsInstance(xml_entity.source_mds, str)

        # needed?
        for handle in consumer_mdib._entities.keys():
            ent = consumer_mdib.handle.get(handle)
            self.assertIsInstance(ent, (Entity, MultiStateEntity))

        # verify that NODETYPE filter works as expected
        consumer_ent_list = consumer_mdib.node_type.get(pm_qnames.VmdDescriptor)
        provider_list = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.VmdDescriptor)
        self.assertEqual(len(provider_list), len(consumer_ent_list))

        # descriptor_handle = '0x34F00100'
        # # set value of a metric
        # first_value = Decimal(12)
        # with self.sdc_provider.mdib.metric_state_transaction() as mgr:
        #     # mgr automatically increases the StateVersion
        #     st = mgr.get_state(descriptor_handle)
        #     if st.MetricValue is None:
        #         st.mk_metric_value()
        #     st.MetricValue.Value = first_value
        #     st.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID
        #
        # time.sleep(1)
        # consumer_entity = consumer_mdib.handle.get(descriptor_handle)
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        # self.assertIsNone(provider_state.diff(consumer_entity.state, max_float_diff=1e-6))

        # # verify that waveform state version of consumer is plausible
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        # consumer_entity = consumer_mdib.handle.get('0x34F05500')
        # self.assertGreaterEqual(consumer_entity.state.StateVersion, provider_state.StateVersion)

    def test_metric_update(self):
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.descriptions.objects), len(consumer_mdib._entities))

        # for handle in consumer_mdib._entities.keys():
        #     # ent = consumer_mdib.mk_entity(handle)
        #     # self.assertIsInstance(ent, (Entity, MultiStateEntity))
        #     # print(ent)
        #     ent = consumer_mdib.handle.get(handle)
        #     self.assertIsInstance(ent, (Entity, MultiStateEntity))
        #
        # consumer_ent_list = consumer_mdib.node_type.get(pm_qnames.VmdDescriptor)
        # provider_list = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.VmdDescriptor)
        # self.assertEqual(len(provider_list), len(consumer_ent_list))
        #
        descriptor_handle = '0x34F00100'
        # set value of a metric
        first_value = Decimal(12)
        with self.sdc_provider.mdib.metric_state_transaction() as mgr:
            # mgr automatically increases the StateVersion
            st = mgr.get_state(descriptor_handle)
            if st.MetricValue is None:
                st.mk_metric_value()
            st.MetricValue.Value = first_value
            st.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID

        time.sleep(1)
        consumer_entity = consumer_mdib.handle.get(descriptor_handle)
        provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        self.assertIsNone(provider_state.diff(consumer_entity.state, max_float_diff=1e-6))

        # # verify that waveform state version of consumer is plausible
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        # consumer_entity = consumer_mdib.handle.get('0x34F05500')
        # self.assertGreaterEqual(consumer_entity.state.StateVersion, provider_state.StateVersion)


    def test_description_modification(self):
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        msg_reader = self.sdc_consumer.msg_reader
        device_mdib = self.sdc_provider.mdib
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)
        metric_descriptor_handle = '0x34F00100'  # a metric
        entity =  consumer_mdib.handle.get(metric_descriptor_handle)
        initial_descriptor_version = entity.descriptor.DescriptorVersion

        # now update something and  wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_consumer,
                                                         'description_modification_report')
        new_determination_period = 3.14159
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            descr = mgr.get_descriptor(metric_descriptor_handle)
            descr.DeterminationPeriod = new_determination_period
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        expected_descriptor_version = initial_descriptor_version + 1

        # verify that client got updates
        entity =  consumer_mdib.handle.get(metric_descriptor_handle)

        self.assertEqual(entity.descriptor.DescriptorVersion, expected_descriptor_version)
        self.assertEqual(entity.descriptor.DeterminationPeriod, new_determination_period)
        self.assertEqual(entity.state.DescriptorVersion, expected_descriptor_version)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # # verify that devices mdib contains the updated descriptor_container
        # # plus an updated state wit correct DescriptorVersion
        # descriptor_container = device_mdib.descriptions.handle.get_one(descriptor_handle)
        # state_container = device_mdib.states.descriptor_handle.get_one(descriptor_handle)
        # self.assertEqual(descriptor_container.DescriptorVersion, expected_descriptor_version)
        # self.assertEqual(descriptor_container.DeterminationPeriod, new_determination_period)
        # self.assertEqual(state_container.DescriptorVersion, expected_descriptor_version)


        channel_descriptor_handle = '2.1.6.1'  # a channel
        xml_entity =  consumer_mdib._entities[channel_descriptor_handle]
        initial_descriptor_version = int(xml_entity.descriptor.attrib.get('DescriptorVersion', 0))

        children_with_handle = [ (idx, node.attrib['Handle']) for idx, node in enumerate(xml_entity.descriptor) if 'Handle' in node.attrib.keys()]

        parent_handle = xml_entity.descriptor.getparent().attrib['Handle']
        # now update something and  wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_consumer,
                                                         'description_modification_report')
        new_concept_description = 'foo bar'
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            descr = mgr.get_descriptor(channel_descriptor_handle)
            descr.Type.ConceptDescription[0].text = new_concept_description
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        expected_descriptor_version = initial_descriptor_version + 1

        xml_entity = consumer_mdib._entities[channel_descriptor_handle]
        updated_parent_handle = xml_entity.descriptor.getparent().attrib['Handle']
        self.assertEqual(parent_handle, updated_parent_handle)

        updated_children_with_handle = [ (idx, node.attrib['Handle']) for idx, node in enumerate(xml_entity.descriptor) if 'Handle' in node.attrib.keys()]
        self.assertEqual(children_with_handle, updated_children_with_handle)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # verify that client entity got updates
        entity =  consumer_mdib.handle.get(channel_descriptor_handle)
        self.assertEqual(entity.descriptor.DescriptorVersion, expected_descriptor_version)
        self.assertEqual(entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(entity.state.DescriptorVersion, expected_descriptor_version)

        # test creating a numeric descriptor
        # coll: wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'description_modification_report')
        new_handle = 'a_generated_descriptor'
        node_name = pm.NumericMetricDescriptor
        cls = self.sdc_provider.mdib.data_model.get_descriptor_container_class(node_name)
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            new_descriptor_container = cls(handle=new_handle,
                                           parent_handle=channel_descriptor_handle
                                           )
            new_descriptor_container.Type = pm_types.CodedValue('12345')
            new_descriptor_container.Unit = pm_types.CodedValue('hector')
            new_descriptor_container.Resolution = Decimal('0.42')
            mgr.add_descriptor(new_descriptor_container)
            cls = self.sdc_provider.mdib.data_model.get_state_container_class(new_descriptor_container.STATE_QNAME)
            state = cls(new_descriptor_container)
            mgr.add_state(state)
        # long timeout, sometimes high load on jenkins makes these tests fail
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity =  consumer_mdib.handle.get(new_handle)
        # cl_descriptor_container = client_mdib.descriptions.handle.get_one(new_handle, allow_none=True)
        self.assertEqual(entity.descriptor.Handle, new_handle)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # test creating a battery descriptor
        mds_descriptor_handle = '3569'  # a channel

        # coll: wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'description_modification_report')
        new_handle = 'another_generated_descriptor'
        node_name = pm.BatteryDescriptor
        cls = self.sdc_provider.mdib.data_model.get_descriptor_container_class(node_name)
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            new_descriptor_container = cls(handle=new_handle,
                                           parent_handle=mds_descriptor_handle
                                           )
            new_descriptor_container.Type = pm_types.CodedValue('23456')
            mgr.add_descriptor(new_descriptor_container)
            cls = self.sdc_provider.mdib.data_model.get_state_container_class(new_descriptor_container.STATE_QNAME)
            state = cls(new_descriptor_container)
            mgr.add_state(state)
        # long timeout, sometimes high load on jenkins makes these tests fail
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity = consumer_mdib.handle.get(new_handle)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # cl_descriptor_container = client_mdib.descriptions.handle.get_one(new_handle, allow_none=True)
        self.assertEqual(entity.descriptor.Handle, new_handle)

        # test deleting a descriptor
        coll = observableproperties.SingleValueCollector(self.sdc_consumer,
                                                         'description_modification_report')
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.remove_descriptor(new_handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity =  consumer_mdib.handle.get(new_handle)
        self.assertIsNone(entity)
