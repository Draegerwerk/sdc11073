from __future__ import annotations
import logging
import sys
import time
import traceback
import unittest.mock
import copy
import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from lxml import etree as etree_

from sdc11073 import loghelper
from sdc11073 import observableproperties
from sdc11073.consumer import SdcConsumer
from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.dispatch import RequestDispatcher
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.provider.components import (default_sdc_provider_components_async, SdcProviderComponents)
from sdc11073.provider.sco import AbstractScoOperationsRegistry
from sdc11073.roles.product import BaseProduct
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_mdib.xml_consumermdib import XmlConsumerMdib
from sdc11073.xml_mdib.xml_entities import ConsumerEntity, ConsumerMultiStateEntity, XmlEntity, XmlMultiStateEntity
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types import pm_types, pm_qnames as pm
from tests import utils
from tests.mockstuff import SomeDeviceXmlMdib, SomeDevice


if TYPE_CHECKING:
    from sdc11073.xml_mdib.xml_entities import ProviderMultiStateEntity


CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value

# mdib_70041 = '70041_MDIB_Final.xml'
mdib_70041 = '70041_MDIB_multi.xml'


class EmptyProduct(BaseProduct):
    def _register_existing_mdib_operations(self, sco: AbstractScoOperationsRegistry):
        pass

    def init_operations(self):
        pass

my_sdc_provider_components_async = copy.deepcopy(default_sdc_provider_components_async)
my_sdc_provider_components_async.role_provider_class = EmptyProduct  # no role providers

def provide_realtime_data(sdc_device):
    waveform_provider = sdc_device.waveform_provider
    if waveform_provider is None:
        return
    paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveform_period=1.1, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05500', paw)

    flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveform_period=1.2, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05501', flow)

    co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveform_period=1.0, sample_period=0.01)
    waveform_provider.register_waveform_generator('0x34F05506', co2)

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


class Test_Client_SomeDeviceXml(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        self.logger = get_logger_adapter('sdc.test')
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        self.logger.info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_provider = SomeDeviceXmlMdib.from_mdib_file(self.wsd, None, mdib_70041,
                                                      default_components=my_sdc_provider_components_async,
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

    def add_random_patient(self, count: int = 1) -> [ProviderMultiStateEntity, list]:
        ret = []
        entities = self.sdc_provider.mdib.entities.node_type(pm.PatientContextDescriptor)
        if len(entities) != 1:
            raise ValueError(f'cannot handle {len(entities)} instances of PatientContextDescriptor')
        # patientDescriptorContainer = self.sdc_provider.mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)
        entity = entities[0]
        for i in range(count):
            st = self.sdc_provider.mdib.entities.new_state(entity)
            st.CoreData.Givenname = f'Max{i}'
            st.CoreData.Middlename = ['Willy']
            st.CoreData.Birthname = f'Mustermann{i}'
            st.CoreData.Familyname = f'Musterfrau{i}'
            st.CoreData.Title = 'Rex'
            st.CoreData.Sex = pm_types.Sex.MALE
            st.CoreData.PatientType = pm_types.PatientType.ADULT
            st.CoreData.Height = pm_types.Measurement(Decimal('88.2'), pm_types.CodedValue('abc', 'def'))
            st.CoreData.Weight = pm_types.Measurement(Decimal('68.2'), pm_types.CodedValue('abc'))
            st.CoreData.Race = pm_types.CodedValue('123', 'def')
            st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)
            ret.append(st)

        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            for st in ret:
                mgr.add_state(st)
        return entity, ret

    def test_consumer_xml_mdib(self):
        patient_descriptor_entity, _ = self.add_random_patient(2)
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        # check sequence_id and instance_id
        self.assertEqual(consumer_mdib.sequence_id, self.sdc_provider.mdib.sequence_id)
        self.assertEqual(consumer_mdib.instance_id, self.sdc_provider.mdib.instance_id)

        # check difference of mdib versions (consumer is allowed to be max. one smaller
        self.assertLess(self.sdc_provider.mdib.mdib_version - consumer_mdib.mdib_version, 2)
        # check also in DOM tree
        self.assertLess(self.sdc_provider.mdib.mdib_version
                        - int(consumer_mdib._get_mdib_response_node.get('MdibVersion')), 2)
        self.assertLess(self.sdc_provider.mdib.mdib_version
                        - int(consumer_mdib._get_mdib_response_node[0].get('MdibVersion')), 2)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        for handle, xml_entity in consumer_mdib._entities.items():
            self.assertIsInstance(xml_entity, (XmlEntity, XmlMultiStateEntity))
            self.assertIsInstance(xml_entity.node_type, etree_.QName)
            self.assertIsInstance(xml_entity.source_mds, str)

        # needed?
        for handle in consumer_mdib._entities.keys():
            ent = consumer_mdib.entities.handle(handle)
            self.assertIsInstance(ent, (ConsumerEntity, ConsumerMultiStateEntity))

        # verify that NODETYPE filter works as expected
        consumer_ent_list = consumer_mdib.entities.node_type(pm_qnames.VmdDescriptor)
        # provider_list = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm_qnames.VmdDescriptor)
        provider_list = self.sdc_provider.mdib.entities.node_type(pm_qnames.VmdDescriptor)
        self.assertEqual(len(provider_list), len(consumer_ent_list))

        # test update method of entities
        metric_descriptor_handle = '0x34F00100'
        consumer_metric_entity = consumer_mdib.entities.handle(metric_descriptor_handle)
        descriptor_version = consumer_metric_entity.descriptor.DescriptorVersion
        state_version = consumer_metric_entity.state.StateVersion
        consumer_metric_entity.descriptor.DescriptorVersion += 1
        consumer_metric_entity.state.StateVersion += 1
        consumer_metric_entity.update()
        self.assertEqual(descriptor_version, consumer_metric_entity.descriptor.DescriptorVersion)
        self.assertEqual(state_version, consumer_metric_entity.state.StateVersion)

        # calling update with deleted xml entity source shall raise an error
        del consumer_mdib._entities[metric_descriptor_handle]
        self.assertRaises(ValueError, consumer_metric_entity.update)

        # # check multi state entity
        # context_descriptor_handle = patient_descriptor_entity.descriptor.Handle
        # context_consumer_entity = consumer_mdib.entities.handle(context_descriptor_handle)
        # self.assertEqual(2, len(context_consumer_entity.states))
        # self.assertEqual(set(context_consumer_entity.states), set(patient_descriptor_entity))
        # descriptor_version = context_consumer_entity.descriptor.DescriptorVersion
        # state_versions = [(st.Handle,st.StateVersion) for st in context_consumer_entity.states.values()]
        # context_consumer_entity.descriptor.DescriptorVersion += 1
        # del context_consumer_entity.states[0]
        # context_consumer_entity.update()
        # self.assertEqual(descriptor_version, context_consumer_entity.descriptor.DescriptorVersion)
        # updated_state_versions = [(st.Handle,st.StateVersion) for st in context_consumer_entity.states.values()]
        # self.assertEqual(state_versions, updated_state_versions)

        # same for multi state entity
        context_descriptor_handle = patient_descriptor_entity.descriptor.Handle
        context_consumer_entity = consumer_mdib.entities.handle(context_descriptor_handle)
        del consumer_mdib._entities[context_descriptor_handle]
        self.assertRaises(ValueError, context_consumer_entity.update)


    def test_metric_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        descriptor_handle = '0x34F00100'
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'metric_handles')

        # set value of a metric
        first_value = Decimal(12)
        provider_entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
        st = provider_entity.state
        old_state_version = st.StateVersion
        if st.MetricValue is None:
            st.mk_metric_value()
        st.MetricValue.Value = first_value
        st.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID

        with self.sdc_provider.mdib.metric_state_transaction() as mgr:
            # mgr automatically increases the StateVersion
            mgr.add_state(st)

        # time.sleep(1)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        self.assertEqual(provider_entity.state.StateVersion, old_state_version + 1)
        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_alert_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        # self.assertEqual(len(self.sdc_provider.mdib.descriptions.objects), len(consumer_mdib._entities))
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        descriptor_handle = '0xD3C00108'  # a LimitAlertCondition

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'alert_handles')

        with self.sdc_provider.mdib.alert_state_transaction() as mgr:
            # mgr automatically increases the StateVersion
            entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
            # st = mgr.get_state(descriptor_handle)
            entity.state.ActivationState = pm_types.AlertActivation.PAUSED
            entity.state.ActualPriority = pm_types.AlertConditionPriority.MEDIUM
            mgr.add_state(entity.state)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        provider_entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_component_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        # self.assertEqual(len(self.sdc_provider.mdib.descriptions.objects), len(consumer_mdib._entities))

        descriptor_handle = '2.1.2.1'  # a Channel

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'component_handles')

        provider_entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
        old_state_version = provider_entity.state.StateVersion
        with self.sdc_provider.mdib.component_state_transaction() as mgr:
            provider_entity.state.ActivationState = pm_types.ComponentActivation.FAILURE
            mgr.add_state(provider_entity.state)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        self.assertEqual(provider_entity.state.StateVersion, old_state_version +1)
        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)


    def test_operational_state_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib._entities))

        descriptor_handle = 'SVO.37.3569'  # an Activate operation

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'operation_handles')
        provider_entity = self.sdc_provider.mdib.entities.handle((descriptor_handle))
        provider_entity.state.OperatingMode = pm_types.OperatingMode.NA

        with self.sdc_provider.mdib.operational_state_transaction() as mgr:
            mgr.add_state(provider_entity.state)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        provider_entity.update()

        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_remove_mds(self):
        msg_reader = self.sdc_consumer.msg_reader
        self.sdc_provider.stop_realtime_sample_loop()
        time.sleep(0.1)
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        dev_descriptor_count1 = len(self.sdc_provider.mdib.entities)
        descr_handles = list(self.sdc_provider.mdib._entities.keys())
        # state_descriptor_handles = list(self.sdc_provider.mdib.states.descriptor_handle.keys())
        # context_state_handles = list(self.sdc_provider.mdib.context_states.handle.keys())

        coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'description_modification_report')
        mds_entities = self.sdc_provider.mdib.entities.node_type(pm.MdsDescriptor)
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            for entity in mds_entities:
                mgr.remove_descriptor(entity.descriptor.Handle)
            # mds_descriptors = self.sdc_provider.mdib.descriptions.NODETYPE.get(pm.MdsDescriptor)
            # for descr in mds_descriptors:
            #     mgr.remove_descriptor(descr.Handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        # verify that all state versions were saved
        descr_handles_lookup1 = copy.copy(self.sdc_provider.mdib.descriptions.handle_version_lookup)
        state_descriptor_handles_lookup1 = copy.copy(self.sdc_provider.mdib.states.handle_version_lookup)
        context_state_descriptor_handles_lookup1 = copy.copy(self.sdc_provider.mdib.context_states.handle_version_lookup)
        for h in descr_handles:
            self.assertTrue(h in descr_handles_lookup1)
        for h in state_descriptor_handles:
            self.assertTrue(h in state_descriptor_handles_lookup1)
        for h in context_state_handles:
            self.assertTrue(h in context_state_descriptor_handles_lookup1)

        # verify that client mdib has same number of objects as device mdib
        dev_descriptor_count2 = len(self.sdc_provider.mdib.descriptions.objects)
        cl_descriptor_count2 = len(consumer_mdib._entities)
        self.assertTrue(dev_descriptor_count2 < dev_descriptor_count1)
        self.assertEqual(dev_descriptor_count2, 0)
        self.assertEqual(dev_descriptor_count2, cl_descriptor_count2)
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_set_patient_context_on_device(self):
        """device updates patient.
         verify that a notification device->client updates the client mdib."""
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        entities = self.sdc_provider.mdib.entities.node_type(pm.PatientContextDescriptor)
        self.assertEqual(len(entities), 1)
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'context_handles')
        provider_entity, states = self.add_random_patient(1)  # this runs a transaction
        st_handle  = states[0].Handle
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        # entity = consumer_mdib.entities.handle(patientDescriptorContainer.Handle)
        provider_state = provider_entity.states[st_handle]
        consumer_entity = consumer_mdib.entities.handle(provider_entity.descriptor.Handle)
        consumer_state = consumer_entity.states[st_handle]
        self.assertIsNone(consumer_state.diff(provider_state, max_float_diff=1e-6))

        # test update of same patient
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'context_handles')
        provider_entity.update()

        provider_state = provider_entity.states[st_handle]
        provider_state.CoreData.Givenname = 'Moritz'
        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            mgr.add_state(provider_state)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        time.sleep(1)
        provider_entity.update()
        provider_state = provider_entity.states[st_handle]
        consumer_entity.update()
        consumer_state = consumer_entity.states[st_handle]
        self.assertIsNone(consumer_state.diff(provider_state, max_float_diff=1e-6))


    def test_description_modification(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = XmlConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        metric_descriptor_handle = '0x34F00100'  # a metric
        consumer_entity = consumer_mdib.entities.handle(metric_descriptor_handle)
        initial_descriptor_version = consumer_entity.descriptor.DescriptorVersion
        initial_state_version = consumer_entity.state.StateVersion

        # now update a metric descriptor and wait for the next DescriptionModificationReport
        # coll = observableproperties.SingleValueCollector(self.sdc_consumer,
        #                                                  'description_modification_report')
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')

        new_determination_period = 3.14159
        provider_entity = self.sdc_provider.mdib.entities.handle(metric_descriptor_handle)
        provider_entity.descriptor.DeterminationPeriod = new_determination_period
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(provider_entity)
            # descr = mgr.get_descriptor(metric_descriptor_handle)
            # descr.DeterminationPeriod = new_determination_period
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        # verify that client got updates
        # entity = consumer_mdib.entities.handle(metric_descriptor_handle)
        consumer_entity.update()
        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.DeterminationPeriod, new_determination_period)
        self.assertEqual(consumer_entity.state.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # now update a channel descriptor and wait for the next DescriptionModificationReport
        channel_descriptor_handle = '2.1.6.1'  # a channel
        consumer_entity = consumer_mdib.entities.handle(channel_descriptor_handle)
        # # initial_descriptor_version = int(xml_entity.descriptor.attrib.get('DescriptorVersion', 0))
        #
        # children_with_handle = [(idx, node.attrib['Handle']) for idx, node in enumerate(xml_entity.descriptor) if
        #                         'Handle' in node.attrib.keys()]
        #
        # parent_handle = xml_entity.descriptor.getparent().attrib['Handle']
        # now update something and  wait for the next DescriptionModificationReport
        # coll = observableproperties.SingleValueCollector(self.sdc_consumer,
        #                                                  'description_modification_report')
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')
        new_concept_description = 'foo bar'
        provider_entity = self.sdc_provider.mdib.entities.handle(channel_descriptor_handle)
        provider_entity.descriptor.Type.ConceptDescription[0].text = new_concept_description
        initial_descriptor_version = provider_entity.descriptor.DescriptorVersion
        initial_state_version = provider_entity.state.StateVersion

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(provider_entity)

        provider_entity.update()
        self.assertEqual(provider_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(provider_entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(provider_entity.state.StateVersion, initial_state_version + 1)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        # time.sleep(0.01)

        consumer_entity.update()

        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(consumer_entity.state.DescriptorVersion, consumer_entity.descriptor.DescriptorVersion)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)


        # expected_descriptor_version = initial_descriptor_version + 1

        # updated_parent_handle = xml_entity.descriptor.getparent().attrib['Handle']
        # self.assertEqual(parent_handle, updated_parent_handle)
        #
        # updated_children_with_handle = [(idx, node.attrib['Handle']) for idx, node in enumerate(xml_entity.descriptor)
        #                                 if 'Handle' in node.attrib.keys()]
        # self.assertEqual(children_with_handle, updated_children_with_handle)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # verify that client entity got updates
        # entity = consumer_mdib.entities.handle(channel_descriptor_handle)
        # self.assertEqual(entity.descriptor.DescriptorVersion, expected_descriptor_version)
        # self.assertEqual(entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        # self.assertEqual(entity.state.DescriptorVersion, expected_descriptor_version)

        # test creating a numeric descriptor
        # coll: wait for the next DescriptionModificationReport
        # coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'description_modification_report')
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'new_descriptors_handles')

        new_handle = 'a_generated_descriptor'
        node_name = pm.NumericMetricDescriptor
        # cls = self.sdc_provider.mdib.data_model.get_descriptor_container_class(node_name)
        # new_descriptor_container = cls(handle=new_handle,
        #                                parent_handle=channel_descriptor_handle
        #                                )

        new_entity = self.sdc_provider.mdib.entities.new_entity(pm.NumericMetricDescriptor,
                                                                new_handle,
                                                                channel_descriptor_handle)
        new_entity.descriptor.Type = pm_types.CodedValue('12345')
        new_entity.descriptor.Unit = pm_types.CodedValue('hector')
        new_entity.descriptor.Resolution = Decimal('0.42')

        # verify that it is possible to create an entity with same handle twice
        self.assertRaises(ValueError, self.sdc_provider.mdib.entities.new_entity,
                                  pm.NumericMetricDescriptor,
                                  new_handle,
                                  channel_descriptor_handle
                                  )

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(new_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        new_consumer_entity = consumer_mdib.entities.handle(new_handle)
        self.assertEqual(new_consumer_entity.descriptor.Resolution, Decimal('0.42'))
        # self.assertEqual(new_consumer_entity.descriptor.Handle, new_handle)
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # test creating a battery descriptor
        mds_descriptor_handle = '3569'  # a channel

        # coll: wait for the next DescriptionModificationReport
        # coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'description_modification_report')
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'new_descriptors_handles')
        new_battery_handle = 'new_battery_handle'
        node_name = pm.BatteryDescriptor
        new_entity = self.sdc_provider.mdib.entities.new_entity(node_name,
                                                                new_battery_handle,
                                                                mds_descriptor_handle)
        new_entity.descriptor.Type = pm_types.CodedValue('23456')

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(new_entity)
        # long timeout, sometimes high load on jenkins makes these tests fail
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        consumer_entity = consumer_mdib.entities.handle(new_battery_handle)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # cl_descriptor_container = client_mdib.descriptions.handle.get_one(new_handle, allow_none=True)
        self.assertEqual(consumer_entity.descriptor.Handle, new_battery_handle)

        # test deleting a descriptor
        # coll = observableproperties.SingleValueCollector(self.sdc_consumer,
        #                                                  'description_modification_report')
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'deleted_descriptors_handles')
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.remove_descriptor(channel_descriptor_handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity = consumer_mdib.entities.handle(new_handle)
        self.assertIsNone(entity)
