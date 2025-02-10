"""The module tests functionality between consumer and provider, both using entity based mdibs."""
from __future__ import annotations

import datetime
import logging
import sys
import time
import traceback
import unittest.mock
from decimal import Decimal
from itertools import cycle
from typing import TYPE_CHECKING

from lxml import etree as etree_

from sdc11073 import loghelper, observableproperties
from sdc11073 import commlog
from sdc11073.consumer import SdcConsumer
from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.dispatch import RequestDispatcher
from sdc11073.entity_mdib.entities import ConsumerEntity, ConsumerMultiStateEntity, XmlEntity, XmlMultiStateEntity
from sdc11073.entity_mdib.entity_consumermdib import EntityConsumerMdib
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_qnames, pm_types
from sdc11073.xml_types import pm_qnames as pm
from tests import utils
from tests.mockstuff import SomeDeviceEntityMdib

if TYPE_CHECKING:
    from sdc11073.entity_mdib.entities import ProviderMultiStateEntity
    from sdc11073.provider import SdcProvider

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


default_mdib_file = 'mdib_two_mds.xml'


def provide_realtime_data(sdc_provider: SdcProvider):
    waveform_provider = sdc_provider.waveform_provider
    if waveform_provider is None:
        return
    iterator = cycle([waveforms.SawtoothGenerator,
                     waveforms.SinusGenerator,
                     waveforms.TriangleGenerator])
    waveform_entities = sdc_provider.mdib.entities.by_node_type(pm_qnames.RealTimeSampleArrayMetricDescriptor)
    for i, waveform_entity in enumerate(waveform_entities):
        cls = iterator.__next__()
        gen = cls(min_value=1, max_value=i+10, waveform_period=1.1, sample_period=0.01)
        waveform_provider.register_waveform_generator(waveform_entity.handle, gen)

        if i == 2:
            # make this generator the annotator source
            waveform_provider.add_annotation_generator(pm_types.CodedValue('a', 'b'),
                                                       trigger_handle=waveform_entity.handle,
                                                       annotated_handles=[waveform_entities[0].handle],
                                                       )


class TestClientSomeDeviceXml(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        self.logger = get_logger_adapter('sdc.test')
        sys.stderr.write(f'\n############### start setUp {self._testMethodName} ##############\n'.format())
        self.logger.info('############### start setUp %s ##############', self._testMethodName)
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_provider: SomeDeviceEntityMdib | None = None
        self.sdc_consumer: SdcConsumer | None = None
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def _init_provider_consumer(self, mdib_file: str = default_mdib_file):
        self.sdc_provider = SomeDeviceEntityMdib.from_mdib_file(
            self.wsd, None, mdib_file, max_subscription_duration=10)  # shorter duration for faster tests
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        self.sdc_provider.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_provider.set_location(utils.random_location(), self._loc_validators)
        provide_realtime_data(self.sdc_provider)

        time.sleep(0.5)  # allow init of devices to complete
        # no deferred action handling for easier debugging
        specific_components = SdcConsumerComponents(
            action_dispatcher_class=RequestDispatcher,
        )

        x_addr = self.sdc_provider.get_xaddrs()
        self.sdc_consumer = SdcConsumer(x_addr[0],
                                        sdc_definitions=self.sdc_provider.mdib.sdc_definitions,
                                        ssl_context_container=None,
                                        validate=CLIENT_VALIDATE,
                                        specific_components=specific_components)
        self.sdc_consumer.start_all()  # with periodic reports and system error report
        time.sleep(1)
        sys.stderr.write(f'\n############### setUp done {self._testMethodName} ##############\n')
        self.logger.info('############### setUp done %s ##############', self._testMethodName)
        time.sleep(0.5)

    def tearDown(self):
        sys.stderr.write(f'############### tearDown {self._testMethodName}... ##############\n')
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
        sys.stderr.write(f'############### tearDown {self._testMethodName} done ##############\n')

    def add_random_patient(self, count: int = 1) -> tuple[ProviderMultiStateEntity, list]:
        new_states = []
        entities = self.sdc_provider.mdib.entities.by_node_type(pm.PatientContextDescriptor)
        if len(entities) != 1:
            msg = f'cannot handle {len(entities)} instances of PatientContextDescriptor'
            raise ValueError(msg)
        entity = entities[0]
        handles = []
        for i in range(count):
            st = entity.new_state()
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
            st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)  # noqa: DTZ001
            handles.append(st.Handle)
            new_states.append(st)

        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            mgr.write_entity(entity, handles)
        return entity, new_states

    def test_consumer_xml_mdib(self):
        self._init_provider_consumer()
        patient_descriptor_entity, _ = self.add_random_patient(2)
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297, maintain_xml_tree=True)
        consumer_mdib.init_mdib()

        # check sequence_id and instance_id
        self.assertEqual(consumer_mdib.sequence_id, self.sdc_provider.mdib.sequence_id)
        self.assertEqual(consumer_mdib.instance_id, self.sdc_provider.mdib.instance_id)

        # check difference of mdib versions (consumer is allowed to be max. one smaller)
        self.assertLess(self.sdc_provider.mdib.mdib_version - consumer_mdib.mdib_version, 2)
        if consumer_mdib._maintain_xml_tree:
            # check also in DOM tree
            self.assertLess(self.sdc_provider.mdib.mdib_version
                            - int(consumer_mdib.get_mdib_response_node.get('MdibVersion')), 2)
            self.assertLess(self.sdc_provider.mdib.mdib_version
                            - int(consumer_mdib.get_mdib_response_node[0].get('MdibVersion')), 2)

            msg_reader._validate_node(consumer_mdib.get_mdib_response_node)
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        for xml_entity in consumer_mdib._entities.values():
            self.assertIsInstance(xml_entity, (XmlEntity, XmlMultiStateEntity))
            self.assertIsInstance(xml_entity.node_type, etree_.QName)
            self.assertIsInstance(xml_entity.source_mds, str)

        # needed?
        for handle in consumer_mdib._entities:
            ent = consumer_mdib.entities.by_handle(handle)
            self.assertIsInstance(ent, (ConsumerEntity, ConsumerMultiStateEntity))

        # verify that NODETYPE filter works as expected
        consumer_ent_list = consumer_mdib.entities.by_node_type(pm_qnames.VmdDescriptor)
        provider_list = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.VmdDescriptor)
        self.assertEqual(len(provider_list), len(consumer_ent_list))

        # test update method of entities
        metric_entities = consumer_mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
        consumer_metric_entity = metric_entities[0]
        descriptor_version = consumer_metric_entity.descriptor.DescriptorVersion
        state_version = consumer_metric_entity.state.StateVersion
        consumer_metric_entity.descriptor.DescriptorVersion += 1
        consumer_metric_entity.state.StateVersion += 1
        consumer_metric_entity.update()
        self.assertEqual(descriptor_version, consumer_metric_entity.descriptor.DescriptorVersion)
        self.assertEqual(state_version, consumer_metric_entity.state.StateVersion)

        # calling update with deleted xml entity source shall raise an error
        del consumer_mdib._entities[consumer_metric_entity.handle]
        self.assertRaises(ValueError, consumer_metric_entity.update)

        # same for multi state entity
        context_descriptor_handle = patient_descriptor_entity.descriptor.Handle
        context_consumer_entity = consumer_mdib.entities.by_handle(context_descriptor_handle)
        del consumer_mdib._entities[context_descriptor_handle]
        self.assertRaises(ValueError, context_consumer_entity.update)

    def test_metric_update(self):
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        metric_entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
        provider_entity = metric_entities[0]

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'metric_handles')

        # set value of a metric
        first_value = Decimal(12)
        st = provider_entity.state
        old_state_version = st.StateVersion
        if st.MetricValue is None:
            st.mk_metric_value()
        st.MetricValue.Value = first_value
        st.MetricValue.MetricQuality.Validity = pm_types.MeasurementValidity.VALID

        with self.sdc_provider.mdib.metric_state_transaction() as mgr:
            # mgr automatically increases the StateVersion
            mgr.write_entity(provider_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        self.assertEqual(provider_entity.state.StateVersion, old_state_version + 1)
        consumer_entity = consumer_mdib.entities.by_handle(provider_entity.handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))

    def test_alert_update(self):
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib.entities))

        provider_entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.AlertConditionDescriptor)
        provider_entity = provider_entities[0]
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'alert_handles')

        with self.sdc_provider.mdib.alert_state_transaction() as mgr:
            # mgr automatically increases the StateVersion
            provider_entity.state.ActivationState = pm_types.AlertActivation.PAUSED
            provider_entity.state.ActualPriority = pm_types.AlertConditionPriority.MEDIUM
            mgr.write_entity(provider_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()  # update to get correct version counters
        consumer_entity = consumer_mdib.entities.by_handle(provider_entity.handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))

    def test_component_update(self):
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        channel_entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.ChannelDescriptor)
        provider_channel_entity = channel_entities[0]
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'component_handles')

        old_state_version = provider_channel_entity.state.StateVersion
        with self.sdc_provider.mdib.component_state_transaction() as mgr:
            provider_channel_entity.state.ActivationState = pm_types.ComponentActivation.FAILURE
            mgr.write_entity(provider_channel_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_channel_entity.update()
        self.assertEqual(provider_channel_entity.state.StateVersion, old_state_version + 1)
        consumer_channel_entity = consumer_mdib.entities.by_handle(provider_channel_entity.handle)
        self.assertIsNone(provider_channel_entity.state.diff(consumer_channel_entity.state, max_float_diff=1e-6))

    def test_operational_state_update(self):
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib._entities))

        entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.ActivateOperationDescriptor)
        provider_entity = entities[0]
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'operation_handles')
        provider_entity.state.OperatingMode = pm_types.OperatingMode.NA

        with self.sdc_provider.mdib.operational_state_transaction() as mgr:
            mgr.write_entity(provider_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity = consumer_mdib.entities.by_handle(provider_entity.handle)
        provider_entity.update()

        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))

    def test_remove_mds(self):
        self._init_provider_consumer()
        self.sdc_provider.stop_realtime_sample_loop()
        time.sleep(0.1)
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        # get all versions
        descriptor_versions = {}
        state_versions = {}
        for handle, entity in self.sdc_provider.mdib._entities.items():
            descriptor_versions[handle] = entity.descriptor.DescriptorVersion
            if entity.is_multi_state:
                for state in entity.states.values():
                    state_versions[state.Handle] = state.StateVersion
            else:
                state_versions[handle] = entity.state.StateVersion

        # now remove all
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'deleted_descriptors_handles')
        mds_entities = self.sdc_provider.mdib.entities.by_node_type(pm.MdsDescriptor)
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            for entity in mds_entities:
                mgr.remove_entity(entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        # verify both mdibs are empty
        self.assertEqual(len(self.sdc_provider.mdib.entities), 0)
        self.assertEqual(len(consumer_mdib.entities), 0)
        # verify all version info is saved
        self.assertEqual(descriptor_versions, self.sdc_provider.mdib.descr_handle_version_lookup)
        self.assertEqual(state_versions, self.sdc_provider.mdib.state_handle_version_lookup)

    def test_set_patient_context_on_device(self):
        """Verify that device updates patient.

        Verify that a notification device->client updates the client mdib.
        """
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        entities = self.sdc_provider.mdib.entities.by_node_type(pm.PatientContextDescriptor)
        self.assertEqual(len(entities), 1)
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'context_handles')
        provider_entity, states = self.add_random_patient(1)  # this runs a transaction
        st_handle = states[0].Handle
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        provider_state = provider_entity.states[st_handle]
        consumer_entity = consumer_mdib.entities.by_context_handle(st_handle)
        consumer_state = consumer_entity.states[st_handle]
        self.assertIsNone(consumer_state.diff(provider_state, max_float_diff=1e-6))

        # test update of same patient
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'context_handles')
        provider_entity.update()

        provider_state = provider_entity.states[st_handle]
        provider_state.CoreData.Givenname = 'Moritz'
        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            mgr.write_entity(provider_entity, [st_handle])
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        time.sleep(1)
        provider_entity.update()
        provider_state = provider_entity.states[st_handle]
        consumer_entity.update()
        consumer_state = consumer_entity.states[st_handle]
        self.assertIsNone(consumer_state.diff(provider_state, max_float_diff=1e-6))

    def test_description_modification(self):
        self._init_provider_consumer()
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297, maintain_xml_tree=True)
        consumer_mdib.init_mdib()

        metric_entities = consumer_mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
        consumer_entity = metric_entities[0]

        initial_descriptor_version = consumer_entity.descriptor.DescriptorVersion
        initial_state_version = consumer_entity.state.StateVersion

        # now update a metric descriptor and wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')

        new_determination_period = 3.14159
        provider_entity = self.sdc_provider.mdib.entities.by_handle(consumer_entity.handle)
        provider_entity.descriptor.DeterminationPeriod = new_determination_period
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(provider_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        # verify that client got updates
        consumer_entity.update()
        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.DeterminationPeriod, new_determination_period)
        self.assertEqual(consumer_entity.state.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)

        if consumer_mdib._maintain_xml_tree:
            msg_reader._validate_node(consumer_mdib.get_mdib_response_node)

        # now update a channel descriptor and wait for the next DescriptionModificationReport
        channel_descriptor_handle = consumer_entity.descriptor.parent_handle  #'2.1.6.1'  # a channel
        consumer_entity = consumer_mdib.entities.by_handle(channel_descriptor_handle)
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')
        new_concept_description = 'foo bar'
        provider_entity = self.sdc_provider.mdib.entities.by_handle(channel_descriptor_handle)
        provider_entity.descriptor.Type.ConceptDescription[0].text = new_concept_description
        initial_descriptor_version = provider_entity.descriptor.DescriptorVersion
        initial_state_version = provider_entity.state.StateVersion

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(provider_entity)

        provider_entity.update()
        self.assertEqual(provider_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(provider_entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(provider_entity.state.StateVersion, initial_state_version + 1)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity.update()

        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(consumer_entity.state.DescriptorVersion, consumer_entity.descriptor.DescriptorVersion)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)

        if consumer_mdib._maintain_xml_tree:
            msg_reader._validate_node(consumer_mdib.get_mdib_response_node)

        # test creating a numeric descriptor
        # coll: wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'new_descriptors_handles')

        new_handle = 'a_generated_descriptor'

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
                          channel_descriptor_handle,
                          )

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(new_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        new_consumer_entity = consumer_mdib.entities.by_handle(new_handle)
        self.assertEqual(new_consumer_entity.descriptor.Resolution, Decimal('0.42'))
        if consumer_mdib._maintain_xml_tree:
            msg_reader._validate_node(consumer_mdib.get_mdib_response_node)

        # test creating a battery descriptor
        entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.MdsDescriptor)
        provider_mds_entity = entities[0]

        # coll: wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'new_descriptors_handles')
        new_battery_handle = 'new_battery_handle'
        node_name = pm.BatteryDescriptor
        new_entity = self.sdc_provider.mdib.entities.new_entity(node_name,
                                                                new_battery_handle,
                                                                provider_mds_entity.handle)
        new_entity.descriptor.Type = pm_types.CodedValue('23456')

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(new_entity)
        # long timeout, sometimes high load on jenkins makes these tests fail
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        consumer_entity = consumer_mdib.entities.by_handle(new_battery_handle)

        if consumer_mdib._maintain_xml_tree:
            msg_reader._validate_node(consumer_mdib.get_mdib_response_node)

        self.assertEqual(consumer_entity.descriptor.Handle, new_battery_handle)

        # test deleting a descriptor
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'deleted_descriptors_handles')
        provider_channel_entity = self.sdc_provider.mdib.entities.by_handle(channel_descriptor_handle)

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.remove_entity(provider_channel_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity = consumer_mdib.entities.by_handle(new_handle)
        self.assertIsNone(entity)

        # test deleting a context descriptor
        entities = self.sdc_provider.mdib.entities.by_node_type(pm_qnames.PatientContextDescriptor)
        patient_entity = entities[0]
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(patient_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity = consumer_mdib.entities.by_handle(patient_entity.handle)
        # now DescriptorVersion shall be incremented
        self.assertEqual(patient_entity.descriptor.DescriptorVersion +1, entity.descriptor.DescriptorVersion)
