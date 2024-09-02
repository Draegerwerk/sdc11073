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

import mockstuff
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
from sdc11073.roles.metricprovider import GenericMetricProvider

from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.entity_mdib.entity_consumermdib import EntityConsumerMdib
from sdc11073.entity_mdib.entities import ConsumerEntity, ConsumerMultiStateEntity, XmlEntity, XmlMultiStateEntity
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types import pm_types, pm_qnames as pm
from tests import utils
from tests.mockstuff import SomeDeviceXmlMdib, SomeDevice


if TYPE_CHECKING:
    from sdc11073.entity_mdib.entities import ProviderMultiStateEntity
    from sdc11073.entity_mdib.entity_providermdib import EntityProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.roles.providerbase import OperationClassGetter
    from sdc11073.provider.operations import OperationDefinitionBase


CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value

# mdib_70041 = '70041_MDIB_Final.xml'
mdib_70041 = '70041_MDIB_multi.xml'


class EntityMdibProduct(BaseProduct):

    def __init__(self,
                 mdib: EntityProviderMdib,
                 sco: AbstractScoOperationsRegistry,
                 log_prefix: str | None = None):
        super().__init__(mdib, sco, log_prefix)
        self.metric_provider = GenericMetricProvider(mdib, log_prefix=log_prefix)  # needed in a test
        # self._ordered_providers.extend([# AudioPauseProvider(mdib, log_prefix=log_prefix),
        #                                 # GenericSDCClockProvider(mdib, log_prefix=log_prefix),
        #                                 # GenericPatientContextProvider(mdib, log_prefix=log_prefix),
        #                                 # GenericAlarmProvider(mdib, log_prefix=log_prefix),
        #                                 self.metric_provider,
        #                                 # OperationProvider(mdib, log_prefix=log_prefix),
        #                                 # GenericSetComponentStateOperationProvider(mdib, log_prefix=log_prefix),
        #                                 ])


    def _register_existing_mdib_operations(self, sco: AbstractScoOperationsRegistry):
        operation_descriptor_entities = self._mdib.entities.parent_handle(self._sco.sco_descriptor_container.Handle)
        for entity in operation_descriptor_entities:
            registered_op = sco.get_operation_by_handle(entity.descriptor.Handle)
            if registered_op is None:
                self._logger.debug('found unregistered %s in mdib, handle=%s, code=%r target=%s',
                                   entity.descriptor.NODETYPE.localname, entity.descriptor.Handle,
                                   entity.descriptor.Type, entity.descriptor.OperationTarget)
                operation = self.make_operation_instance(entity.descriptor,
                                                         sco.operation_cls_getter)
                if operation is not None:
                    sco.register_operation(operation)

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Call make_operation_instance of all role providers, until the first returns not None."""
        operation_target_handle = operation_descriptor_container.OperationTarget
        operation_target_entity = self._mdib.entities.handle(operation_target_handle)
        if operation_target_entity is None:
            # this operation is incomplete, the operation target does not exist. Registration not possible.
            self._logger.warning('Operation %s: target %s does not exist, will not register operation',
                                 operation_descriptor_container.Handle, operation_target_handle)
            return None
        for role_handler in self._all_providers_sorted():
            operation = role_handler.make_operation_instance(operation_descriptor_container, operation_cls_getter)
            if operation is not None:
                self._logger.debug('%s provided operation for %s',
                                   role_handler.__class__.__name__, operation_descriptor_container)
                return operation
            self._logger.debug('%s: no handler for %s', role_handler.__class__.__name__, operation_descriptor_container)
        return None

    def init_operations(self):
        """Register all actively provided operations."""
        sco_handle = self._sco.sco_descriptor_container.Handle
        self._logger.info('init_operations for sco %s.', sco_handle)

        for role_handler in self._all_providers_sorted():
            role_handler.init_operations(self._sco)

        self._register_existing_mdib_operations(self._sco)

        for role_handler in self._all_providers_sorted():
            operations = role_handler.make_missing_operations(self._sco)
            if operations:
                info = ', '.join([f'{op.OP_DESCR_QNAME.localname} {op.handle}' for op in operations])
                self._logger.info('role handler %s added operations to mdib: %s',
                                  role_handler.__class__.__name__, info)
            for operation in operations:
                self._sco.register_operation(operation)

        all_sco_operation_entities = self._mdib.entities.parent_handle(self._sco.sco_descriptor_container.Handle)
        all_op_handles = [op.descriptor.Handle for op in all_sco_operation_entities]
        all_not_registered_op_handles = [op_h for op_h in all_op_handles if
                                         self._sco.get_operation_by_handle(op_h) is None]

        if not all_op_handles:
            self._logger.info('sco %s has no operations in mdib.', sco_handle)
        elif all_not_registered_op_handles:
            self._logger.info('sco %s has operations without handler! handles = %r',
                              sco_handle, all_not_registered_op_handles)
        else:
            self._logger.info('sco %s: all operations have a handler.', sco_handle)
        self._mdib.xtra.mk_state_containers_for_all_descriptors()
        self._mdib.pre_commit_handler = self._on_pre_commit
        self._mdib.post_commit_handler = self._on_post_commit


my_sdc_provider_components_async = copy.deepcopy(default_sdc_provider_components_async)
my_sdc_provider_components_async.role_provider_class = EntityMdibProduct  # no role providers
my_sdc_provider_components_async.waveform_provider_class = mockstuff.XmGenericWaveformProvider


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
        new_states = []
        entities = self.sdc_provider.mdib.entities.node_type(pm.PatientContextDescriptor)
        if len(entities) != 1:
            raise ValueError(f'cannot handle {len(entities)} instances of PatientContextDescriptor')
        # patientDescriptorContainer = self.sdc_provider.mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)
        entity = entities[0]
        handles = []
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
            handles.append(st.Handle)
            new_states.append(st)

        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            mgr.add_state(entity, handles)
        return entity, new_states

    def test_consumer_xml_mdib(self):
        patient_descriptor_entity, _ = self.add_random_patient(2)
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
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

        # same for multi state entity
        context_descriptor_handle = patient_descriptor_entity.descriptor.Handle
        context_consumer_entity = consumer_mdib.entities.handle(context_descriptor_handle)
        del consumer_mdib._entities[context_descriptor_handle]
        self.assertRaises(ValueError, context_consumer_entity.update)

    def test_metric_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
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
            mgr.add_state(provider_entity)

        # time.sleep(1)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        self.assertEqual(provider_entity.state.StateVersion, old_state_version + 1)
        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_alert_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
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
            mgr.add_state(entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        provider_entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
        # provider_state = self.sdc_provider.mdib.states.descriptor_handle.get_one(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_component_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        descriptor_handle = '2.1.2.1'  # a Channel

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'component_handles')

        provider_entity = self.sdc_provider.mdib.entities.handle(descriptor_handle)
        old_state_version = provider_entity.state.StateVersion
        with self.sdc_provider.mdib.component_state_transaction() as mgr:
            provider_entity.state.ActivationState = pm_types.ComponentActivation.FAILURE
            mgr.add_state(provider_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
        self.assertEqual(provider_entity.state.StateVersion, old_state_version +1)
        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)


    def test_operational_state_update(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()
        self.assertEqual(len(self.sdc_provider.mdib.entities), len(consumer_mdib._entities))

        descriptor_handle = 'SVO.37.3569'  # an Activate operation

        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'operation_handles')
        provider_entity = self.sdc_provider.mdib.entities.handle((descriptor_handle))
        provider_entity.state.OperatingMode = pm_types.OperatingMode.NA

        with self.sdc_provider.mdib.operational_state_transaction() as mgr:
            mgr.add_state(provider_entity)

        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity = consumer_mdib.entities.handle(descriptor_handle)
        provider_entity.update()

        self.assertIsNone(provider_entity.state.diff(consumer_entity.state, max_float_diff=1e-6))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

    def test_remove_mds(self):
        # msg_reader = self.sdc_consumer.msg_reader
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
        mds_entities = self.sdc_provider.mdib.entities.node_type(pm.MdsDescriptor)
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            for entity in mds_entities:
                mgr.remove_descriptor(entity.descriptor.Handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        # verify both mdibs are empty
        self.assertEqual(len(self.sdc_provider.mdib.entities), 0)
        self.assertEqual(len(consumer_mdib.entities), 0)
        # verify all version info is saved
        self.assertEqual(descriptor_versions, self.sdc_provider.mdib.descr_handle_version_lookup)
        self.assertEqual(state_versions, self.sdc_provider.mdib.state_handle_version_lookup)

    def test_set_patient_context_on_device(self):
        """device updates patient.
         verify that a notification device->client updates the client mdib."""
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        entities = self.sdc_provider.mdib.entities.node_type(pm.PatientContextDescriptor)
        self.assertEqual(len(entities), 1)
        coll = observableproperties.SingleValueCollector(consumer_mdib, 'context_handles')
        provider_entity, states = self.add_random_patient(1)  # this runs a transaction
        st_handle  = states[0].Handle
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        provider_entity.update()
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
            mgr.add_state(provider_entity, [st_handle])
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        time.sleep(1)
        provider_entity.update()
        provider_state = provider_entity.states[st_handle]
        consumer_entity.update()
        consumer_state = consumer_entity.states[st_handle]
        self.assertIsNone(consumer_state.diff(provider_state, max_float_diff=1e-6))


    def test_description_modification(self):
        msg_reader = self.sdc_consumer.msg_reader
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer, max_realtime_samples=297)
        consumer_mdib.init_mdib()

        metric_descriptor_handle = '0x34F00100'  # a metric
        consumer_entity = consumer_mdib.entities.handle(metric_descriptor_handle)
        initial_descriptor_version = consumer_entity.descriptor.DescriptorVersion
        initial_state_version = consumer_entity.state.StateVersion

        # now update a metric descriptor and wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'updated_descriptors_handles')

        new_determination_period = 3.14159
        provider_entity = self.sdc_provider.mdib.entities.handle(metric_descriptor_handle)
        provider_entity.descriptor.DeterminationPeriod = new_determination_period
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(provider_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        # verify that client got updates
        consumer_entity.update()
        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.DeterminationPeriod, new_determination_period)
        self.assertEqual(consumer_entity.state.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # now update a channel descriptor and wait for the next DescriptionModificationReport
        channel_descriptor_handle = '2.1.6.1'  # a channel
        consumer_entity = consumer_mdib.entities.handle(channel_descriptor_handle)
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

        consumer_entity.update()

        self.assertEqual(consumer_entity.descriptor.DescriptorVersion, initial_descriptor_version + 1)
        self.assertEqual(consumer_entity.descriptor.Type.ConceptDescription[0].text, new_concept_description)
        self.assertEqual(consumer_entity.state.DescriptorVersion, consumer_entity.descriptor.DescriptorVersion)
        self.assertEqual(consumer_entity.state.StateVersion, initial_state_version + 1)

        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

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
                                  channel_descriptor_handle
                                  )

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.handle_entity(new_entity)
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        new_consumer_entity = consumer_mdib.entities.handle(new_handle)
        self.assertEqual(new_consumer_entity.descriptor.Resolution, Decimal('0.42'))
        msg_reader._validate_node(consumer_mdib._get_mdib_response_node)

        # test creating a battery descriptor
        mds_descriptor_handle = '3569'  # a channel

        # coll: wait for the next DescriptionModificationReport
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

        self.assertEqual(consumer_entity.descriptor.Handle, new_battery_handle)

        # test deleting a descriptor
        coll = observableproperties.SingleValueCollector(consumer_mdib,
                                                         'deleted_descriptors_handles')
        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.remove_descriptor(channel_descriptor_handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        entity = consumer_mdib.entities.handle(new_handle)
        self.assertIsNone(entity)
