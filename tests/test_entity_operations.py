"""The module tests operations with provider and consumer that use entity mdibs."""
from __future__ import annotations

import datetime
import logging
import time
import unittest
from decimal import Decimal

from sdc11073 import commlog, loghelper, observableproperties
from sdc11073.consumer import SdcConsumer
from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.dispatch import RequestDispatcher
from sdc11073.entity_mdib.entity_consumermdib import EntityConsumerMdib
from sdc11073.loghelper import basic_logging_setup
from sdc11073.roles.nomenclature import NomenclatureCodes
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import msg_types, pm_types
from sdc11073.xml_types import pm_qnames as pm
from tests import utils
from tests.mockstuff import SomeDeviceEntityMdib

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

default_mdib_file = 'mdib_two_mds.xml'
mdib_70041_file = '70041_MDIB_Final.xml'


class TestEntityOperations(unittest.TestCase):
    """Test role providers (located in sdc11073.roles)."""

    def setUp(self):
        basic_logging_setup()
        self._logger = logging.getLogger('sdc.test')
        self._logger.info('############### start setUp %s ##############', self._testMethodName)
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_provider: SomeDeviceEntityMdib | None = None
        self.sdc_consumer: SdcConsumer | None = None
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)
        self._logger.info('############### setUp done %s ##############', self._testMethodName)

    def _init_provider_consumer(self, mdib_file: str = default_mdib_file):
        self.sdc_provider = SomeDeviceEntityMdib.from_mdib_file(
            self.wsd, None,
            mdib_file,
            max_subscription_duration=10)  # shorter duration for faster tests
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        self.sdc_provider.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_provider.set_location(utils.random_location(), self._loc_validators)

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

    def tearDown(self):
        self._logger.info('############### tearDown %s... ##############\n', self._testMethodName)
        self.log_watcher.setPaused(True)
        if self.sdc_consumer:
            self.sdc_consumer.stop_all()
        if self.sdc_provider:
            self.sdc_provider.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            self._logger.warning(repr(ex))
            raise
        self._logger.info('############### tearDown %s done ##############\n', self._testMethodName)

    def test_set_patient_context_operation(self):
        """Client calls corresponding operation of GenericContextProvider.

        - verify that operation is successful.
        - verify that a notification device->client also updates the consumer mdib.
        """
        self._init_provider_consumer()

        # delete possible existing states
        patient_entities = self.sdc_provider.mdib.entities.by_node_type(pm.PatientContextDescriptor)
        with self.sdc_provider.mdib.context_state_transaction() as tr:
            for ent in patient_entities:
                handles = list(ent.states.keys())
                if len(handles) > 0:
                    ent.states.clear()
                    tr.write_entity(ent, handles)

        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()

        patient_entities = client_mdib.entities.by_node_type(pm.PatientContextDescriptor)
        my_patient_entity = patient_entities[0]
        # initially the device shall not have any patient
        self.assertEqual(len(my_patient_entity.states), 0)
        operation_entities = client_mdib.entities.by_node_type(pm.SetContextStateOperationDescriptor)
        pat_op_entities = [ ent for ent in operation_entities if ent.descriptor.OperationTarget == my_patient_entity.handle]
        self.assertEqual(len(pat_op_entities), 1)
        my_operation = pat_op_entities[0]
        self._logger.info('Handle for SetContextState Operation = %s', my_operation.handle)
        context = self.sdc_consumer.client('Context')

        # insert a new patient with wrong handle, this shall fail
        proposed_context = my_patient_entity.new_state()
        proposed_context.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        proposed_context.Handle = 'some_nonexisting_handle'
        proposed_context.CoreData.Givenname = 'Karl'
        proposed_context.CoreData.Middlename = ['M.']
        proposed_context.CoreData.Familyname = 'Klammer'
        proposed_context.CoreData.Birthname = 'Bourne'
        proposed_context.CoreData.Title = 'Dr.'
        proposed_context.CoreData.Sex = pm_types.Sex.MALE
        proposed_context.CoreData.PatientType = pm_types.PatientType.ADULT
        proposed_context.CoreData.set_birthdate('2000-12-12')
        proposed_context.CoreData.Height = pm_types.Measurement(Decimal('88.2'), pm_types.CodedValue('abc', 'def'))
        proposed_context.CoreData.Weight = pm_types.Measurement(Decimal('68.2'), pm_types.CodedValue('abc'))
        proposed_context.CoreData.Race = pm_types.CodedValue('somerace')
        self.log_watcher.setPaused(True)
        future = context.set_context_state(my_operation.handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)
        self.assertIsNone(result.OperationTarget)

        # insert two new patients for same descriptor, both associated. This shall fail
        proposed_context1 = my_patient_entity.new_state()

        proposed_context1.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        proposed_context2 = my_patient_entity.new_state()
        proposed_context2.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        future = context.set_context_state(my_operation.handle, [proposed_context1, proposed_context2])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)

        self.log_watcher.setPaused(False)

        # insert a new patient with correct handle, this shall succeed
        proposed_context.Handle = my_patient_entity.handle
        future = context.set_context_state(my_operation.handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        self.assertIsNotNone(result.OperationTarget)

        # check client side patient context, this shall have been set via notification
        consumer_entity = client_mdib.entities.by_handle(my_patient_entity.handle)
        patient_context_state_container = list(consumer_entity.states.values())[0]  # noqa: RUF015
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Karl')
        self.assertEqual(patient_context_state_container.CoreData.Middlename, ['M.'])
        self.assertEqual(patient_context_state_container.CoreData.Familyname, 'Klammer')
        self.assertEqual(patient_context_state_container.CoreData.Birthname, 'Bourne')
        self.assertEqual(patient_context_state_container.CoreData.Title, 'Dr.')
        self.assertEqual(patient_context_state_container.CoreData.Sex, 'M')
        self.assertEqual(patient_context_state_container.CoreData.PatientType, pm_types.PatientType.ADULT)
        self.assertEqual(patient_context_state_container.CoreData.Height.MeasuredValue, Decimal('88.2'))
        self.assertEqual(patient_context_state_container.CoreData.Weight.MeasuredValue, Decimal('68.2'))
        self.assertEqual(patient_context_state_container.CoreData.Race, pm_types.CodedValue('somerace'))
        self.assertNotEqual(patient_context_state_container.Handle,
                            my_patient_entity.handle)  # device replaced it with its own handle
        self.assertEqual(patient_context_state_container.ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)
        self.assertIsNotNone(patient_context_state_container.BindingMdibVersion)
        self.assertIsNotNone(patient_context_state_container.BindingStartTime)

        # test update of the patient
        patient_context_state_container.CoreData.Givenname = 'Karla'
        future = context.set_context_state(my_operation.handle, [patient_context_state_container])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertEqual(result.OperationTarget, patient_context_state_container.Handle)

        consumer_entity.update()
        patient_context_state_container = list(consumer_entity.states.values())[0]  # noqa: RUF015
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Karla')
        self.assertEqual(patient_context_state_container.CoreData.Familyname, 'Klammer')

        # set new patient, check binding mdib versions and context association
        proposed_context = my_patient_entity.new_state()
        proposed_context.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        proposed_context.CoreData.Givenname = 'Heidi'
        proposed_context.CoreData.Middlename = ['M.']
        proposed_context.CoreData.Familyname = 'Klammer'
        proposed_context.CoreData.Birthname = 'Bourne'
        proposed_context.CoreData.Title = 'Dr.'
        proposed_context.CoreData.Sex = pm_types.Sex.FEMALE
        proposed_context.CoreData.PatientType = pm_types.PatientType.ADULT
        proposed_context.CoreData.set_birthdate('2000-12-12')
        proposed_context.CoreData.Height = pm_types.Measurement(Decimal('88.2'), pm_types.CodedValue('abc', 'def'))
        proposed_context.CoreData.Weight = pm_types.Measurement(Decimal('68.2'), pm_types.CodedValue('abc'))
        proposed_context.CoreData.Race = pm_types.CodedValue('somerace')
        future = context.set_context_state(my_operation.handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        invocation_state = result.InvocationInfo.InvocationState
        self.assertEqual(invocation_state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertIsNotNone(result.OperationTarget)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        consumer_entity.update()
        patient_context_state_containers = list(consumer_entity.states.values())

        # sort by BindingMdibVersion
        patient_context_state_containers.sort(key=lambda obj: obj.BindingMdibVersion)
        self.assertEqual(len(patient_context_state_containers), 2)
        old_patient = patient_context_state_containers[0]
        new_patient = patient_context_state_containers[1]
        self.assertEqual(old_patient.ContextAssociation, pm_types.ContextAssociation.DISASSOCIATED)
        self.assertEqual(new_patient.ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)

        # create a patient locally on device, then test update from client
        pat_entity = self.sdc_provider.mdib.entities.by_handle(my_patient_entity.handle)
        st = pat_entity.new_state()
        st.CoreData.Givenname = 'Max123'
        st.CoreData.Middlename = ['Willy']
        st.CoreData.Birthname = 'Mustermann'
        st.CoreData.Familyname = 'Musterfrau'
        st.CoreData.Title = 'Rex'
        st.CoreData.Sex = pm_types.Sex.MALE
        st.CoreData.PatientType = pm_types.PatientType.ADULT
        st.CoreData.Height = pm_types.Measurement(Decimal('88.2'), pm_types.CodedValue('abc', 'def'))
        st.CoreData.Weight = pm_types.Measurement(Decimal('68.2'), pm_types.CodedValue('abc'))
        st.CoreData.Race = pm_types.CodedValue('123', 'def')
        st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)

        coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'episodic_context_report')
        with self.sdc_provider.mdib.context_state_transaction() as mgr:
            mgr.write_entity(pat_entity, modified_handles=[st.Handle])
        coll.result(timeout=NOTIFICATION_TIMEOUT)

        consumer_entity.update()
        patient_context_state_containers = list(consumer_entity.states.values())

        my_patients = [p for p in patient_context_state_containers if p.CoreData.Givenname == 'Max123']
        self.assertEqual(len(my_patients), 1)
        my_patient = my_patients[0]
        my_patient.CoreData.Givenname = 'Karl123'
        future = context.set_context_state(my_operation.handle, [my_patient])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        consumer_entity.update()
        my_updated_patient = consumer_entity.states[my_patient.Handle]

        self.assertEqual(my_updated_patient.CoreData.Givenname, 'Karl123')

    def test_location_context(self):
        # initially the device shall have one location, and the client must have it in its mdib
        self._init_provider_consumer()
        device_mdib = self.sdc_provider.mdib
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()

        dev_location_entities = device_mdib.entities.by_node_type(pm.LocationContextDescriptor)
        self.assertEqual(len(dev_location_entities), 1)
        dev_location_entity = dev_location_entities[0]
        loc_context_handle = dev_location_entity.handle
        cl_location_entity = client_mdib.entities.by_handle(loc_context_handle)
        self.assertIsNotNone(cl_location_entity)
        initial_number_of_states = len(dev_location_entity.states)
        self.assertGreater(initial_number_of_states, 0)

        self.assertEqual(len(dev_location_entity.states), len(cl_location_entity.states))

        for i in range(10):
            new_location = utils.random_location()
            coll = observableproperties.SingleValueCollector(client_mdib, 'context_handles')
            self.sdc_provider.set_location(new_location)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            dev_location_entity = device_mdib.entities.by_handle(loc_context_handle)
            cl_location_entity = client_mdib.entities.by_handle(loc_context_handle)
            self.assertEqual(len(dev_location_entity.states), i + 1 + initial_number_of_states)
            self.assertEqual(len(cl_location_entity.states), i + 1 + initial_number_of_states)

            # sort by mdib_version
            dev_locations = list(dev_location_entity.states.values())
            cl_locations = list(cl_location_entity.states.values())

            dev_locations.sort(key=lambda a: a.BindingMdibVersion)
            cl_locations.sort(key=lambda a: a.BindingMdibVersion)
            # Plausibility check that the new location has expected data
            self.assertEqual(dev_locations[-1].LocationDetail.PoC, new_location.poc)
            self.assertEqual(cl_locations[-1].LocationDetail.PoC, new_location.poc)
            self.assertEqual(dev_locations[-1].LocationDetail.Bed, new_location.bed)
            self.assertEqual(cl_locations[-1].LocationDetail.Bed, new_location.bed)
            self.assertEqual(dev_locations[-1].ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)
            self.assertEqual(cl_locations[-1].ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)
            self.assertEqual(dev_locations[-1].UnbindingMdibVersion, None)
            self.assertEqual(cl_locations[-1].UnbindingMdibVersion, None)

            for j, loc in enumerate(dev_locations[:-1]):
                self.assertEqual(loc.ContextAssociation, pm_types.ContextAssociation.DISASSOCIATED)
                self.assertEqual(loc.UnbindingMdibVersion, dev_locations[j + 1].BindingMdibVersion)

            for j, loc in enumerate(cl_locations[:-1]):
                self.assertEqual(loc.ContextAssociation, pm_types.ContextAssociation.DISASSOCIATED)
                self.assertEqual(loc.UnbindingMdibVersion, cl_locations[j + 1].BindingMdibVersion)

    def test_activate(self):
        """Test AudioPauseProvider."""
        # switch one alert system off
        self._init_provider_consumer(mdib_70041_file)
        alert_system_entity_off = self.sdc_provider.mdib.entities.by_handle('Asy.3208')
        self.assertIsNotNone(alert_system_entity_off)
        alert_system_entity_off.state.ActivationState = pm_types.AlertActivation.OFF
        with self.sdc_provider.mdib.alert_state_transaction() as mgr:
            mgr.write_entity(alert_system_entity_off)

        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
        operation_pause_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
        operation_cancel_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        self.assertEqual(len(operation_pause_entities), 1)
        self.assertEqual(len(operation_cancel_entities), 1)

        pause_entity = operation_pause_entities[0]
        cancel_entity = operation_cancel_entities[0]

        future = set_service.activate(operation_handle=pause_entity.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        alert_system_entities = self.sdc_provider.mdib.entities.by_node_type(pm.AlertSystemDescriptor)
        for alert_system_entity in alert_system_entities:
            if alert_system_entity.handle != alert_system_entity_off.handle:
                self.assertEqual(alert_system_entity.state.SystemSignalActivation[0].State,
                                 pm_types.AlertActivation.PAUSED)

        future = set_service.activate(operation_handle=cancel_entity.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        for alert_system_entity in alert_system_entities:
            if alert_system_entity.handle != alert_system_entity_off.handle:
                alert_system_entity.update()
                self.assertEqual(alert_system_entity.state.SystemSignalActivation[0].State,
                                 pm_types.AlertActivation.ON)

        # now remove all alert systems from provider mdib and verify that operation now fails
        alert_system_entities = self.sdc_provider.mdib.entities.by_node_type(pm.AlertSystemDescriptor)

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            for ent in alert_system_entities:
                mgr.remove_entity(ent)
        future = set_service.activate(operation_handle=pause_entity.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)

        future = set_service.activate(operation_handle=cancel_entity.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)

    def test_set_ntp_server(self):
        self._init_provider_consumer()
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
        operation_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        self.assertGreater(len(operation_entities), 0)
        my_operation_entity = operation_entities[0]
        operation_handle = my_operation_entity.handle
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            self._logger.info('ntp server = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            invocation_state = result.InvocationInfo.InvocationState
            self.assertEqual(invocation_state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            op_target_entity = client_mdib.entities.by_handle(my_operation_entity.descriptor.OperationTarget)
            if op_target_entity.node_type == pm.MdsState:
                # look for the ClockState child
                clock_entities = client_mdib.entities.by_node_type(pm.ClockDescriptor)
                clock_entities = [c for c in clock_entities if c.parent_handle == op_target_entity.handle]
                if len(clock_entities) == 1:
                    op_target_entity = clock_entities[0]
            self.assertEqual(op_target_entity.state.ReferenceSource[0], value)

    def test_set_time_zone(self):
        self._init_provider_consumer()
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()

        coding = pm_types.Coding(NomenclatureCodes.MDC_ACT_SET_TIME_ZONE)
        operation_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        self.assertGreater(len(operation_entities), 0)
        my_operation_entity = operation_entities[0]
        operation_handle = my_operation_entity.handle
        for value in ('+03:00', '-03:00'):  # are these correct values?
            self._logger.info('time zone = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            op_target_entity = client_mdib.entities.by_handle(my_operation_entity.descriptor.OperationTarget)
            if op_target_entity.node_type == pm.MdsState:
                # look for the ClockState child
                clock_entities = client_mdib.entities.by_node_type(pm.ClockDescriptor)
                clock_entities = [c for c in clock_entities if c.parent_handle == op_target_entity.handle]
                if len(clock_entities) == 1:
                    op_target_entity = clock_entities[0]
            self.assertEqual(op_target_entity.state.TimeZone, value)

    def test_set_metric_state(self):
        # first we need to add a set_metric_state Operation
        self._init_provider_consumer()
        sco_entities = self.sdc_provider.mdib.entities.by_node_type(pm.ScoDescriptor)
        my_sco = sco_entities[0]

        metric_entities = self.sdc_provider.mdib.entities.by_node_type(pm.NumericMetricDescriptor)
        my_metric_entity = metric_entities[0]

        new_operation_entity = self.sdc_provider.mdib.entities.new_entity(pm.SetMetricStateOperationDescriptor,
                                                                        handle='HANDLE_FOR_MY_TEST',
                                                                        parent_handle=my_sco.handle)
        my_code = pm_types.CodedValue('99999')
        new_operation_entity.descriptor.Type = my_code
        new_operation_entity.descriptor.SafetyClassification = pm_types.SafetyClassification.INF
        new_operation_entity.descriptor.OperationTarget = my_metric_entity.handle

        with self.sdc_provider.mdib.descriptor_transaction() as mgr:
            mgr.write_entity(new_operation_entity)

        sco = self.sdc_provider._sco_operations_registries[my_sco.handle]
        role_provider = self.sdc_provider.product_lookup[my_sco.handle]

        op = role_provider.metric_provider.make_operation_instance(
            new_operation_entity.descriptor, sco.operation_cls_getter)
        sco.register_operation(op)
        self.sdc_provider.mdib.xtra.mk_state_containers_for_all_descriptors()
        set_service = self.sdc_consumer.client('Set')
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer)
        consumer_mdib.init_mdib()

        consumer_entity = consumer_mdib.entities.by_handle(my_metric_entity.handle)
        self.assertIsNotNone(consumer_entity)

        # modify entity.state as new proposed state
        before_state_version = consumer_entity.state.StateVersion

        operation_handle = new_operation_entity.handle
        new_lifetime_period = 42.5
        consumer_entity.state.LifeTimePeriod = new_lifetime_period
        future = set_service.set_metric_state(operation_handle=operation_handle,
                                              proposed_metric_states=[consumer_entity.state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        consumer_entity.update()
        self.assertEqual(consumer_entity.state.StateVersion, before_state_version + 1)
        self.assertAlmostEqual(consumer_entity.state.LifeTimePeriod, new_lifetime_period)

    def test_set_component_state(self):
        """Test GenericSetComponentStateOperationProvider."""
        # Use a single mds mdib. This makes test easier because source_mds of channel and sco are the same.
        self._init_provider_consumer('mdib_tns.xml')
        channels = self.sdc_provider.mdib.entities.by_node_type(pm.ChannelDescriptor)
        my_channel_entity = channels[0]
        # first we need to add a set_component_state Operation
        sco_entities = self.sdc_provider.mdib.entities.by_node_type(pm.ScoDescriptor)
        my_sco_entity = sco_entities[0]

        operation_entity = self.sdc_provider.mdib.entities.new_entity(pm.SetComponentStateOperationDescriptor,
                                                                      'HANDLE_FOR_MY_TEST',
                                                                      my_sco_entity.handle)

        operation_entity.descriptor.SafetyClassification = pm_types.SafetyClassification.INF
        operation_entity.descriptor.OperationTarget = my_channel_entity.handle
        operation_entity.descriptor.Type = pm_types.CodedValue('999998')
        with self.sdc_provider.mdib.descriptor_transaction() as tr:
            tr.write_entity(operation_entity)

        sco = self.sdc_provider._sco_operations_registries[my_sco_entity.handle]
        role_provider = self.sdc_provider.product_lookup[my_sco_entity.handle]
        op = role_provider.make_operation_instance(operation_entity.descriptor,
                                                   sco.operation_cls_getter)
        sco.register_operation(op)
        self.sdc_provider.mdib.xtra.mk_state_containers_for_all_descriptors()
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()

        proposed_component_state = my_channel_entity.state  #client_mdib.xtra.mk_proposed_state(my_channel_entity.handle)
        self.assertIsNone(
            proposed_component_state.OperatingHours)  # just to be sure that we know the correct intitial value
        before_state_version = proposed_component_state.StateVersion
        new_operating_hours = 42
        proposed_component_state.OperatingHours = new_operating_hours
        future = set_service.set_component_state(operation_handle=operation_entity.handle,
                                                 proposed_component_states=[proposed_component_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

        updated_channel_entity = self.sdc_provider.mdib.entities.by_handle(my_channel_entity.handle)
        self.assertEqual(updated_channel_entity.state.OperatingHours, new_operating_hours)
        self.assertEqual(updated_channel_entity.state.StateVersion, before_state_version + 1)

    def test_operation_without_handler(self):
        """Verify that a correct response is sent."""
        self._init_provider_consumer()
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()

        operation_handle = 'SVO.42.2.1.1.2.0-6'
        value = 'foobar'
        future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)
        self.assertIsNotNone(result.InvocationInfo.InvocationError)
        # Verify that transaction id increases even with "invalid" calls.
        future2 = set_service.set_string(operation_handle=operation_handle, requested_string=value)
        result2 = future2.result(timeout=SET_TIMEOUT)
        self.assertGreater(result2.InvocationInfo.TransactionId, result.InvocationInfo.TransactionId)

    def test_delayed_processing(self):
        """Verify that flag 'delayed_processing' changes responses as expected."""
        self._init_provider_consumer()
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
        entities = self.sdc_provider.mdib.entities.by_coding(coding)
        self.assertEqual(len(entities), 1)
        my_operation_entity = entities[0]

        operation = self.sdc_provider.get_operation_by_handle(my_operation_entity.handle)
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            self._logger.info('ntp server = %s', value)
            operation.delayed_processing = True  # first OperationInvokedReport shall have InvocationState.WAIT
            coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'operation_invoked_report')
            future = set_service.set_string(operation_handle=my_operation_entity.handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            received_message = coll.result(timeout=5)
            my_msg_types = received_message.msg_reader.msg_types
            operation_invoked_report = my_msg_types.OperationInvokedReport.from_node(received_message.p_msg.msg_node)
            self.assertEqual(operation_invoked_report.ReportPart[0].InvocationInfo.InvocationState,
                             my_msg_types.InvocationState.WAIT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, my_msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
            time.sleep(0.5)
            # disable delayed processing
            self._logger.info("disable delayed processing")
            operation.delayed_processing = False  # first OperationInvokedReport shall have InvocationState.FINISHED
            coll = observableproperties.SingleValueCollector(self.sdc_consumer, 'operation_invoked_report')
            future = set_service.set_string(operation_handle=my_operation_entity.handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            received_message = coll.result(timeout=5)
            my_msg_types = received_message.msg_reader.msg_types
            operation_invoked_report = my_msg_types.OperationInvokedReport.from_node(received_message.p_msg.msg_node)
            self.assertEqual(operation_invoked_report.ReportPart[0].InvocationInfo.InvocationState,
                             my_msg_types.InvocationState.FINISHED)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, my_msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

    def test_set_operating_mode(self):
        self._init_provider_consumer()
        consumer_mdib = EntityConsumerMdib(self.sdc_consumer)
        consumer_mdib.init_mdib()

        operation_entities = consumer_mdib.entities.by_node_type(pm.ActivateOperationDescriptor)
        my_operation_entity = operation_entities[0]
        operation = self.sdc_provider.get_operation_by_handle(my_operation_entity.handle)
        for op_mode in (pm_types.OperatingMode.NA, pm_types.OperatingMode.ENABLED):
            operation.set_operating_mode(op_mode)
            time.sleep(1)
            my_operation_entity.update()
            self.assertEqual(my_operation_entity.state.OperatingMode, op_mode)

    def test_set_string_value(self):
        """Verify that metric provider instantiated an operation for SetString call.

        OperationTarget of operation 0815 is an EnumStringMetricState.
        """
        self._init_provider_consumer(mdib_70041_file)
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()
        coding = pm_types.Coding('0815')
        my_operation_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        self.assertEqual(len(my_operation_entities), 1)
        my_operation_entity = my_operation_entities[0]
        operation_handle = my_operation_entity.handle
        for value in ('ADULT', 'PEDIATRIC'):
            self._logger.info('string value = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            consumer_entity = client_mdib.entities.by_handle(my_operation_entity.descriptor.OperationTarget)
            self.assertEqual(consumer_entity.state.MetricValue.Value, value)

    def test_set_metric_value(self):
        """Verify that metric provider instantiated an operation for SetNumericValue call.

        OperationTarget of operation 0815-1 is a NumericMetricState.
        """
        self._init_provider_consumer(mdib_70041_file)
        set_service = self.sdc_consumer.client('Set')
        client_mdib = EntityConsumerMdib(self.sdc_consumer)
        client_mdib.init_mdib()
        coding = pm_types.Coding('0815-1')
        my_operation_entities = self.sdc_provider.mdib.entities.by_coding(coding)
        my_operation_entity = my_operation_entities[0]

        for value in (Decimal(1), Decimal(42), 1.1, 10, "12"):
            self._logger.info('metric value = %s', value)
            future = set_service.set_numeric_value(operation_handle=my_operation_entity.handle,
                                                   requested_numeric_value=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            ent = client_mdib.entities.by_handle(my_operation_entity.descriptor.OperationTarget)
            self.assertEqual(ent.state.MetricValue.Value, Decimal(str(value)))
