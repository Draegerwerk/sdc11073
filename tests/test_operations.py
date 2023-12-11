import datetime
import logging
import time
import unittest
from decimal import Decimal

from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073 import observableproperties
from sdc11073.xml_types import pm_types, msg_types, pm_qnames as pm
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib import ConsumerMdib
from sdc11073.roles.nomenclature import NomenclatureCodes
from sdc11073.consumer import SdcConsumer
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.dispatch import RequestDispatcher
from tests import utils
from tests.mockstuff import SomeDevice

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


class Test_BuiltinOperations(unittest.TestCase):
    """Test role providers (located in sdc11073.roles)."""

    def setUp(self):
        basic_logging_setup()
        self._logger = logging.getLogger('sdc.test')
        self._logger.info('############### start setUp %s ##############', self._testMethodName)
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        self.sdc_device.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(utils.random_location(), self._loc_validators)

        time.sleep(0.5)  # allow init of devices to complete

        x_addr = self.sdc_device.get_xaddrs()
        # no deferred action handling for easier debugging
        specific_components = SdcConsumerComponents(
            action_dispatcher_class=RequestDispatcher
        )
        self.sdc_client = SdcConsumer(x_addr[0],
                                      sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                      ssl_context_container=None,
                                      validate=CLIENT_VALIDATE,
                                      specific_components=specific_components)
        self.sdc_client.start_all()
        time.sleep(1)
        self._logger.info('############### setUp done %s ##############', self._testMethodName)
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        self._logger.info('############### tearDown %s... ##############\n', self._testMethodName)
        self.log_watcher.setPaused(True)
        if self.sdc_client:
            self.sdc_client.stop_all()
        if self.sdc_device:
            self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            self._logger.warning(repr(ex))
            raise
        self._logger.info('############### tearDown %s done ##############\n', self._testMethodName)

    def test_set_patient_context_operation(self):
        """client calls corresponding operation of GenericContextProvider.
        - verify that operation is successful.
         verify that a notification device->client also updates the client mdib."""
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        patient_descriptor_container = client_mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)
        # initially the device shall not have any patient
        patient_context_state_container = client_mdib.context_states.NODETYPE.get_one(
            pm.PatientContext, allow_none=True)
        self.assertIsNone(patient_context_state_container)

        my_operations = client_mdib.get_operation_descriptors_for_descriptor_handle(
            patient_descriptor_container.Handle,
            NODETYPE=pm.SetContextStateOperationDescriptor)
        self.assertEqual(len(my_operations), 1)
        operation_handle = my_operations[0].Handle
        self._logger.info('Handle for SetContextState Operation = %s', operation_handle)
        context = self.sdc_client.client('Context')

        # insert a new patient with wrong handle, this shall fail
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle)
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
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FAILED)
        self.assertIsNone(result.OperationTarget)
        self.log_watcher.setPaused(False)

        # insert a new patient with correct handle, this shall succeed
        proposed_context.Handle = patient_descriptor_container.Handle
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        self.assertIsNotNone(result.OperationTarget)

        # check client side patient context, this shall have been set via notification
        patient_context_state_container = client_mdib.context_states.NODETYPE.get_one(pm.PatientContextState)
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
                            patient_descriptor_container.Handle)  # device replaced it with its own handle
        self.assertEqual(patient_context_state_container.ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)

        # test update of the patient
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle,
                                                              handle=patient_context_state_container.Handle)
        proposed_context.CoreData.Givenname = 'Karla'
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertEqual(result.OperationTarget, proposed_context.Handle)
        patient_context_state_container = client_mdib.context_states.handle.get_one(
            patient_context_state_container.Handle)
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Karla')
        self.assertEqual(patient_context_state_container.CoreData.Familyname, 'Klammer')

        # set new patient, check binding mdib versions and context association
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle)
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
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        invocation_state = result.InvocationInfo.InvocationState
        self.assertEqual(invocation_state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertIsNotNone(result.OperationTarget)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        patient_context_state_containers = client_mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
        # sort by BindingMdibVersion
        patient_context_state_containers.sort(key=lambda obj: obj.BindingMdibVersion)
        self.assertEqual(len(patient_context_state_containers), 2)
        old_patient = patient_context_state_containers[0]
        new_patient = patient_context_state_containers[1]
        self.assertEqual(old_patient.ContextAssociation, pm_types.ContextAssociation.DISASSOCIATED)
        self.assertEqual(new_patient.ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)

        # create a patient locally on device, then test update from client
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_context_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.mk_context_state(patient_descriptor_container.Handle)
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
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        patient_context_state_containers = client_mdib.context_states.NODETYPE.get(pm.PatientContextState)
        my_patients = [p for p in patient_context_state_containers if p.CoreData.Givenname == 'Max123']
        self.assertEqual(len(my_patients), 1)
        my_patient = my_patients[0]
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle, my_patient.Handle)
        proposed_context.CoreData.Givenname = 'Karl123'
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        my_patient2 = self.sdc_device.mdib.context_states.handle.get_one(my_patient.Handle)
        self.assertEqual(my_patient2.CoreData.Givenname, 'Karl123')

    def test_location_context(self):
        # initially the device shall have one location, and the client must have it in its mdib
        device_mdib = self.sdc_device.mdib
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()

        dev_locations = device_mdib.context_states.NODETYPE.get(pm.LocationContextState)
        cl_locations = client_mdib.context_states.NODETYPE.get(pm.LocationContextState)
        self.assertEqual(len(dev_locations), 1)
        self.assertEqual(len(cl_locations), 1)
        self.assertEqual(dev_locations[0].Handle, cl_locations[0].Handle)
        self.assertEqual(cl_locations[0].ContextAssociation, pm_types.ContextAssociation.ASSOCIATED)
        self.assertIsNotNone(cl_locations[0].BindingMdibVersion)
        self.assertEqual(cl_locations[0].UnbindingMdibVersion, None)

        for i in range(10):
            new_location = utils.random_location()
            coll = observableproperties.SingleValueCollector(client_mdib, 'context_by_handle')
            self.sdc_device.set_location(new_location)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            dev_locations = device_mdib.context_states.NODETYPE.get(pm.LocationContextState)
            cl_locations = client_mdib.context_states.NODETYPE.get(pm.LocationContextState)
            self.assertEqual(len(dev_locations), i + 2)
            self.assertEqual(len(cl_locations), i + 2)

            # sort by mdib_version
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
                self.assertEqual(loc.UnbindingMdibVersion, dev_locations[j + 1].BindingMdibVersion - 1)

            for j, loc in enumerate(cl_locations[:-1]):
                self.assertEqual(loc.ContextAssociation, pm_types.ContextAssociation.DISASSOCIATED)
                self.assertEqual(loc.UnbindingMdibVersion, cl_locations[j + 1].BindingMdibVersion - 1)

    def test_audio_pause(self):
        """Tests AudioPauseProvider

        """
        # switch one alert system off
        alert_system_off = 'Asy.3208'
        with self.sdc_device.mdib.transaction_manager() as mgr:
            state = mgr.get_state(alert_system_off)
            state.ActivationState = pm_types.AlertActivation.OFF
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)

        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            state = self.sdc_client.mdib.states.descriptor_handle.get_one(alert_system_descriptor.Handle)
            # we know that the state has only one SystemSignalActivation entity, which is audible and should be paused now
            if alert_system_descriptor.Handle != alert_system_off:
                self.assertEqual(state.SystemSignalActivation[0].State, pm_types.AlertActivation.PAUSED)

        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            state = self.sdc_client.mdib.states.descriptor_handle.get_one(alert_system_descriptor.Handle)
            self.assertEqual(state.SystemSignalActivation[0].State, pm_types.AlertActivation.ON)

    def test_audio_pause_two_clients(self):
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)

        set_service = self.sdc_client.client('Set')
        client_mdib1 = ConsumerMdib(self.sdc_client)
        client_mdib1.init_mdib()

        # connect a 2nd client
        x_addr = self.sdc_device.get_xaddrs()
        # no deferred action handling for easier debugging
        specific_components = SdcConsumerComponents(
            action_dispatcher_class=RequestDispatcher
        )
        sdc_client2 = SdcConsumer(x_addr[0],
                                  sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                  ssl_context_container=None,
                                  validate=CLIENT_VALIDATE,
                                  specific_components=specific_components,
                                  log_prefix='client2')
        sdc_client2.start_all()
        try:
            client_mdib2 = ConsumerMdib(sdc_client2)
            client_mdib2.init_mdib()
            clients = (self.sdc_client, sdc_client2)
            coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
            operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
            future = set_service.activate(operation_handle=operation.Handle, arguments=None)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            time.sleep(0.5)  # allow notifications to arrive
            # the whole tests only makes sense if there is an alert system
            alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
            self.assertTrue(alert_system_descriptors is not None)
            self.assertGreater(len(alert_system_descriptors), 0)
            for alert_system_descriptor in alert_system_descriptors:
                for client in clients:
                    state = client.mdib.states.descriptor_handle.get_one(alert_system_descriptor.Handle)
                    # we know that the state has only one SystemSignalActivation entity, which is audible and should be paused now
                    self.assertEqual(state.SystemSignalActivation[0].State, pm_types.AlertActivation.PAUSED)

            coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
            operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
            future = set_service.activate(operation_handle=operation.Handle, arguments=None)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            time.sleep(0.5)  # allow notifications to arrive
            # the whole tests only makes sense if there is an alert system
            alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
            self.assertTrue(alert_system_descriptors is not None)
            self.assertGreater(len(alert_system_descriptors), 0)
            for alert_system_descriptor in alert_system_descriptors:
                for client in clients:
                    state = client.mdib.states.descriptor_handle.get_one(alert_system_descriptor.Handle)
                    self.assertEqual(state.SystemSignalActivation[0].State, pm_types.AlertActivation.ON)
        finally:
            sdc_client2.stop_all()

    def test_set_ntp_server(self):
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)

        operation_handle = my_operation_descriptor.Handle
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            self._logger.info('ntp server = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptor_handle.get_one(my_operation_descriptor.OperationTarget)
            if state.NODETYPE == pm.MdsState:
                # look for the ClockState child
                clock_descriptors = client_mdib.descriptions.NODETYPE.get(pm.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.descriptor_handle == state.descriptor_handle]
                if len(clock_descriptors) == 1:
                    state = client_mdib.states.descriptor_handle.get_one(clock_descriptors[0].Handle)
            self.assertEqual(state.ReferenceSource[0], value)

    def test_set_time_zone(self):
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()

        coding = pm_types.Coding(NomenclatureCodes.MDC_ACT_SET_TIME_ZONE)
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)

        operation_handle = my_operation_descriptor.Handle
        for value in ('+03:00', '-03:00'):  # are these correct values?
            self._logger.info('time zone = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptor_handle.get_one(my_operation_descriptor.OperationTarget)
            if state.NODETYPE == pm.MdsState:
                # look for the ClockState child
                clock_descriptors = client_mdib.descriptions.NODETYPE.get(pm.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == state.DescriptorHandle]
                if len(clock_descriptors) == 1:
                    state = client_mdib.states.descriptor_handle.get_one(clock_descriptors[0].Handle)
            self.assertEqual(state.TimeZone, value)

    def test_set_metric_state(self):
        # first we need to add a set_metric_state Operation
        sco_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.ScoDescriptor)
        cls = self.sdc_device.mdib.data_model.get_descriptor_container_class(pm.SetMetricStateOperationDescriptor)
        operation_target_handle = '0x34F001D5'
        my_code = pm_types.CodedValue('99999')
        my_operation_descriptor = cls('HANDLE_FOR_MY_TEST', sco_descriptors[0].Handle)
        my_operation_descriptor.Type = my_code
        my_operation_descriptor.SafetyClassification = pm_types.SafetyClassification.INF
        my_operation_descriptor.OperationTarget = operation_target_handle
        self.sdc_device.mdib.descriptions.add_object(my_operation_descriptor)
        sco_handle = 'Sco.mds0'
        sco = self.sdc_device._sco_operations_registries[sco_handle]
        role_provider = self.sdc_device.product_lookup[sco_handle]

        op = role_provider.metric_provider.make_operation_instance(
            my_operation_descriptor, sco.operation_cls_getter)
        sco.register_operation(op)
        self.sdc_device.mdib.xtra.mk_state_containers_for_all_descriptors()
        setService = self.sdc_client.client('Set')
        clientMdib = ConsumerMdib(self.sdc_client)
        clientMdib.init_mdib()

        operation_handle = my_operation_descriptor.Handle
        proposed_metric_state = clientMdib.xtra.mk_proposed_state(operation_target_handle)
        self.assertIsNone(
            proposed_metric_state.LifeTimePeriod)  # just to be sure that we know the correct intitial value
        before_state_version = proposed_metric_state.StateVersion
        newLifeTimePeriod = 42.5
        proposed_metric_state.LifeTimePeriod = newLifeTimePeriod
        future = setService.set_metric_state(operation_handle=operation_handle,
                                             proposed_metric_states=[proposed_metric_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        updated_metric_state = clientMdib.states.descriptor_handle.get_one(operation_target_handle)
        self.assertEqual(updated_metric_state.StateVersion, before_state_version + 1)
        self.assertAlmostEqual(updated_metric_state.LifeTimePeriod, newLifeTimePeriod)

    def test_set_component_state(self):
        """ tests GenericSetComponentStateOperationProvider"""
        operation_target_handle = '2.1.2.1'  # a channel
        # first we need to add a set_component_state Operation
        sco_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.ScoDescriptor)
        cls = self.sdc_device.mdib.data_model.get_descriptor_container_class(pm.SetComponentStateOperationDescriptor)
        my_operation_descriptor = cls('HANDLE_FOR_MY_TEST', sco_descriptors[0].Handle)
        my_operation_descriptor.SafetyClassification = pm_types.SafetyClassification.INF

        my_operation_descriptor.OperationTarget = operation_target_handle
        my_operation_descriptor.Type = pm_types.CodedValue('999998')
        self.sdc_device.mdib.descriptions.add_object(my_operation_descriptor)
        sco_handle = 'Sco.mds0'
        sco = self.sdc_device._sco_operations_registries[sco_handle]
        role_provider = self.sdc_device.product_lookup[sco_handle]
        op = role_provider.make_operation_instance(my_operation_descriptor, sco.operation_cls_getter)
        sco.register_operation(op)
        self.sdc_device.mdib.xtra.mk_state_containers_for_all_descriptors()
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()

        operation_handle = my_operation_descriptor.Handle
        proposed_component_state = client_mdib.xtra.mk_proposed_state(operation_target_handle)
        self.assertIsNone(
            proposed_component_state.OperatingHours)  # just to be sure that we know the correct intitial value
        before_state_version = proposed_component_state.StateVersion
        new_operating_hours = 42
        proposed_component_state.OperatingHours = new_operating_hours
        future = set_service.set_component_state(operation_handle=operation_handle,
                                                 proposed_component_states=[proposed_component_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        updated_component_state = client_mdib.states.descriptor_handle.get_one(operation_target_handle)
        self.assertEqual(updated_component_state.StateVersion, before_state_version + 1)
        self.assertEqual(updated_component_state.OperatingHours, new_operating_hours)

    def test_operation_without_handler(self):
        """Verify that a correct response is sent."""
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
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
        logging.getLogger('sdc.client.op_mgr').setLevel(logging.DEBUG)
        logging.getLogger('sdc.device.op_reg').setLevel(logging.DEBUG)
        logging.getLogger('sdc.device.SetService').setLevel(logging.DEBUG)
        logging.getLogger('sdc.device.subscrMgr').setLevel(logging.DEBUG)
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        coding = pm_types.Coding(NomenclatureCodes.MDC_OP_SET_TIME_SYNC_REF_SRC)
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)

        operation_handle = my_operation_descriptor.Handle
        operation = self.sdc_device.get_operation_by_handle(operation_handle)
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            self._logger.info('ntp server = %s', value)
            operation.delayed_processing = True  # first OperationInvokedReport shall have InvocationState.WAIT
            coll = observableproperties.SingleValueCollector(self.sdc_client, 'operation_invoked_report')
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            received_message = coll.result(timeout=5)
            msg_types = received_message.msg_reader.msg_types
            operation_invoked_report = msg_types.OperationInvokedReport.from_node(received_message.p_msg.msg_node)
            self.assertEqual(operation_invoked_report.ReportPart[0].InvocationInfo.InvocationState,
                             msg_types.InvocationState.WAIT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
            time.sleep(0.5)
            # disable delayed processing
            self._logger.info("disable delayed processing")
            operation.delayed_processing = False  # first OperationInvokedReport shall have InvocationState.FINISHED
            coll = observableproperties.SingleValueCollector(self.sdc_client, 'operation_invoked_report')
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            received_message = coll.result(timeout=5)
            msg_types = received_message.msg_reader.msg_types
            operation_invoked_report = msg_types.OperationInvokedReport.from_node(received_message.p_msg.msg_node)
            self.assertEqual(operation_invoked_report.ReportPart[0].InvocationInfo.InvocationState,
                             msg_types.InvocationState.FINISHED)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

    def test_set_operating_mode(self):
        logging.getLogger('sdc.device.subscrMgr').setLevel(logging.DEBUG)
        logging.getLogger('ssdc.client.subscr').setLevel(logging.DEBUG)
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()

        operation_handle = 'SVO.37.3569'
        operation = self.sdc_device.get_operation_by_handle(operation_handle)
        for op_mode in (pm_types.OperatingMode.NA, pm_types.OperatingMode.ENABLED):
            operation.set_operating_mode(op_mode)
            time.sleep(1)
            operation_state = client_mdib.states.descriptor_handle.get_one(operation_handle)
            self.assertEqual(operation_state.OperatingMode, op_mode)

    def test_set_string_value(self):
        """Verify that metricprovider instantiated an operation for SetString call.

         OperationTarget of operation 0815 is an EnumStringMetricState.
         """
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        coding = pm_types.Coding('0815')
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)

        operation_handle = my_operation_descriptor.Handle
        for value in ('ADULT', 'PEDIATRIC'):
            self._logger.info('string value = %s', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptor_handle.get_one(my_operation_descriptor.OperationTarget)
            self.assertEqual(state.MetricValue.Value, value)

    def test_set_metric_value(self):
        """Verify that metricprovider instantiated an operation for SetNumericValue call.

         OperationTarget of operation 0815-1 is a NumericMetricState.
         """
        set_service = self.sdc_client.client('Set')
        client_mdib = ConsumerMdib(self.sdc_client)
        client_mdib.init_mdib()
        coding = pm_types.Coding('0815-1')
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)

        operation_handle = my_operation_descriptor.Handle
        for value in (Decimal(1), Decimal(42)):
            self._logger.info('metric value = %s', value)
            future = set_service.set_numeric_value(operation_handle=operation_handle,
                                                   requested_numeric_value=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msg_types.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptor_handle.get_one(my_operation_descriptor.OperationTarget)
            self.assertEqual(state.MetricValue.Value, value)
