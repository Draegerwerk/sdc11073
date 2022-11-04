import copy
import datetime
import logging
import sys
import time
import unittest
from urllib import parse
from decimal import Decimal
from itertools import product

from lxml import etree as etree_

from sdc11073 import commlog
from sdc11073 import compression
from sdc11073 import loghelper
from sdc11073 import msg_qnames as msg
from sdc11073 import observableproperties
from sdc11073 import pm_qnames as pm
from sdc11073 import pmtypes
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib import ClientMdibContainer
from sdc11073.mdib.devicewaveform import Annotator
from sdc11073.pysoap.soapclient import SoapClient, HTTPReturnCodeError
from sdc11073.roles.nomenclature import NomenclatureCodes as nc
from sdc11073.sdcclient import SdcClient
from sdc11073.sdcclient.components import SdcClientComponents
from sdc11073.sdcclient.subscription import ClientSubscriptionManagerReferenceParams
from sdc11073.sdcdevice import waveforms
from sdc11073.sdcdevice.httpserver import DeviceHttpServerThread
from sdc11073.wsdiscovery import WSDiscoveryWhitelist
from sdc11073.sdcdevice.components import SdcDeviceComponents
from sdc11073.sdcdevice.subscriptionmgr import SubscriptionsManagerReferenceParam
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from tests.mockstuff import SomeDevice, dec_list

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    comm_logger = commlog.CommLogger(log_folder=r'c:\temp\sdc_commlog',
                                     log_out=True,
                                     log_in=True,
                                     broadcast_ip_filter=None)
    commlog.set_communication_logger(comm_logger)

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


def provide_realtime_data(sdc_device):
    waveform_provider = sdc_device.mdib.xtra.waveform_provider
    if waveform_provider is None:
        return
    paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

    flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=1.2, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

    co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveformperiod=1.0, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05506',
                                                  co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

    # make SinusGenerator (0x34F05501) the annotator source
    annotator = Annotator(annotation=pmtypes.Annotation(pmtypes.CodedValue('a', 'b')),
                          trigger_handle='0x34F05501',
                          annotated_handles=['0x34F05500', '0x34F05501', '0x34F05506'])
    waveform_provider.register_annotation_generator(annotator)


class Test_Client_SomeDevice(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()

        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        ns_mapper = self.sdc_device.mdib.nsmapper
        ns_mapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._loc_validators)
        provide_realtime_data(self.sdc_device)

        time.sleep(0.5)  # allow init of devices to complete

        x_addr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(x_addr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE)
        self.sdc_client.start_all(subscribe_periodic_reports=True, async_dispatch=False)
        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        if self.sdc_client:
            self.sdc_client.stop_all()
        if self.sdc_device:
            self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_basic_connect(self):
        # simply check that correct top node is returned
        cl_getService = self.sdc_client.client('Get')
        get_result = cl_getService.get_mdib()  # GetResult
        descriptor_containers, state_containers = get_result.result
        self.assertGreater(len(descriptor_containers), 0)
        self.assertGreater(len(state_containers), 0)

        get_result = cl_getService.get_md_description()
        descriptor_containers = get_result.result
        self.assertGreater(len(descriptor_containers), 0)

        get_result = cl_getService.get_md_state()
        state_containers = get_result.result
        self.assertGreater(len(state_containers), 0)

        contextService = self.sdc_client.client('Context')
        result = contextService.get_context_states()
        self.assertGreater(len(result.result), 0)

    def test_renew_get_status(self):
        for s in self.sdc_client._subscription_mgr.subscriptions.values():
            remaining_seconds = s.renew(1)  # one minute
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins
            remaining_seconds = s.get_status()
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins
            # verify that device returns fault message on wrong subscription identifier
            if s.dev_reference_param.has_parameters:
                # ToDo: manipulate reference parameter
                pass
            else:
                tmp = s._subscription_manager_address
                try:
                    # manipulate path
                    s._subscription_manager_address = parse.ParseResult(
                        scheme=tmp.scheme, netloc=tmp.netloc, path=tmp.path + 'xxx', params=tmp.params,
                        query=tmp.query, fragment=tmp.fragment)
                    # renew
                    self.log_watcher.setPaused(True)  # ignore logged error
                    remaining_seconds = s.renew(1)  # one minute
                    self.log_watcher.setPaused(False)
                    self.assertFalse(s.is_subscribed)  # it did not work
                    self.assertEqual(remaining_seconds, 0)
                    s.is_subscribed = True
                    # get_status
                    self.log_watcher.setPaused(True)  # ignore logged error
                    remaining_seconds = s.get_status()
                    self.log_watcher.setPaused(False)
                    self.assertFalse(s.is_subscribed)  # it did not work
                    self.assertEqual(remaining_seconds, 0)
                    # unsubscribe
                    self.log_watcher.setPaused(True)  # ignore logged error
                    s.unsubscribe()
                    self.log_watcher.setPaused(False)
                    self.assertFalse(s.is_subscribed)  # it did not work

                finally:
                    s._subscription_manager_address = tmp
                    s.is_subscribed = True

    def test_client_stop(self):
        """ verify that sockets get closed"""
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        # first check that we see subscriptions on devices side
        self.assertEqual(len(self.sdc_device.subscriptions_manager._subscriptions.objects),
                         len(self.sdc_client._subscription_mgr.subscriptions))
        subscriptions = list(self.sdc_device.subscriptions_manager._subscriptions.objects)  # make a copy of this list
        for s in subscriptions:
            self.assertFalse(s.is_closed())
        self.sdc_client._subscription_mgr.unsubscribe_all()
        self.assertEqual(len(self.sdc_device.subscriptions_manager._subscriptions.objects), 0)
        for s in subscriptions:
            self.assertTrue(s.is_closed())

    def test_device_stop(self):
        """ verify that sockets get closed"""
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        # first check that we see subscriptions on devices side
        self.assertEqual(len(self.sdc_device.subscriptions_manager._subscriptions.objects),
                         len(self.sdc_client._subscription_mgr.subscriptions))
        subscriptions = list(self.sdc_device.subscriptions_manager._subscriptions.objects)  # make a copy of this list
        for s in subscriptions:
            self.assertFalse(s.is_closed())

        self.sdc_device.stop_all()

        self.assertEqual(len(self.sdc_device.subscriptions_manager._subscriptions.objects), 0)
        for s in subscriptions:
            self.assertTrue(s.is_closed())

    def test_client_stop_no_unsubscribe(self):
        self.log_watcher.setPaused(True)  # this test will have error logs, no check
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        # first check that we see subscriptions on devices side
        self.assertEqual(len(self.sdc_device.subscriptions_manager._subscriptions.objects),
                         len(self.sdc_client._subscription_mgr.subscriptions))
        subscriptions = list(self.sdc_device.subscriptions_manager._subscriptions.objects)  # make a copy of this list
        for s in subscriptions:
            self.assertFalse(s.is_closed())
        self.sdc_client.stop_all(unsubscribe=False)
        time.sleep(SoapClient.SOCKET_TIMEOUT + 2)  # just a little longer than socket timeout 5 seconds
        self.assertLess(len(self.sdc_device.subscriptions_manager._subscriptions.objects),
                        8)  # at least waveform subscription must have ended

        subscriptions = list(self.sdc_device.subscriptions_manager._subscriptions.objects)  # make a copy of this list
        for s in subscriptions:
            self.assertTrue(s.is_closed(), msg=f'subscription is not closed: {s}')

    def test_subscription_end(self):
        self.sdc_device.stop_all()
        time.sleep(1)
        self.sdc_client.stop_all()
        self.sdc_device = None
        self.sdc_client = None

    def test_get_mdstate_parameters(self):
        """ verify that get_md_state correctly handles call parameters
        """
        cl_get_service = self.sdc_client.client('Get')
        result = cl_get_service.get_md_state(['0x34F05500'])
        self.assertEqual(len(result.result), 1)
        result = cl_get_service.get_md_state(['not_existing_handle'])
        self.assertEqual(len(result.result), 0)

    def test_get_md_description_parameters(self):
        """ verify that getMdDescription correctly handles call parameters
        """
        cl_get_service = self.sdc_client.client('Get')
        message_data = cl_get_service.get_md_description(['not_existing_handle'])
        node = message_data.p_msg.msg_node
        print(etree_.tostring(node, pretty_print=True))
        descriptors = list(node[0])  # that is /m:GetMdDescriptionResponse/m:MdDescription/*
        self.assertEqual(len(descriptors), 0)
        message_data = cl_get_service.get_md_description(['0x34F05500'])
        node = message_data.p_msg.msg_node
        print(etree_.tostring(node, pretty_print=True))
        descriptors = list(node[0])
        self.assertEqual(len(descriptors), 1)

    def test_metric_reports(self):
        """ verify that the client receives correct EpisodicMetricReports and PeriodicMetricReports"""
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        # wait for the next EpisodicMetricReport
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_metric_report')
        # wait for the next PeriodicMetricReport
        coll2 = observableproperties.SingleValueCollector(self.sdc_client, 'periodic_metric_report')

        # create a state instance
        descriptor_handle = '0x34F00100'
        first_value = Decimal(12)
        my_physical_connector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)
        now = time.time()
        with self.sdc_device.mdib.transaction_manager(set_determination_time=False) as mgr:
            st = mgr.get_state(descriptor_handle)
            if st.MetricValue is None:
                st.mk_metric_value()
            st.MetricValue.Value = first_value
            st.MetricValue.MetricQuality.Validity = pmtypes.MeasurementValidity.VALID
            st.MetricValue.DeterminationTime = now
            st.PhysiologicalRange = [pmtypes.Range(*dec_list(1, 2, 3, 4, 5)),
                                     pmtypes.Range(*dec_list(10, 20, 30, 40, 50))]
            if self.sdc_device is self.sdc_device:
                st.PhysicalConnector = my_physical_connector

        # verify that client automatically got the state (via EpisodicMetricReport )
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        cl_state1 = cl_mdib.states.descriptorHandle.get_one(descriptor_handle)
        self.assertEqual(cl_state1.MetricValue.Value, first_value)
        self.assertAlmostEqual(cl_state1.MetricValue.DeterminationTime, now, delta=0.01)
        self.assertEqual(cl_state1.MetricValue.MetricQuality.Validity, pmtypes.MeasurementValidity.VALID)
        self.assertEqual(cl_state1.StateVersion, 1)  # this is the first state update after init
        if self.sdc_device is self.sdc_device:
            self.assertEqual(cl_state1.PhysicalConnector, my_physical_connector)

        # set new Value
        new_value = Decimal('13')
        coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                         'episodic_metric_report')  # wait for the next EpisodicMetricReport
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.get_state(descriptor_handle)
            st.MetricValue.Value = new_value

        # verify that client automatically got the state (via EpisodicMetricReport )
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        cl_state1 = cl_mdib.states.descriptorHandle.get_one(descriptor_handle)
        self.assertEqual(cl_state1.MetricValue.Value, new_value)
        self.assertEqual(cl_state1.StateVersion, 2)  # this is the 2nd state update after init

        # verify that client also got a PeriodicMetricReport
        message_data = coll2.result(timeout=NOTIFICATION_TIMEOUT)
        states = self.sdc_client.msg_reader.read_periodic_metric_report(message_data)
        self.assertGreaterEqual(len(states), 1)

    def test_component_state_reports(self):
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()

        # create a state instance
        metric_descriptor_handle = '0x34F00100'  # this is a metric state. look for its parent, that is a component
        metric_descriptor_container = self.sdc_device.mdib.descriptions.handle.get_one(metric_descriptor_handle)
        parent_handle = metric_descriptor_container.parent_handle
        # wait for the next EpisodicComponentReport
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_component_report')
        # wait for the next PeriodicComponentReport
        coll2 = observableproperties.SingleValueCollector(self.sdc_client, 'periodic_component_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.get_state(parent_handle)
            st.ActivationState = pmtypes.ComponentActivation.ON \
                if st.ActivationState != pmtypes.ComponentActivation.ON \
                else pmtypes.ComponentActivation.OFF
            st.OperatingHours = 43
            st.OperatingCycles = 11

        coll.result(timeout=NOTIFICATION_TIMEOUT)
        # verify that client automatically got the state (via EpisodicComponentReport )
        cl_state1 = cl_mdib.states.descriptorHandle.get_one(parent_handle)
        self.assertEqual(cl_state1.diff(st), None)
        # verify that client also got a PeriodicMetricReport
        message_data = coll2.result(timeout=NOTIFICATION_TIMEOUT)
        states = self.sdc_client.msg_reader.read_periodic_component_report(message_data)
        self.assertGreaterEqual(len(states), 1)

    def test_alert_reports(self):
        """ verify that the client receives correct EpisodicAlertReports and PeriodicAlertReports"""
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        # wait for the next PeriodicAlertReport
        coll2 = observableproperties.SingleValueCollector(self.sdc_client, 'periodic_alert_report')

        # pick an AlertCondition for testing
        alert_condition_state = self.sdc_device.mdib.states.NODETYPE[pm.AlertConditionState][0]
        descriptor_handle = alert_condition_state.DescriptorHandle

        for _activation_state, _actual_priority, _presence in product(list(pmtypes.AlertActivation),
                                                                    list(pmtypes.AlertConditionPriority),
                                                                    (True,
                                                                     False)):  # test every possible combination
            # wait for the next EpisodicAlertReport
            coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                             'episodic_alert_report')
            with self.sdc_device.mdib.transaction_manager() as mgr:
                st = mgr.get_state(descriptor_handle)
                st.ActivationState = _activation_state
                st.ActualPriority = _actual_priority
                st.Presence = _presence
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            client_state_container = client_mdib.states.descriptorHandle.get_one(
                descriptor_handle)  # this shall be updated by notification
            self.assertEqual(client_state_container.diff(st), None)

        # pick an AlertSignal for testing
        alert_condition_state = self.sdc_device.mdib.states.NODETYPE[pm.AlertSignalState][0]
        descriptor_handle = alert_condition_state.DescriptorHandle

        for _activation_state, _presence, _location, _slot in product(list(pmtypes.AlertActivation),
                                                                     list(pmtypes.AlertSignalPresence),
                                                                     list(pmtypes.AlertSignalPrimaryLocation),
                                                                     (0, 1, 2)):
            # wait for the next EpisodicAlertReport
            coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_alert_report')
            with self.sdc_device.mdib.transaction_manager() as mgr:
                st = mgr.get_state(descriptor_handle)
                st.ActivationState = _activation_state
                st.Presence = _presence
                st.Location = _location
                st.Slot = _slot
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            client_state_container = client_mdib.states.descriptorHandle.get_one(
                descriptor_handle)  # this shall be updated by notification
            self.assertEqual(client_state_container.diff(st), None)

        # verify that client also got a PeriodicAlertReport
        message_data = coll2.result(timeout=NOTIFICATION_TIMEOUT)
        states = self.sdc_client.msg_reader.read_periodic_alert_report(message_data)
        self.assertGreaterEqual(len(states), 1)

    def test_set_patient_context_on_device(self):
        """device updates patient.
         verify that a notification device->client updates the client mdib."""
        clientMdib = ClientMdibContainer(self.sdc_client)
        clientMdib.init_mdib()

        patientDescriptorContainer = self.sdc_device.mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)

        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_context_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            tr_MdibVersion = self.sdc_device.mdib.mdib_version
            st = mgr.mk_context_state(patientDescriptorContainer.Handle, set_associated=True)
            st.CoreData.Givenname = 'Max'
            st.CoreData.Middlename = ['Willy']
            st.CoreData.Birthname = 'Mustermann'
            st.CoreData.Familyname = 'Musterfrau'
            st.CoreData.Title = 'Rex'
            st.CoreData.Sex = pmtypes.T_Sex.MALE
            st.CoreData.PatientType = pmtypes.PatientType.ADULT
            st.CoreData.Height = pmtypes.Measurement(Decimal('88.2'), pmtypes.CodedValue('abc', 'def'))
            st.CoreData.Weight = pmtypes.Measurement(Decimal('68.2'), pmtypes.CodedValue('abc'))
            st.CoreData.Race = pmtypes.CodedValue('123', 'def')
            st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        patient_context_state_container = clientMdib.context_states.NODETYPE.get_one(
            pm.PatientContextState, allow_none=True)
        self.assertTrue(patient_context_state_container is not None)
        self.assertEqual(patient_context_state_container.CoreData.Givenname, st.CoreData.Givenname)
        self.assertEqual(patient_context_state_container.CoreData.Middlename, st.CoreData.Middlename)
        self.assertEqual(patient_context_state_container.CoreData.Birthname, st.CoreData.Birthname)
        self.assertEqual(patient_context_state_container.CoreData.Familyname, st.CoreData.Familyname)
        self.assertEqual(patient_context_state_container.CoreData.Title, st.CoreData.Title)
        self.assertEqual(patient_context_state_container.CoreData.Sex, st.CoreData.Sex)
        self.assertEqual(patient_context_state_container.CoreData.PatientType, st.CoreData.PatientType)
        self.assertEqual(patient_context_state_container.CoreData.Height, st.CoreData.Height)
        self.assertEqual(patient_context_state_container.CoreData.Weight, st.CoreData.Weight)
        self.assertEqual(patient_context_state_container.CoreData.Race, st.CoreData.Race)
        self.assertEqual(patient_context_state_container.CoreData.DateOfBirth, st.CoreData.DateOfBirth)
        self.assertEqual(patient_context_state_container.BindingMdibVersion,
                         tr_MdibVersion)  # created at the beginning
        self.assertEqual(patient_context_state_container.UnbindingMdibVersion, None)

        # test update of same patient
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_context_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.get_context_state(patient_context_state_container.Handle)
            st.CoreData.Givenname = 'Moritz'
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        patient_context_state_container = clientMdib.context_states.NODETYPE.get_one(
            pm.PatientContextState, allow_none=True)
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Moritz')
        self.assertEqual(patient_context_state_container.BindingMdibVersion,
                         tr_MdibVersion)  # created at the beginning
        self.assertEqual(patient_context_state_container.UnbindingMdibVersion, None)



    def test_get_containment_tree(self):
        self.log_watcher.setPaused(True)  # this will create an error log, but that shall be ignored
        self.assertRaises(HTTPReturnCodeError,
                          self.sdc_client.containment_tree_service_client.get_containment_tree,
                          ['0x34F05500', '0x34F05501', '0x34F05506'])

        self.assertRaises(HTTPReturnCodeError,
                          self.sdc_client.containment_tree_service_client.get_descriptor,
                          ['0x34F05500', '0x34F05501', '0x34F05506'])

    def test_get_supported_languages(self):
        storage = self.sdc_device.localization_storage
        storage.add(pmtypes.LocalizedText('bla', lang='de-de', ref='a', version=1, text_width=pmtypes.T_TextWidth.XS),
                    pmtypes.LocalizedText('foo', lang='en-en', ref='a', version=1, text_width=pmtypes.T_TextWidth.XS)
                    )

        get_request_response = self.sdc_client.localization_service_client.get_supported_languages()
        languages = get_request_response.result
        self.assertEqual(len(languages), 2)
        self.assertTrue('de-de' in languages)
        self.assertTrue('en-en' in languages)

    def test_get_localized_texts(self):
        storage = self.sdc_device.localization_storage
        storage.add(pmtypes.LocalizedText('bla_a', lang='de-de', ref='a', version=1, text_width=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('foo_a', lang='en-en', ref='a', version=1, text_width=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('bla_b', lang='de-de', ref='b', version=1, text_width=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('foo_b', lang='en-en', ref='b', version=1, text_width=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('bla_aa', lang='de-de', ref='a', version=2, text_width=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('foo_aa', lang='en-en', ref='a', version=2, text_width=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('bla_bb', lang='de-de', ref='b', version=2, text_width=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('foo_bb', lang='en-en', ref='b', version=2, text_width=pmtypes.T_TextWidth.S))

        get_request_response = self.sdc_client.localization_service_client.get_localized_texts()
        texts = get_request_response.result
        self.assertEqual(len(texts), 4)
        for t in texts:
            self.assertEqual(t.TextWidth, 's')
            self.assertTrue(t.Ref in ('a', 'b'))

        get_request_response = self.sdc_client.localization_service_client.get_localized_texts(version=1)
        texts = get_request_response.result
        self.assertEqual(len(texts), 4)
        for t in texts:
            self.assertEqual(t.TextWidth, 'xs')

        get_request_response = self.sdc_client.localization_service_client.get_localized_texts(refs=['a'], langs=['de-de'],
                                                                                         version=1)
        texts = get_request_response.result
        self.assertEqual(len(texts), 1)
        self.assertEqual(texts[0].text, 'bla_a')

        get_request_response = self.sdc_client.localization_service_client.get_localized_texts(refs=['b'], langs=['en-en'],
                                                                                         version=2)
        texts = get_request_response.result
        self.assertEqual(len(texts), 1)
        self.assertEqual(texts[0].text, 'foo_bb')

    def test_realtime_samples(self):
        # a random number for maxRealtimeSamples, not too big, otherwise we have to wait too long. 
        # But wait long enough to have at least one full waveform period in buffer for annotations.
        client_mdib = ClientMdibContainer(self.sdc_client, max_realtime_samples=297)
        client_mdib.init_mdib()
        client_mdib.xtra.set_calculate_wf_age_stats(True)
        time.sleep(3.5)  # Wait long enough to make the rt_buffers full.
        d_handles = ('0x34F05500', '0x34F05501', '0x34F05506')

        # now verify that we have real time samples
        for d_handle in d_handles:
            # check content of state container
            container = client_mdib.states.descriptorHandle.get_one(d_handle)
            self.assertEqual(container.ActivationState, pmtypes.ComponentActivation.ON)
            self.assertIsNotNone(container.MetricValue)
            self.assertAlmostEqual(container.MetricValue.DeterminationTime, time.time(), delta=0.5)
            self.assertGreater(len(container.MetricValue.Samples), 1)

        for d_handle in d_handles:
            # check content of rt_buffer
            rt_buffer = client_mdib.rt_buffers.get(d_handle)
            self.assertTrue(rt_buffer is not None, msg='no rtBuffer for handle {}'.format(d_handle))
            rt_data = copy.copy(rt_buffer.rt_data)  # we need a copy that not change during test
            self.assertEqual(len(rt_data), client_mdib._max_realtime_samples)
            self.assertAlmostEqual(rt_data[-1].determination_time, time.time(), delta=0.5)
            with_annotation = [x for x in rt_data if len(x.annotations) > 0]
            # verify that we have annotations
            self.assertGreater(len(with_annotation), 0)
            for w_a in with_annotation:
                self.assertEqual(len(w_a.annotations), 1)
                self.assertEqual(w_a.annotations[0].Type,
                                 pmtypes.CodedValue('a', 'b'))  # like in provide_realtime_data
            # the cycle time of the annotator source is 1.2 seconds. The difference of the observation times must be almost 1.2
            self.assertAlmostEqual(with_annotation[1].determination_time - with_annotation[0].determination_time,
                                   1.2,
                                   delta=0.05)

        # now disable one waveform
        d_handle = d_handles[0]
        waveform_provider = self.sdc_device.mdib.xtra.waveform_provider
        waveform_provider.set_activation_state(d_handle, pmtypes.ComponentActivation.OFF)
        time.sleep(0.5)
        container = client_mdib.states.descriptorHandle.get_one(d_handle)
        self.assertEqual(container.ActivationState, pmtypes.ComponentActivation.OFF)
        self.assertTrue(container.MetricValue is None)

        rt_buffer = client_mdib.rt_buffers.get(d_handle)
        self.assertEqual(len(rt_buffer.rt_data), client_mdib._max_realtime_samples)
        self.assertLess(rt_buffer.rt_data[-1].determination_time, time.time() - 0.4)

        # check waveform for completeness: the delta between all two-value-pairs of the triangle must be identical
        my_handle = d_handles[-1]
        expected_delta = 0.4  # triangle, waveform-period = 1 sec., 10 values per second, max-min=2

        time.sleep(1)
        rt_buffer = client_mdib.rt_buffers.get(my_handle)  # this is the handle for triangle wf
        values = rt_buffer.read_rt_data()
        dt_s = [values[i + 1].determination_time - values[i].determination_time for i in range(len(values) - 1)]
        v_s = [value.dec_value for value in values]
        print(['{:.3f}'.format(x) for x in dt_s])
        print(v_s)
        for i in range(len(values) - 1):
            n, m = values[i], values[i + 1]
            self.assertAlmostEqual(abs(m.value - n.value), expected_delta, delta=0.01)

        dt = values[-1].determination_time - values[1].determination_time
        self.assertAlmostEqual(0.01 * len(values), dt, delta=0.5)

        age_data = client_mdib.xtra.get_wf_age_stdev()
        self.assertLess(abs(age_data.mean_age), 1)
        self.assertLess(abs(age_data.stdev), 0.5)
        self.assertLess(abs(age_data.min_age), 1)
        self.assertGreater(abs(age_data.max_age), 0.0)

    def test_description_modification(self):
        descriptor_handle = '0x34F00100'
        logging.getLogger('sdc.device').setLevel(logging.DEBUG)
        # set value of a metric
        first_value = Decimal(12)
        with self.sdc_device.mdib.transaction_manager() as mgr:
            # mgr automatically increases the StateVersion
            st = mgr.get_state(descriptor_handle)
            if st.MetricValue is None:
                st.mk_metric_value()
            st.MetricValue.Value = first_value
            st.MetricValue.MetricQuality.Validity = pmtypes.MeasurementValidity.VALID

        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        descriptor_container = client_mdib.descriptions.handle.get_one(descriptor_handle)
        initial_descriptor_version = descriptor_container.DescriptorVersion

        state_container = client_mdib.states.descriptorHandle.get_one(descriptor_handle)
        self.assertEqual(state_container.DescriptorVersion, initial_descriptor_version)

        # now update something and  wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                         'description_modification_report')
        new_determination_period = 3.14159
        with self.sdc_device.mdib.transaction_manager() as mgr:
            descr = mgr.get_descriptor(descriptor_handle)
            descr.DeterminationPeriod = new_determination_period
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        device_mdib = self.sdc_device.mdib
        expected_descriptor_version = initial_descriptor_version + 1

        # verify that devices mdib contains the updated descriptor_container
        # plus an updated state wit correct DescriptorVersion
        descriptor_container = device_mdib.descriptions.handle.get_one(descriptor_handle)
        state_container = device_mdib.states.descriptorHandle.get_one(descriptor_handle)
        self.assertEqual(descriptor_container.DescriptorVersion, expected_descriptor_version)
        self.assertEqual(descriptor_container.DeterminationPeriod, new_determination_period)
        self.assertEqual(state_container.DescriptorVersion, expected_descriptor_version)

        # verify that client got updates
        descriptor_container = client_mdib.descriptions.handle.get_one(descriptor_handle)
        state_container = client_mdib.states.descriptorHandle.get_one(descriptor_handle)
        self.assertEqual(descriptor_container.DescriptorVersion, expected_descriptor_version)
        self.assertEqual(descriptor_container.DeterminationPeriod, new_determination_period)
        self.assertEqual(state_container.DescriptorVersion, expected_descriptor_version)

        # test creating a descriptor
        # coll: wait for the next DescriptionModificationReport
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'description_modification_report')
        new_handle = 'a_generated_descriptor'
        node_name = pm.NumericMetricDescriptor
        cls = self.sdc_device.mdib.data_model.get_descriptor_container_class(node_name)
        with self.sdc_device.mdib.transaction_manager() as mgr:
            new_descriptor_container = cls(handle=new_handle,
                                         parent_handle=descriptor_container.parent_handle,
                                         )
            new_descriptor_container.Type = pmtypes.CodedValue('12345')
            new_descriptor_container.Unit = pmtypes.CodedValue('hector')
            new_descriptor_container.Resolution = Decimal('0.42')
            mgr.add_descriptor(new_descriptor_container)
        # long timeout, sometimes high load on jenkins makes these tests fail
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        cl_descriptor_container = client_mdib.descriptions.handle.get_one(new_handle, allow_none=True)
        self.assertEqual(cl_descriptor_container.Handle, new_handle)

        # test deleting a descriptor
        coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                         'description_modification_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            mgr.remove_descriptor(new_handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        cl_descriptor_container = client_mdib.descriptions.handle.get_one(new_handle, allow_none=True)
        self.assertIsNone(cl_descriptor_container)

    def test_alert_condition_modification(self):
        alert_descriptor_handle = '0xD3C00100'
        limit_alert_descriptor_handle = '0xD3C00108'

        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        coll = observableproperties.SingleValueCollector(self.sdc_client, 'description_modification_report')
        # update descriptors
        with self.sdc_device.mdib.transaction_manager() as mgr:
            alert_descriptor = mgr.get_descriptor(alert_descriptor_handle)
            limit_alert_descriptor = mgr.get_descriptor(limit_alert_descriptor_handle)

            # update descriptors
            alert_descriptor.SafetyClassification = pmtypes.SafetyClassification.MED_C
            limit_alert_descriptor.SafetyClassification = pmtypes.SafetyClassification.MED_B
            limit_alert_descriptor.AutoLimitSupported = True
        coll.result(timeout=NOTIFICATION_TIMEOUT)  # wait for update in client
        # verify that descriptor updates are transported to client
        client_alert_descriptor = client_mdib.descriptions.handle.get_one(alert_descriptor_handle)
        self.assertEqual(client_alert_descriptor.SafetyClassification, pmtypes.SafetyClassification.MED_C)

        client_limit_alert_descriptor = client_mdib.descriptions.handle.get_one(limit_alert_descriptor_handle)
        self.assertEqual(client_limit_alert_descriptor.SafetyClassification, pmtypes.SafetyClassification.MED_B)
        self.assertEqual(client_limit_alert_descriptor.AutoLimitSupported, True)

        # set alert state presence to true
        time.sleep(0.01)
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_alert_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            alert_state = mgr.get_state(alert_descriptor_handle)

            limit_alert_state = mgr.get_state(limit_alert_descriptor_handle)

            alert_state.Presence = True
            alert_state.ActualPriority = pmtypes.AlertConditionPriority.HIGH
            limit_alert_state.ActualPriority = pmtypes.AlertConditionPriority.MEDIUM
            limit_alert_state.Limits = pmtypes.Range(upper=Decimal('3'))

        coll.result(timeout=NOTIFICATION_TIMEOUT)  # wait for update in client
        # verify that state updates are transported to client
        client_alert_state = client_mdib.states.descriptorHandle.get_one(alert_descriptor_handle)
        self.assertEqual(client_alert_state.ActualPriority, pmtypes.AlertConditionPriority.HIGH)
        self.assertEqual(client_alert_state.Presence, True)

        # verify that alert system state is also updated
        alert_system_descr = client_mdib.descriptions.handle.get_one(client_alert_descriptor.parent_handle)
        alert_system_state = client_mdib.states.descriptorHandle.get_one(alert_system_descr.Handle)
        self.assertTrue(alert_descriptor_handle in alert_system_state.PresentPhysiologicalAlarmConditions)
        self.assertGreater(alert_system_state.SelfCheckCount, 0)

        client_limit_alert_state = client_mdib.states.descriptorHandle.get_one(limit_alert_descriptor_handle)
        self.assertEqual(client_limit_alert_state.ActualPriority, pmtypes.AlertConditionPriority.MEDIUM)
        self.assertEqual(client_limit_alert_state.Limits, pmtypes.Range(upper=Decimal(3)))
        self.assertEqual(client_limit_alert_state.Presence, False)
        self.assertEqual(client_limit_alert_state.MonitoredAlertLimits,
                         pmtypes.AlertConditionMonitoredLimits.NONE)  # default

    def test_metadata_modification(self):
        with self.sdc_device.mdib.transaction_manager() as mgr:
            # set Metadata
            mds_descriptor_handle = self.sdc_device.mdib.descriptions.NODETYPE.get_one(pm.MdsDescriptor).Handle
            mds_descriptor = mgr.get_descriptor(mds_descriptor_handle)
            mds_descriptor.MetaData.Manufacturer.append(pmtypes.LocalizedText(u'Draeger GmbH'))
            mds_descriptor.MetaData.ModelName.append(pmtypes.LocalizedText(u'pySDC'))
            mds_descriptor.MetaData.SerialNumber.append('pmDCBA-4321')
            mds_descriptor.MetaData.ModelNumber = '1.09'

        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        cl_mds_descriptor = client_mdib.descriptions.NODETYPE.get_one(pm.MdsDescriptor)
        self.assertEqual(cl_mds_descriptor.MetaData.ModelNumber, '1.09')
        self.assertEqual(cl_mds_descriptor.MetaData.Manufacturer[-1].text, u'Draeger GmbH')

    def test_remove_mds(self):
        self.sdc_device.stop_realtime_sample_loop()
        time.sleep(0.1)
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()
        dev_descriptor_count1 = len(self.sdc_device.mdib.descriptions.objects)
        descr_handles = list(self.sdc_device.mdib.descriptions.handle.keys())
        state_descriptor_handles = list(self.sdc_device.mdib.states.descriptorHandle.keys())
        context_state_handles = list(self.sdc_device.mdib.context_states.handle.keys())
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'description_modification_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            mds_descriptor = self.sdc_device.mdib.descriptions.NODETYPE.get_one(pm.MdsDescriptor)
            mgr.remove_descriptor(mds_descriptor.Handle)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        # verify that all state versions were saved
        descr_handles_lookup1 = copy.copy(self.sdc_device.mdib.descriptions.handle_version_lookup)
        state_descriptor_handles_lookup1 = copy.copy(self.sdc_device.mdib.states.handle_version_lookup)
        context_state_descriptor_handles_lookup1 = copy.copy(self.sdc_device.mdib.context_states.handle_version_lookup)
        for h in descr_handles:
            self.assertTrue(h in descr_handles_lookup1)
        for h in state_descriptor_handles:
            self.assertTrue(h in state_descriptor_handles_lookup1)
        for h in context_state_handles:
            self.assertTrue(h in context_state_descriptor_handles_lookup1)

        # verify that client mdib has same number of objects as device mdib
        dev_descriptor_count2 = len(self.sdc_device.mdib.descriptions.objects)
        dev_state_count2 = len(self.sdc_device.mdib.states.objects)
        cl_descriptor_count2 = len(client_mdib.descriptions.objects)
        cl_state_count2 = len(client_mdib.states.objects)
        self.assertTrue(dev_descriptor_count2 < dev_descriptor_count1)
        self.assertEqual(dev_descriptor_count2, 0)
        self.assertEqual(dev_descriptor_count2, cl_descriptor_count2)
        self.assertEqual(dev_state_count2, cl_state_count2)

    def test_client_mdib_observables(self):
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        # wait for the next EpisodicMetricReport
        coll = observableproperties.SingleValueCollector(client_mdib, 'metrics_by_handle')
        descriptor_handle = '0x34F00100'
        first_value = Decimal('12')
        with self.sdc_device.mdib.transaction_manager(set_determination_time=False) as mgr:
            st = mgr.get_state(descriptor_handle)
            if st.MetricValue is None:
                st.mk_metric_value()
            st.MetricValue.Value = first_value
            st.MetricValue.MetricQuality.Validity = pmtypes.MeasurementValidity.VALID
            st.MetricValue.DeterminationTime = time.time()
            st.PhysiologicalRange = [pmtypes.Range(*dec_list(1, 2, 3, 4, 5)),
                                     pmtypes.Range(*dec_list(10, 20, 30, 40, 50))]
        data = coll.result(timeout=NOTIFICATION_TIMEOUT)
        self.assertTrue(descriptor_handle in data.keys())
        self.assertEqual(st.MetricValue.Value, data[descriptor_handle].MetricValue.Value)  # compare some data

        coll = observableproperties.SingleValueCollector(client_mdib,
                                                         'alert_by_handle')  # wait for the next EpisodicAlertReport
        descriptor_handle = '0xD3C00108'  # an AlertConditionDescriptorHandle
        with self.sdc_device.mdib.transaction_manager(set_determination_time=False) as mgr:
            st = mgr.get_state(descriptor_handle)
            st.Presence = True
            st.Rank = 3
            st.DeterminationTime = time.time()
        data = coll.result(timeout=NOTIFICATION_TIMEOUT)
        self.assertTrue(descriptor_handle in data.keys())
        self.assertEqual(st.Rank, data[descriptor_handle].Rank)  # compare some data

        coll = observableproperties.SingleValueCollector(client_mdib, 'updated_descriptors_by_handle')
        descriptor_handle = '0x34F00100'
        with self.sdc_device.mdib.transaction_manager(set_determination_time=False) as mgr:
            descr = mgr.get_descriptor(descriptor_handle)
            descr.DeterminationPeriod = 42
        data = coll.result(timeout=NOTIFICATION_TIMEOUT)
        self.assertTrue(descriptor_handle in data.keys())
        self.assertEqual(descr.DeterminationPeriod,
                         data[descriptor_handle].DeterminationPeriod)  # compare some data

        coll = observableproperties.SingleValueCollector(client_mdib,
                                                         'waveform_by_handle')  # wait for the next WaveformReport
        # waveforms are already sent, no need to trigger anything
        data = coll.result(timeout=NOTIFICATION_TIMEOUT)
        self.assertGreater(len(data.keys()), 0)  # at least one real time sample array

    def test_is_connected_unfriendly(self):
        """ Test device stop without sending subscription end messages"""
        self.log_watcher.setPaused(True)
        time.sleep(1)
        self.assertEqual(self.sdc_client.is_connected, True)
        collectors = []
        coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                         'is_connected')  # waiter for the next state transition
        collectors.append(coll)
        self.sdc_device.stop_all(send_subscription_end=False)
        for coll in collectors:
            is_connected = coll.result(timeout=15)
            self.assertEqual(is_connected, False)
        self.sdc_client.stop_all(unsubscribe=False)  # without unsubscribe, is faster and would make no sense anyway

    def test_is_connected_friendly(self):
        """ Test device stop with sending subscription end messages"""
        self.log_watcher.setPaused(True)
        time.sleep(1)
        self.assertEqual(self.sdc_client.is_connected, True)
        collectors = []
        coll = observableproperties.SingleValueCollector(self.sdc_client,
                                                         'is_connected')  # waiter for the next state transition
        collectors.append(coll)
        self.sdc_device.stop_all(send_subscription_end=True)
        for coll in collectors:
            is_connected = coll.result(timeout=15)
            self.assertEqual(is_connected, False)
        self.sdc_client.stop_all(unsubscribe=False)  # without unsubscribe, is faster and would make no sense anyway

    def test_invalid_request(self):
        """MDPWS R0012: If a HOSTED SERVICE receives a MESSAGE that is inconsistent with its WSDL description, the HOSTED
        SERVICE SHOULD generate a SOAP Fault with a Code Value of 'Sender', unless a 'MustUnderstand' or
        'VersionMismatch' Fault is generated
        """
        self.log_watcher.setPaused(True)
        self.sdc_client.get_service_client._validate = False  # want to send an invalid request
        try:
            method = 'Nonsense'
            message = self.sdc_client.get_service_client._msg_factory._mk_get_method_message(
                self.sdc_client.get_service_client.endpoint_reference.address,
                self.sdc_client.get_service_client.porttype,
                method)
            self.sdc_client.get_service_client._call_get_method(message, method)
        except HTTPReturnCodeError as ex:
            self.assertEqual(ex.status, 400)
            self.assertEqual(ex.soap_fault.code, 's12:Sender')
        else:
            self.fail('HTTPReturnCodeError not raised')

    def test_extension(self):
        def are_equivalent(node1, node2):
            if node1.tag != node2.tag or node1.attrib != node2.attrib or node1.text != node2.text:
                return False
            for ch1, ch2 in zip(node1, node2):
                if not are_equivalent(ch1, ch2):
                    return False
            return True

        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        for cl_descriptor in cl_mdib.descriptions.objects:
            dev_descriptor = self.sdc_device.mdib.descriptions.handle.get_one(cl_descriptor.Handle)
            self.assertEqual(dev_descriptor.Extension.value.keys(), cl_descriptor.Extension.value.keys())
            for key, dev_val in dev_descriptor.Extension.value.items():
                cl_val = cl_descriptor.Extension.value[key]
                try:
                    if isinstance(dev_val, etree_._Element):
                        self.assertTrue(are_equivalent(dev_val, cl_val))
                    else:
                        self.assertEqual(dev_val, cl_val)
                except:
                    raise


class Test_DeviceCommonHttpServer(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()

        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1', poc='CU1', bed='Bed')
        self.sdc_device_1 = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<dev1> ')

        # common http server for both devices, borrow ssl context from device
        self.httpserver = DeviceHttpServerThread(
            my_ipaddress='0.0.0.0', ssl_context=self.sdc_device_1._ssl_context,
            supported_encodings=compression.CompressionHandler.available_encodings[:],
            msg_reader=self.sdc_device_1.msg_reader, msg_factory=self.sdc_device_1.msg_factory,
            log_prefix='http_srv')
        self.httpserver.start()
        self.httpserver.started_evt.wait(timeout=5)

        self.sdc_device_1.start_all(shared_http_server=self.httpserver)
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device_1.set_location(location, self._loc_validators)
        provide_realtime_data(self.sdc_device_1)

        self.sdc_device_2 = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<dev2> ')
        self.sdc_device_2.start_all(shared_http_server=self.httpserver)
        self.sdc_device_2.set_location(location, self._loc_validators)
        provide_realtime_data(self.sdc_device_2)

        time.sleep(0.5)  # allow full init of devices

        x_addr = self.sdc_device_1.get_xaddrs()
        self.sdcClient_1 = SdcClient(x_addr[0],
                                     sdc_definitions=self.sdc_device_1.mdib.sdc_definitions,
                                     ssl_context=None,
                                     validate=CLIENT_VALIDATE,
                                     log_prefix='<cl1> ')
        self.sdcClient_1.start_all()

        x_addr = self.sdc_device_2.get_xaddrs()
        self.sdcClient_2 = SdcClient(x_addr[0],
                                     sdc_definitions=self.sdc_device_2.mdib.sdc_definitions,
                                     ssl_context=None,
                                     validate=CLIENT_VALIDATE,
                                     log_prefix='<cl2> ')
        self.sdcClient_2.start_all()

        self._all_cl_dev = ((self.sdcClient_1, self.sdc_device_1),
                            (self.sdcClient_2, self.sdc_device_2))

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        for sdc_client, sdc_device in self._all_cl_dev:
            sdc_client.stop_all()
            sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_basic_connect(self):
        # simply check that correct top node is returned
        for sdc_client, _ in self._all_cl_dev:
            cl_get_service = sdc_client.client('Get')
            get_result = cl_get_service.get_mdib()
            descriptor_containers, state_containers = get_result.result
            self.assertGreater(len(descriptor_containers), 0)
            self.assertGreater(len(state_containers), 0)

            get_result = cl_get_service.get_md_description()
            descriptor_containers = get_result.result
            self.assertGreater(len(descriptor_containers), 0)

            get_result = cl_get_service.get_md_state()
            state_containers = get_result.result
            self.assertGreater(len(state_containers), 0)

            context_service = sdc_client.client('Context')
            result = context_service.get_context_states()
            self.assertGreater(len(result.result), 0)


class Test_Client_SomeDevice_chunked(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<Final> ',
                                                    chunked_messages=True)
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        ns_mapper = self.sdc_device.mdib.nsmapper
        ns_mapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all()
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._loc_validators)
        provide_realtime_data(self.sdc_device)

        time.sleep(0.5)  # allow full init of devices

        x_addr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(x_addr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE,
                                    log_prefix='<Final> ',
                                    chunked_requests=True)
        self.sdc_client.start_all()

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        self.sdc_client.stop_all()
        self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_basic_connect(self):
        # simply check that correct top node is returned
        get_service = self.sdc_client.client('Get')
        message_data = get_service.get_md_description()
        self.assertEqual(message_data.msg_name, 'GetMdDescriptionResponse')

        message_data = get_service.get_mdib()
        self.assertEqual(message_data.msg_name, 'GetMdibResponse')

        message_data = get_service.get_md_state()
        self.assertEqual(message_data.msg_name, 'GetMdStateResponse')

        context_service = self.sdc_client.client('Context')
        result = context_service.get_context_states()
        self.assertGreater(len(result.result), 0)


class TestClientSomeDeviceReferenceParametersDispatch(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1', poc='CU1', bed='Bed')
        specific_components = SdcDeviceComponents(subscriptions_manager_class=SubscriptionsManagerReferenceParam,
                                                  soap_client_class=SoapClientAsync
                                                  )
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<Final> ',
                                                    specific_components=specific_components,
                                                    chunked_messages=True)
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        ns_mapper = self.sdc_device.mdib.nsmapper
        ns_mapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all()
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._loc_validators)

        time.sleep(0.5)  # allow full init of devices

        x_addr = self.sdc_device.get_xaddrs()
        specific_components = SdcClientComponents(subscription_manager_class=ClientSubscriptionManagerReferenceParams)
        self.sdc_client = SdcClient(x_addr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE,
                                    log_prefix='<Final> ',
                                    specific_components=specific_components,
                                    chunked_requests=True)
        self.sdc_client.start_all()

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        self.sdc_client.stop_all()
        self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_basic_connect(self):
        # simply check that correct top node is returned
        get_service = self.sdc_client.client('Get')
        get_request_result = get_service.get_md_description()
        node = get_request_result.p_msg.msg_node
        self.assertEqual(node.tag, str(msg.GetMdDescriptionResponse))
        self.assertEqual(get_request_result.msg_name, 'GetMdDescriptionResponse')

        get_request_result = get_service.get_mdib()
        node = get_request_result.p_msg.msg_node
        self.assertEqual(node.tag, str(msg.GetMdibResponse))
        self.assertEqual(get_request_result.msg_name, 'GetMdibResponse')

        get_request_result = get_service.get_md_state()
        node = get_request_result.p_msg.msg_node
        self.assertEqual(node.tag, str(msg.GetMdStateResponse))
        self.assertEqual(get_request_result.msg_name, 'GetMdStateResponse')

        context_service = self.sdc_client.client('Context')
        get_request_result = context_service.get_context_states()
        self.assertGreater(len(get_request_result.result), 0)
        node = get_request_result.p_msg.msg_node
        self.assertEqual(node.tag, str(msg.GetContextStatesResponse))
        self.assertEqual(get_request_result.msg_name, 'GetContextStatesResponse')

    def test_renew_get_status(self):
        """ If renew and get_status work, then reference parameters based dispatching works. """
        for s in self.sdc_client._subscription_mgr.subscriptions.values():
            remaining_seconds = s.renew(1)  # one minute
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins
            remaining_seconds = s.get_status()
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins

    def test_subscription_end(self):
        self.sdc_device.stop_all()
        time.sleep(1)
        self.sdc_client.stop_all()
