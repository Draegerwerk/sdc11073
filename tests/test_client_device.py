import copy
import datetime
import logging
import sys
import time
import unittest
from itertools import product

from lxml import etree as etree_

from sdc11073 import commlog
from sdc11073 import compression
from sdc11073 import loghelper
from sdc11073 import namespaces
from sdc11073 import observableproperties
from sdc11073 import pmtypes
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib import ClientMdibContainer
from sdc11073.pysoap.soapclient import SoapClient, HTTPReturnCodeError
from sdc11073.pysoap.soapenvelope import ReceivedSoapFault
from sdc11073.roles.nomenclature import NomenclatureCodes as nc
from sdc11073.sdcclient import SdcClient
from sdc11073.sdcdevice import waveforms
from sdc11073.sdcdevice.httpserver import DeviceHttpServerThread
from sdc11073.sdcdevice.subscriptionmgr import SubscriptionsManagerReferenceParam
from sdc11073.wsdiscovery import WSDiscoveryWhitelist
from sdc11073.definitions_base import SdcDeviceComponents

from tests.mockstuff import SomeDevice

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


class Test_Client_SomeDevice(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()

        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_level=logging.INFO)
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        nsmapper = self.sdc_device.mdib.nsmapper
        nsmapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all(periodic_reports_interval=1.0)
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._locValidators)
        self.provideRealtimeData(self.sdc_device)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE)
        self.sdc_client.start_all(subscribe_periodic_reports=True)

        self._all_cl_dev = [(self.sdc_client, self.sdc_device)]

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all()
            sdcDevice.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    @staticmethod
    def provideRealtimeData(sdcDevice):
        paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

        flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=1.2, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

        co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveformperiod=1.0, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05506', co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

        # make SinusGenerator (0x34F05501) the annotator source
        annotation = pmtypes.Annotation(pmtypes.CodedValue('a', 'b'))  # what is CodedValue for startOfInspirationCycle?
        sdcDevice.mdib.register_annotation_generator(annotation,
                                                     trigger_handle='0x34F05501',
                                                     annotated_handles=('0x34F05500', '0x34F05501', '0x34F05506'))

    def test_BasicConnect(self):
        # simply check that correct top node is returned
        for sdcClient, _ in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            node = cl_getService.get_md_description_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdDescriptionResponse')))

            node = cl_getService.get_mdib_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdibResponse')))

            node = cl_getService.get_md_state_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdStateResponse')))

            contextService = sdcClient.client('Context')
            node = contextService.get_context_states_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetContextStatesResponse')))

    def test_renew_getStatus(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            for s in sdcClient._subscription_mgr.subscriptions.values():
                remaining_seconds = s.renew(1)  # one minute
                self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins
                remaining_seconds = s.get_status()
                self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins

    def test_clientStop(self):
        """ verify that sockets get closed"""
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()
            # first check that we see subscriptions on devices side
            self.assertEqual(len(sdcDevice.subscriptions_manager._subscriptions.objects),
                             len(sdcClient._subscription_mgr.subscriptions))
            subscriptions = list(sdcDevice.subscriptions_manager._subscriptions.objects)  # make a copy of this list
            for s in subscriptions:
                self.assertFalse(s.is_closed())
            sdcClient._subscription_mgr.unsubscribe_all()
            self.assertEqual(len(sdcDevice.subscriptions_manager._subscriptions.objects), 0)
            for s in subscriptions:
                self.assertTrue(s.is_closed())

    def test_deviceStop(self):
        """ verify that sockets get closed"""
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()
            # first check that we see subscriptions on devices side
            self.assertEqual(len(sdcDevice.subscriptions_manager._subscriptions.objects),
                             len(sdcClient._subscription_mgr.subscriptions))
            subscriptions = list(sdcDevice.subscriptions_manager._subscriptions.objects)  # make a copy of this list
            for s in subscriptions:
                self.assertFalse(s.is_closed())

            sdcDevice.stop_all()

            self.assertEqual(len(sdcDevice.subscriptions_manager._subscriptions.objects), 0)
            for s in subscriptions:
                self.assertTrue(s.is_closed())

    def test_clientStopNoUnsubscribe(self):
        self.log_watcher.setPaused(True)  # this test will have error logs, no check
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()
            # first check that we see subscriptions on devices side
            self.assertEqual(len(sdcDevice.subscriptions_manager._subscriptions.objects),
                             len(sdcClient._subscription_mgr.subscriptions))
            subscriptions = list(sdcDevice.subscriptions_manager._subscriptions.objects)  # make a copy of this list
            for s in subscriptions:
                self.assertFalse(s.is_closed())
            sdcClient.stop_all(unsubscribe=False, close_all_connections=True)
            time.sleep(SoapClient.SOCKET_TIMEOUT + 2)  # just a little bit longer than socket timeout 5 seconds
            self.assertLess(len(sdcDevice.subscriptions_manager._subscriptions.objects),
                            8)  # at least waveform subscription must have ended

            subscriptions = list(sdcDevice.subscriptions_manager._subscriptions.objects)  # make a copy of this list
            for s in subscriptions:
                self.assertTrue(s.is_closed())

    def test_subscriptionEnd(self):
        for _, sdcDevice in self._all_cl_dev:
            sdcDevice.stop_all()
        time.sleep(1)
        for sdcClient, _ in self._all_cl_dev:
            sdcClient.stop_all()
        self._all_cl_dev = []

    def test_getMdStateParameters(self):
        """ verify that get_md_state correctly handles call parameters
        """
        for sdcClient, _ in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            node = cl_getService.get_md_state_node(['nonexisting_handle'])
            print(etree_.tostring(node, pretty_print=True))
            states = list(node[0])  # that is /m:GetMdStateResponse/m:MdState/*
            self.assertEqual(len(states), 0)
            node = cl_getService.get_md_state_node(['0x34F05500'])
            print(etree_.tostring(node, pretty_print=True))
            states = list(node[0])  # that is /m:GetMdStateResponse/m:MdState/*
            self.assertEqual(len(states), 1)

    def test_getMdDescriptionParameters(self):
        """ verify that getMdDescription correctly handles call parameters
        """
        for sdcClient, _ in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            node = cl_getService.get_md_description_node(['nonexisting_handle'])
            print(etree_.tostring(node, pretty_print=True))
            descriptors = list(node[0])  # that is /m:GetMdDescriptionResponse/m:MdDescription/*
            self.assertEqual(len(descriptors), 0)
            node = cl_getService.get_md_description_node(['0x34F05500'])
            print(etree_.tostring(node, pretty_print=True))
            descriptors = list(node[0])
            self.assertEqual(len(descriptors), 1)

    def test_metric_reports(self):
        """ verify that the client receives correct EpisodicMetricReports and PeriodicMetricReports"""
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()
            # wait for the next EpisodicMetricReport
            coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_metric_report')
            # wait for the next PeriodicMetricReport
            coll2 = observableproperties.SingleValueCollector(sdcClient, 'periodic_metric_report')

            # create a state instance
            descriptorHandle = '0x34F00100'
            firstValue = 12
            myPhysicalConnector = pmtypes.PhysicalConnectorInfo([pmtypes.LocalizedText('ABC')], 1)
            now = time.time()
            with sdcDevice.mdib.transaction_manager(set_determination_time=False) as mgr:
                # st = mgr.getMetricState(descriptorHandle)
                st = mgr.get_state(descriptorHandle)
                if st.MetricValue is None:
                    st.mk_metric_value()
                st.MetricValue.Value = firstValue
                st.MetricValue.MetricQuality.Validity = pmtypes.MeasurementValidity.VALID
                st.MetricValue.DeterminationTime = now
                st.PhysiologicalRange = [pmtypes.Range(1, 2, 3, 4, 5), pmtypes.Range(10, 20, 30, 40, 50)]
                if sdcDevice is self.sdc_device:
                    st.PhysicalConnector = myPhysicalConnector

            # verify that client automatically got the state (via EpisodicMetricReport )
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            cl_state1 = cl_mdib.states.descriptorHandle.get_one(descriptorHandle)
            self.assertEqual(cl_state1.MetricValue.Value, firstValue)
            self.assertAlmostEqual(cl_state1.MetricValue.DeterminationTime, now, delta=0.01)
            self.assertEqual(cl_state1.MetricValue.MetricQuality.Validity, pmtypes.MeasurementValidity.VALID)
            self.assertEqual(cl_state1.StateVersion, 1)  # this is the first state update after init
            if sdcDevice is self.sdc_device:
                self.assertEqual(cl_state1.PhysicalConnector, myPhysicalConnector)

            # set new Value
            newValue = 13
            coll = observableproperties.SingleValueCollector(sdcClient,
                                                             'episodic_metric_report')  # wait for the next EpisodicMetricReport
            with sdcDevice.mdib.transaction_manager() as mgr:
                # st = mgr.getMetricState(descriptorHandle)
                st = mgr.get_state(descriptorHandle)
                st.MetricValue.Value = newValue

            # verify that client automatically got the state (via EpisodicMetricReport )
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            cl_state1 = cl_mdib.states.descriptorHandle.get_one(descriptorHandle)
            self.assertEqual(cl_state1.MetricValue.Value, newValue)
            self.assertEqual(cl_state1.StateVersion, 2)  # this is the 2nd state update after init

            # verify that client also got a PeriodicMetricReport
            periodic_report = coll2.result(timeout=NOTIFICATION_TIMEOUT)
            state_nodes = periodic_report.xpath('//msg:MetricState', namespaces=namespaces.nsmap)
            self.assertGreaterEqual(len(state_nodes), 1)

    def test_component_state_reports(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()

            # create a state instance
            metricDescriptorHandle = '0x34F00100'  # this is a metric state. look for its parent, that is a component
            metricDescriptorContainer = sdcDevice.mdib.descriptions.handle.get_one(metricDescriptorHandle)
            parent_handle = metricDescriptorContainer.parent_handle
            # wait for the next EpisodicComponentReport
            coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_component_report')
            # wait for the next PeriodicComponentReport
            coll2 = observableproperties.SingleValueCollector(sdcClient, 'periodic_component_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                st = mgr.get_state(parent_handle)
                st.ActivationState = pmtypes.ComponentActivation.ON \
                    if st.ActivationState != pmtypes.ComponentActivation.ON \
                    else pmtypes.ComponentActivation.OFF
                st.OperatingHours = 43
                st.OperatingCycles = 11

            coll.result(timeout=NOTIFICATION_TIMEOUT)
            # verify that client automatically got the state (via EpisodicComponentReport )
            cl_state1 = cl_mdib.states.descriptorHandle.get_one(parent_handle)
            self.assertEqual(cl_state1.diff(st), [])
            # verify that client also got a PeriodicMetricReport
            periodic_report = coll2.result(timeout=NOTIFICATION_TIMEOUT)
            state_nodes = periodic_report.xpath('//msg:ComponentState', namespaces=namespaces.nsmap)
            self.assertGreaterEqual(len(state_nodes), 1)

    def test_alert_reports(self):
        """ verify that the client receives correct EpisodicAlertReports and PeriodicAlertReports"""
        for sdcClient, sdcDevice in self._all_cl_dev:
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            # wait for the next PeriodicAlertReport
            coll2 = observableproperties.SingleValueCollector(sdcClient, 'periodic_alert_report')

            # pick an AlertCondition for testing
            alertConditionDescr = sdcDevice.mdib.states.NODETYPE[namespaces.domTag('AlertConditionState')][0]
            descriptorHandle = alertConditionDescr.descriptorHandle

            for _activationState, _actualPriority, _presence in product(list(pmtypes.AlertActivation),
                                                                        list(pmtypes.AlertConditionPriority),
                                                                        (True,
                                                                         False)):  # test every possible combination
                # wait for the next EpisodicAlertReport
                coll = observableproperties.SingleValueCollector(sdcClient,
                                                                 'episodic_alert_report')
                with sdcDevice.mdib.transaction_manager() as mgr:
                    st = mgr.get_state(descriptorHandle)
                    st.ActivationState = _activationState
                    st.ActualPriority = _actualPriority
                    st.Presence = _presence
                coll.result(timeout=NOTIFICATION_TIMEOUT)
                clientStateContainer = clientMdib.states.descriptorHandle.get_one(
                    descriptorHandle)  # this shall be updated by notification
                self.assertEqual(clientStateContainer.diff(st), [])

            # pick an AlertSignal for testing
            alertConditionDescr = sdcDevice.mdib.states.NODETYPE[namespaces.domTag('AlertSignalState')][0]
            descriptorHandle = alertConditionDescr.descriptorHandle

            for _activationState, _presence, _location, _slot in product(list(pmtypes.AlertActivation),
                                                                         list(pmtypes.AlertSignalPresence),
                                                                         list(pmtypes.AlertSignalPrimaryLocation),
                                                                         (0, 1, 2)):
                coll = observableproperties.SingleValueCollector(sdcClient,
                                                                 'episodic_alert_report')  # wait for the next EpisodicAlertReport
                with sdcDevice.mdib.transaction_manager() as mgr:
                    st = mgr.get_state(descriptorHandle)
                    st.ActivationState = _activationState
                    st.Presence = _presence
                    st.Location = _location
                    st.Slot = _slot
                coll.result(timeout=NOTIFICATION_TIMEOUT)
                clientStateContainer = clientMdib.states.descriptorHandle.get_one(
                    descriptorHandle)  # this shall be updated by notification
                self.assertEqual(clientStateContainer.diff(st), [])

            # verify that client also got a PeriodicAlertReport
            periodic_report = coll2.result(timeout=NOTIFICATION_TIMEOUT)
            state_nodes = periodic_report.xpath('//msg:AlertState', namespaces=namespaces.nsmap)
            self.assertGreaterEqual(len(state_nodes), 1)
            pass

    def test_setPatientContextOperation(self):
        """client calls corresponding operation.
        - verify that operation is successful.
         verify that a notification device->client also updates the client mdib."""
        for sdcClient, sdcDevice in self._all_cl_dev:
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()
            patientDescriptorContainer = clientMdib.descriptions.NODETYPE.get_one(
                namespaces.domTag('PatientContextDescriptor'))
            # initially the device shall not have any patient
            patientContextStateContainer = clientMdib.context_states.NODETYPE.get_one(
                namespaces.domTag('PatientContext'),
                allow_none=True)
            self.assertIsNone(patientContextStateContainer)

            myOperations = clientMdib.get_operation_descriptors_for_descriptor_handle(patientDescriptorContainer.handle,
                                                                                      NODETYPE=namespaces.domTag(
                                                                                          'SetContextStateOperationDescriptor'))
            self.assertEqual(len(myOperations), 1)
            operation_handle = myOperations[0].handle
            print('Handle for SetContextSTate Operation = {}'.format(operation_handle))
            context = sdcClient.client('Context')

            # insert a new patient with wrong handle, this shall fail
            proposedContext = context.mk_proposed_context_object(patientDescriptorContainer.handle)
            proposedContext.Handle = 'some_nonexisting_handle'
            proposedContext.CoreData.Givenname = 'Karl'
            proposedContext.CoreData.Middlename = ['M.']
            proposedContext.CoreData.Familyname = 'Klammer'
            proposedContext.CoreData.Birthname = 'Bourne'
            proposedContext.CoreData.Title = 'Dr.'
            proposedContext.CoreData.Sex = 'M'
            proposedContext.CoreData.PatientType = pmtypes.PatientType.ADULT
            proposedContext.CoreData.setBirthdate('2000-12-12')
            proposedContext.CoreData.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
            proposedContext.CoreData.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
            proposedContext.CoreData.Race = pmtypes.CodedValue('somerace')
            future = context.set_context_state(operation_handle, [proposedContext])
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FAILED)

            # insert a new patient with correct handle, this shall succeed
            proposedContext.Handle = patientDescriptorContainer.handle
            future = context.set_context_state(operation_handle, [proposedContext])
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            self.assertTrue(result.error in ('', 'Unspec'))
            self.assertEqual(result.errorMsg, '')

            # check client side patient context, this shall have been set via notification
            patientContextStateContainer = clientMdib.context_states.NODETYPE.get_one(
                namespaces.domTag('PatientContextState'))
            self.assertEqual(patientContextStateContainer.CoreData.Givenname, 'Karl')
            self.assertEqual(patientContextStateContainer.CoreData.Middlename, ['M.'])
            self.assertEqual(patientContextStateContainer.CoreData.Familyname, 'Klammer')
            self.assertEqual(patientContextStateContainer.CoreData.Birthname, 'Bourne')
            self.assertEqual(patientContextStateContainer.CoreData.Title, 'Dr.')
            self.assertEqual(patientContextStateContainer.CoreData.Sex, 'M')
            self.assertEqual(patientContextStateContainer.CoreData.PatientType, pmtypes.PatientType.ADULT)
            self.assertEqual(patientContextStateContainer.CoreData.Height.MeasuredValue, 88.2)
            self.assertEqual(patientContextStateContainer.CoreData.Weight.MeasuredValue, 68.2)
            self.assertEqual(patientContextStateContainer.CoreData.Race, pmtypes.CodedValue('somerace'))
            self.assertNotEqual(patientContextStateContainer.Handle,
                                patientDescriptorContainer.handle)  # device replaced it with its own handle
            self.assertEqual(patientContextStateContainer.ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)

            # test update of the patient
            proposedContext = context.mk_proposed_context_object(patientDescriptorContainer.handle,
                                                                 handle=patientContextStateContainer.Handle)
            proposedContext.CoreData.Givenname = 'Karla'
            future = context.set_context_state(operation_handle, [proposedContext])
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            patientContextStateContainer = clientMdib.context_states.handle.get_one(patientContextStateContainer.Handle)
            self.assertEqual(patientContextStateContainer.CoreData.Givenname, 'Karla')
            self.assertEqual(patientContextStateContainer.CoreData.Familyname, 'Klammer')

            # set new patient, check binding mdib versions and context association
            proposedContext = context.mk_proposed_context_object(patientDescriptorContainer.handle)
            proposedContext.CoreData.Givenname = 'Heidi'
            proposedContext.CoreData.Middlename = ['M.']
            proposedContext.CoreData.Familyname = 'Klammer'
            proposedContext.CoreData.Birthname = 'Bourne'
            proposedContext.CoreData.Title = 'Dr.'
            proposedContext.CoreData.Sex = 'F'
            proposedContext.CoreData.PatientType = pmtypes.PatientType.ADULT
            proposedContext.CoreData.setBirthdate('2000-12-12')
            proposedContext.CoreData.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
            proposedContext.CoreData.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
            proposedContext.CoreData.Race = pmtypes.CodedValue('somerace')
            future = context.set_context_state(operation_handle, [proposedContext])
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            self.assertTrue(result.error in ('', 'Unspec'))
            self.assertEqual(result.errorMsg, '')
            patientContextStateContainers = clientMdib.context_states.NODETYPE.get(
                namespaces.domTag('PatientContextState'))
            # sort by BindingMdibVersion
            patientContextStateContainers.sort(key=lambda obj: obj.BindingMdibVersion)
            self.assertEqual(len(patientContextStateContainers), 2)
            oldPatient = patientContextStateContainers[0]
            newPatient = patientContextStateContainers[1]
            self.assertEqual(oldPatient.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
            self.assertEqual(newPatient.ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)

            # create a patient locally on device, then test update from client
            coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_context_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                st = mgr.get_state(patientDescriptorContainer.handle)
                st.CoreData.Givenname = 'Max123'
                st.CoreData.Middlename = ['Willy']
                st.CoreData.Birthname = 'Mustermann'
                st.CoreData.Familyname = 'Musterfrau'
                st.CoreData.Title = 'Rex'
                st.CoreData.Sex = 'M'
                st.CoreData.PatientType = pmtypes.PatientType.ADULT
                st.CoreData.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
                st.CoreData.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
                st.CoreData.Race = pmtypes.CodedValue('123', 'def')
                st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            patientContextStateContainers = clientMdib.context_states.NODETYPE.get(
                namespaces.domTag('PatientContextState'))
            myPatient = [p for p in patientContextStateContainers if p.CoreData.Givenname == 'Max123']
            self.assertEqual(len(myPatient), 1)
            myPatient = myPatient[0]
            proposedContext = context.mk_proposed_context_object(patientDescriptorContainer.handle, myPatient.Handle)
            proposedContext.CoreData.Givenname = 'Karl123'
            future = context.set_context_state(operation_handle, [proposedContext])
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            myPatient2 = sdcDevice.mdib.context_states.handle.get_one(myPatient.Handle)
            self.assertEqual(myPatient2.CoreData.Givenname, 'Karl123')

    def test_setPatientContextOnDevice(self):
        """device updates patient.
         verify that a notification device->client updates the client mdib."""
        for sdcClient, sdcDevice in self._all_cl_dev:
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            patientDescriptorContainer = sdcDevice.mdib.descriptions.NODETYPE.get_one(
                namespaces.domTag('PatientContextDescriptor'))

            coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_context_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                tr_MdibVersion = sdcDevice.mdib.mdib_version
                st = mgr.get_state(patientDescriptorContainer.handle)
                st.CoreData.Givenname = 'Max'
                st.CoreData.Middlename = ['Willy']
                st.CoreData.Birthname = 'Mustermann'
                st.CoreData.Familyname = 'Musterfrau'
                st.CoreData.Title = 'Rex'
                st.CoreData.Sex = 'M'
                st.CoreData.PatientType = pmtypes.PatientType.ADULT
                st.CoreData.Height = pmtypes.Measurement(88.2, pmtypes.CodedValue('abc', 'def'))
                st.CoreData.Weight = pmtypes.Measurement(68.2, pmtypes.CodedValue('abc'))
                st.CoreData.Race = pmtypes.CodedValue('123', 'def')
                st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            patientContextStateContainer = clientMdib.context_states.NODETYPE.get_one(
                namespaces.domTag('PatientContextState'), allow_none=True)
            self.assertTrue(patientContextStateContainer is not None)
            self.assertEqual(patientContextStateContainer.CoreData.Givenname, st.CoreData.Givenname)
            self.assertEqual(patientContextStateContainer.CoreData.Middlename, st.CoreData.Middlename)
            self.assertEqual(patientContextStateContainer.CoreData.Birthname, st.CoreData.Birthname)
            self.assertEqual(patientContextStateContainer.CoreData.Familyname, st.CoreData.Familyname)
            self.assertEqual(patientContextStateContainer.CoreData.Title, st.CoreData.Title)
            self.assertEqual(patientContextStateContainer.CoreData.Sex, st.CoreData.Sex)
            self.assertEqual(patientContextStateContainer.CoreData.PatientType, st.CoreData.PatientType)
            self.assertEqual(patientContextStateContainer.CoreData.Height, st.CoreData.Height)
            self.assertEqual(patientContextStateContainer.CoreData.Weight, st.CoreData.Weight)
            self.assertEqual(patientContextStateContainer.CoreData.Race, st.CoreData.Race)
            self.assertEqual(patientContextStateContainer.CoreData.DateOfBirth, st.CoreData.DateOfBirth)
            self.assertEqual(patientContextStateContainer.BindingMdibVersion,
                             tr_MdibVersion)  # created at the beginning
            self.assertEqual(patientContextStateContainer.UnbindingMdibVersion, None)

            # test update of same patient
            coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_context_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                st = mgr.get_state(patientDescriptorContainer.handle, patientContextStateContainer.Handle)
                st.CoreData.Givenname = 'Moritz'
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            patientContextStateContainer = clientMdib.context_states.NODETYPE.get_one(
                namespaces.domTag('PatientContextState'), allow_none=True)
            self.assertEqual(patientContextStateContainer.CoreData.Givenname, 'Moritz')
            self.assertEqual(patientContextStateContainer.BindingMdibVersion,
                             tr_MdibVersion)  # created at the beginning
            self.assertEqual(patientContextStateContainer.UnbindingMdibVersion, None)

    def test_LocationContext(self):
        # initially the device shall have one location, and the client must have it in its mdib
        for sdcClient, sdcDevice in self._all_cl_dev:
            deviceMdib = sdcDevice.mdib
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            dev_locations = deviceMdib.context_states.NODETYPE.get(namespaces.domTag('LocationContextState'))
            cl_locations = clientMdib.context_states.NODETYPE.get(namespaces.domTag('LocationContextState'))
            self.assertEqual(len(dev_locations), 1)
            self.assertEqual(len(cl_locations), 1)
            self.assertEqual(dev_locations[0].Handle, cl_locations[0].Handle)
            self.assertEqual(cl_locations[0].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
            self.assertEqual(cl_locations[0].BindingMdibVersion, 0)  # created at the beginning
            self.assertEqual(cl_locations[0].UnbindingMdibVersion, None)

            for i in range(10):
                current_bed = 'Bed_{}'.format(i)
                new_location = SdcLocation(fac='tklx', poc='CU2', bed=current_bed)
                coll = observableproperties.SingleValueCollector(clientMdib, 'context_by_handle')
                sdcDevice.set_location(new_location)
                coll.result(timeout=NOTIFICATION_TIMEOUT)
                dev_locations = deviceMdib.context_states.NODETYPE.get(namespaces.domTag('LocationContextState'))
                cl_locations = clientMdib.context_states.NODETYPE.get(namespaces.domTag('LocationContextState'))
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
                self.assertEqual(dev_locations[-1].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
                self.assertEqual(cl_locations[-1].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
                self.assertEqual(dev_locations[-1].UnbindingMdibVersion, None)
                self.assertEqual(cl_locations[-1].UnbindingMdibVersion, None)

                for j, loc in enumerate(dev_locations[:-1]):
                    self.assertEqual(loc.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
                    self.assertEqual(loc.UnbindingMdibVersion, dev_locations[j + 1].BindingMdibVersion)

                for j, loc in enumerate(cl_locations[:-1]):
                    self.assertEqual(loc.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
                    self.assertEqual(loc.UnbindingMdibVersion, cl_locations[j + 1].BindingMdibVersion)

    def test_AudioPause_SDC(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device
        alertSystemDescriptorType = namespaces.domTag('AlertSystemDescriptor')

        alertSystemDescriptors = sdcDevice.mdib.descriptions.NODETYPE.get(alertSystemDescriptorType)
        self.assertTrue(alertSystemDescriptors is not None)
        self.assertGreater(len(alertSystemDescriptors), 0)

        setService = sdcClient.client('Set')
        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()
        coding = pmtypes.Coding(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
        operation = sdcDevice.mdib.descriptions.coding.get_one(coding)
        future = setService.activate(operation_handle=operation.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.state
        self.assertEqual(state, pmtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alertSystemDescriptors = sdcDevice.mdib.descriptions.NODETYPE.get(alertSystemDescriptorType)
        self.assertTrue(alertSystemDescriptors is not None)
        self.assertGreater(len(alertSystemDescriptors), 0)
        for alertSystemDescriptor in alertSystemDescriptors:
            state = sdcClient.mdib.states.descriptorHandle.get_one(alertSystemDescriptor.handle)
            # we know that the state has only one SystemSignalActivation entity, which is audible and should be paused now
            self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.PAUSED)

        coding = pmtypes.Coding(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
        operation = sdcDevice.mdib.descriptions.coding.get_one(coding)
        future = setService.activate(operation_handle=operation.handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.state
        self.assertEqual(state, pmtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alertSystemDescriptors = sdcDevice.mdib.descriptions.NODETYPE.get(alertSystemDescriptorType)
        self.assertTrue(alertSystemDescriptors is not None)
        self.assertGreater(len(alertSystemDescriptors), 0)
        for alertSystemDescriptor in alertSystemDescriptors:
            state = sdcClient.mdib.states.descriptorHandle.get_one(alertSystemDescriptor.handle)
            self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.ON)

    def test_setNtpServer_SDC(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device
        setService = sdcClient.client('Set')
        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()
        coding = pmtypes.Coding(nc.MDC_OP_SET_TIME_SYNC_REF_SRC)
        myOperationDescriptor = sdcDevice.mdib.descriptions.coding.get_one(coding, allow_none=True)
        if myOperationDescriptor is None:
            # try old code:
            coding = pmtypes.Coding(nc.OP_SET_NTP)
            myOperationDescriptor = sdcDevice.mdib.descriptions.coding.get_one(coding)

        operation_handle = myOperationDescriptor.handle
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            print('ntp server', value)
            future = setService.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            self.assertTrue(result.error in ('', 'Unspec'))
            self.assertEqual(result.errorMsg, '')

            # verify that the corresponding state has been updated
            state = clientMdib.states.descriptorHandle.get_one(myOperationDescriptor.OperationTarget)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                # look for the ClockState child
                clockDescriptors = clientMdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'), [])
                clockDescriptors = [c for c in clockDescriptors if c.descriptor_handle == state.descriptorHandle]
                if len(clockDescriptors) == 1:
                    state = clientMdib.states.descriptorHandle.get_one(clockDescriptors[0].handle)

            self.assertEqual(state.ReferenceSource[0].text, value)

    def test_setTimeZone_SDC(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device
        setService = sdcClient.client('Set')
        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()

        coding = pmtypes.Coding(nc.MDC_ACT_SET_TIME_ZONE)
        myOperationDescriptor = sdcDevice.mdib.descriptions.coding.get_one(coding, allow_none=True)
        if myOperationDescriptor is None:
            # use old code:
            coding = pmtypes.Coding(nc.OP_SET_TZ)
            myOperationDescriptor = sdcDevice.mdib.descriptions.coding.get_one(coding)

        operation_handle = myOperationDescriptor.handle
        for value in ('+03:00', '-03:00'):  # are these correct values?
            print('time zone', value)
            future = setService.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.state
            self.assertEqual(state, pmtypes.InvocationState.FINISHED)
            self.assertTrue(result.error in ('', 'Unspec'))
            self.assertEqual(result.errorMsg, '')

            # verify that the corresponding state has been updated
            state = clientMdib.states.descriptorHandle.get_one(myOperationDescriptor.OperationTarget)
            if state.NODETYPE == namespaces.domTag('MdsState'):
                # look for the ClockState child
                clockDescriptors = clientMdib.descriptions.NODETYPE.get(namespaces.domTag('ClockDescriptor'), [])
                clockDescriptors = [c for c in clockDescriptors if c.parent_handle == state.descriptorHandle]
                if len(clockDescriptors) == 1:
                    state = clientMdib.states.descriptorHandle.get_one(clockDescriptors[0].handle)
            self.assertEqual(state.TimeZone, value)

    def test_setMetricState_SDC(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device

        # first we need to add a set_metric_state Operation
        scoDescriptors = sdcDevice.mdib.descriptions.NODETYPE.get(namespaces.domTag('ScoDescriptor'))
        cls = sdcDevice.mdib.sdc_definitions.get_descriptor_container_class(
            namespaces.domTag('SetMetricStateOperationDescriptor'))
        myCode = pmtypes.CodedValue(99999)
        setMetricStateOperationDescriptorContainer = sdcDevice.mdib.descriptor_factory._create_descriptor_container(
            cls, 'HANDLE_FOR_MY_TEST', scoDescriptors[0].handle, myCode, pmtypes.SafetyClassification.INF)
        setMetricStateOperationDescriptorContainer.OperationTarget = '0x34F001D5'
        setMetricStateOperationDescriptorContainer.Type = pmtypes.CodedValue(999998)
        sdcDevice.mdib.descriptions.add_object(setMetricStateOperationDescriptorContainer)
        op = sdcDevice.product_roles.metric_provider.make_operation_instance(
            setMetricStateOperationDescriptorContainer, sdcDevice.sco_operations_registry.operation_cls_getter)
        sdcDevice.sco_operations_registry.register_operation(op)
        sdcDevice.mdib.mk_state_containers_for_all_descriptors()
        setService = sdcClient.client('Set')
        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()

        myOperationDescriptor = setMetricStateOperationDescriptorContainer
        operation_handle = myOperationDescriptor.handle
        proposedMetricState = clientMdib.mk_proposed_state('0x34F001D5')
        self.assertIsNone(proposedMetricState.LifeTimePeriod)  # just to be sure that we know the correct intitial value
        before_stateversion = proposedMetricState.StateVersion
        newLifeTimePeriod = 42.5
        proposedMetricState.LifeTimePeriod = newLifeTimePeriod
        future = setService.set_metric_state(operation_handle=operation_handle,
                                             proposed_metric_states=[proposedMetricState])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.state
        self.assertEqual(state, pmtypes.InvocationState.FINISHED)
        self.assertTrue(result.error in ('', 'Unspec'))
        self.assertEqual(result.errorMsg, '')
        updatedMetricState = clientMdib.states.descriptorHandle.get_one('0x34F001D5')
        self.assertEqual(updatedMetricState.StateVersion, before_stateversion + 1)
        self.assertAlmostEqual(updatedMetricState.LifeTimePeriod, newLifeTimePeriod)

    def test_setComponentState_SDC(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device

        operationtarget_handle = '2.1.2.1'  # a channel
        # first we need to add a set_component_state Operation
        scoDescriptors = sdcDevice.mdib.descriptions.NODETYPE.get(namespaces.domTag('ScoDescriptor'))
        cls = sdcDevice.mdib.sdc_definitions.get_descriptor_container_class(
            namespaces.domTag('SetComponentStateOperationDescriptor'))
        myCode = pmtypes.CodedValue(99999)
        setComponentStateOperationDescriptorContainer = sdcDevice.mdib.descriptor_factory._create_descriptor_container(
            cls,
            'HANDLE_FOR_MY_TEST',
            scoDescriptors[0].handle,
            myCode,
            pmtypes.SafetyClassification.INF)
        setComponentStateOperationDescriptorContainer.OperationTarget = operationtarget_handle
        setComponentStateOperationDescriptorContainer.Type = pmtypes.CodedValue(999998)
        sdcDevice.mdib.descriptions.add_object(setComponentStateOperationDescriptorContainer)
        op = sdcDevice.product_roles.make_operation_instance(setComponentStateOperationDescriptorContainer,
                                                             sdcDevice.sco_operations_registry.operation_cls_getter)
        sdcDevice.sco_operations_registry.register_operation(op)
        sdcDevice.mdib.mk_state_containers_for_all_descriptors()
        setService = sdcClient.client('Set')
        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()

        myOperationDescriptor = setComponentStateOperationDescriptorContainer
        operation_handle = myOperationDescriptor.handle
        proposedComponentState = clientMdib.mk_proposed_state(operationtarget_handle)
        self.assertIsNone(
            proposedComponentState.OperatingHours)  # just to be sure that we know the correct intitial value
        before_stateversion = proposedComponentState.StateVersion
        newOperatingHours = 42
        proposedComponentState.OperatingHours = newOperatingHours
        future = setService.set_component_state(operation_handle=operation_handle,
                                                proposed_component_states=[proposedComponentState])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.state
        self.assertEqual(state, pmtypes.InvocationState.FINISHED)
        self.assertTrue(result.error in ('', 'Unspec'))
        self.assertEqual(result.errorMsg, '')
        updatedComponentState = clientMdib.states.descriptorHandle.get_one(operationtarget_handle)
        self.assertEqual(updatedComponentState.StateVersion, before_stateversion + 1)
        self.assertEqual(updatedComponentState.OperatingHours, newOperatingHours)

    def test_GetContainmentTree(self):
        self.log_watcher.setPaused(True)  # this will create an error log, but that shall be ignored
        for sdcClient, sdcDevice in self._all_cl_dev:
            self.assertRaises(HTTPReturnCodeError,
                              sdcClient.containment_tree_service_client.get_containment_tree_nodes,
                              ['0x34F05500', '0x34F05501', '0x34F05506'])

            self.assertRaises(HTTPReturnCodeError,
                              sdcClient.containment_tree_service_client.get_descriptor_node,
                              ['0x34F05500', '0x34F05501', '0x34F05506'])

    def test_getSupportedLanguages(self):
        sdcDevice = self.sdc_device
        sdcClient = self.sdc_client
        storage = sdcDevice.localization_storage
        storage.add(pmtypes.LocalizedText('bla', lang='de-de', ref='a', version=1, textWidth=pmtypes.T_TextWidth.XS),
                    pmtypes.LocalizedText('foo', lang='en-en', ref='a', version=1, textWidth=pmtypes.T_TextWidth.XS)
                    )

        languages = sdcClient.localization_service_client.get_supported_languages()
        self.assertEqual(len(languages), 2)
        self.assertTrue('de-de' in languages)
        self.assertTrue('en-en' in languages)

    def test_getLocalizedTexts(self):
        sdcDevice = self.sdc_device
        sdcClient = self.sdc_client
        storage = sdcDevice.localization_storage
        storage.add(pmtypes.LocalizedText('bla_a', lang='de-de', ref='a', version=1, textWidth=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('foo_a', lang='en-en', ref='a', version=1, textWidth=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('bla_b', lang='de-de', ref='b', version=1, textWidth=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('foo_b', lang='en-en', ref='b', version=1, textWidth=pmtypes.T_TextWidth.XS))
        storage.add(pmtypes.LocalizedText('bla_aa', lang='de-de', ref='a', version=2, textWidth=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('foo_aa', lang='en-en', ref='a', version=2, textWidth=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('bla_bb', lang='de-de', ref='b', version=2, textWidth=pmtypes.T_TextWidth.S))
        storage.add(pmtypes.LocalizedText('foo_bb', lang='en-en', ref='b', version=2, textWidth=pmtypes.T_TextWidth.S))

        texts = sdcClient.localization_service_client._get_localized_texts()
        self.assertEqual(len(texts), 4)
        for t in texts:
            self.assertEqual(t.TextWidth, 's')
            self.assertTrue(t.Ref in ('a', 'b'))

        texts = sdcClient.localization_service_client._get_localized_texts(version=1)
        self.assertEqual(len(texts), 4)
        for t in texts:
            self.assertEqual(t.TextWidth, 'xs')

        texts = sdcClient.localization_service_client._get_localized_texts(refs=['a'], langs=['de-de'], version=1)
        self.assertEqual(len(texts), 1)
        self.assertEqual(texts[0].text, 'bla_a')

        texts = sdcClient.localization_service_client._get_localized_texts(refs=['b'], langs=['en-en'], version=2)
        self.assertEqual(len(texts), 1)
        self.assertEqual(texts[0].text, 'foo_bb')

    def test_ScoDefaultContent(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            mddescrNode = cl_getService.get_md_description_node()
            print(etree_.tostring(mddescrNode))
            scoNodes = mddescrNode.xpath('//dom:Sco', namespaces=namespaces.nsmap)
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()
            scoContainers = clientMdib.descriptions.NODETYPE.get(namespaces.domTag('ScoDescriptor'))
            self.assertEqual(len(scoContainers), len(scoNodes))
            operationContainers = clientMdib.get_operation_descriptors()
            # verify that a state exits for each operation
            for opContainer in operationContainers:
                print('testing operation handle {}'.format(opContainer.handle))
                state_containers = clientMdib.states.descriptorHandle.get(opContainer.handle)
                self.assertEqual(len(state_containers), 1)
                state_container = state_containers[0]
                self.assertEqual(state_container.OperatingMode, 'En')

    def test_realtimeSamples(self):
        # a random number for maxRealtimeSamples, not too big, otherwise we have to wait too long. 
        # But wait long enough to have at least one full waveform period in buffer for annotations.
        for sdcClient, sdcDevice in self._all_cl_dev:
            clientMdib = ClientMdibContainer(sdcClient, max_realtime_samples=297)
            clientMdib.init_mdib()
            time.sleep(3.5)  # Wait long enough to make the rt_buffers full.
            d_handles = ('0x34F05500', '0x34F05501', '0x34F05506')

            # now verify that we have real time samples
            for d_handle in d_handles:
                # check content of state container
                container = clientMdib.states.descriptorHandle.get_one(d_handle)
                self.assertEqual(container.ActivationState, pmtypes.ComponentActivation.ON)
                self.assertIsNotNone(container.MetricValue)
                self.assertAlmostEqual(container.MetricValue.DeterminationTime, time.time(), delta=0.5)
                self.assertGreater(len(container.MetricValue.Samples), 1)

            for d_handle in d_handles:
                # check content of rt_buffer
                rtBuffer = clientMdib.rt_buffers.get(d_handle)
                self.assertTrue(rtBuffer is not None, msg='no rtBuffer for handle {}'.format(d_handle))
                rt_data = copy.copy(rtBuffer.rt_data)  # we need a copy that that not change during test
                self.assertEqual(len(rt_data), clientMdib._max_realtime_samples)
                self.assertAlmostEqual(rt_data[-1].determination_time, time.time(), delta=0.5)
                with_annotation = [x for x in rt_data if len(x.annotations) > 0]
                # verify that we have annotations
                self.assertGreater(len(with_annotation), 1)
                for w_a in with_annotation:
                    self.assertEqual(len(w_a.annotations), 1)
                    self.assertEqual(w_a.annotations[0].Type,
                                     pmtypes.CodedValue('a', 'b'))  # like in provideRealtimeData
                # the cycle time of the annotator source is 1.2 seconds. The difference of the observation times must be almost 1.2
                self.assertAlmostEqual(with_annotation[1].determination_time - with_annotation[0].determination_time,
                                       1.2,
                                       delta=0.05)

            # now disable one waveform
            d_handle = d_handles[0]
            sdcDevice.mdib.set_waveform_generator_activation_state(d_handle, pmtypes.ComponentActivation.OFF)
            time.sleep(0.5)
            container = clientMdib.states.descriptorHandle.get_one(d_handle)
            self.assertEqual(container.ActivationState, pmtypes.ComponentActivation.OFF)
            self.assertTrue(container.MetricValue is None)

            rtBuffer = clientMdib.rt_buffers.get(d_handle)
            self.assertEqual(len(rtBuffer.rt_data), clientMdib._max_realtime_samples)
            self.assertLess(rtBuffer.rt_data[-1].determination_time, time.time() - 0.4)

            # check waveform for completeness: the delta between all two-value-pairs of the triangle must be identical
            my_handle = d_handles[-1]
            expected_delta = 0.4  # triangle, waveform-period = 1 sec., 10 values per second, max-min=2

            time.sleep(1)
            rtBuffer = clientMdib.rt_buffers.get(my_handle)  # this is the handle for triangle wf
            values = rtBuffer.read_rt_data()
            dt_s = [values[i + 1].determination_time - values[i].determination_time for i in range(len(values) - 1)]
            v_s = [value.value for value in values]
            print(['{:.3f}'.format(x) for x in dt_s])
            print(v_s)
            for i in range(len(values) - 1):
                n, m = values[i], values[i + 1]
                self.assertAlmostEqual(abs(m.value - n.value), expected_delta, delta=0.01)

            dt = values[-1].determination_time - values[1].determination_time
            self.assertAlmostEqual(0.01 * len(values), dt, delta=0.5)

            age_data = clientMdib.get_wf_age_stdev()
            self.assertLess(abs(age_data.mean_age), 1)
            self.assertLess(abs(age_data.stdev), 0.5)
            self.assertLess(abs(age_data.min_age), 1)
            self.assertGreater(abs(age_data.max_age), 0.0)

    def test_DescriptionModification(self):
        descriptorHandle = '0x34F00100'
        logging.getLogger('sdc.device').setLevel(logging.DEBUG)
        for sdcClient, sdcDevice in self._all_cl_dev:
            # set value of a metric
            firstValue = 12
            with sdcDevice.mdib.transaction_manager() as mgr:
                # mgr automatically increases the StateVersion
                st = mgr.get_state(descriptorHandle)
                if st.MetricValue is None:
                    st.mk_metric_value()
                st.MetricValue.Value = firstValue
                st.MetricValue.Validity = 'Vld'

            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            descriptor_container = clientMdib.descriptions.handle.get_one(descriptorHandle)
            initialDescriptorVersion = descriptor_container.DescriptorVersion

            state_container = clientMdib.states.descriptorHandle.get_one(descriptorHandle)
            self.assertEqual(state_container.DescriptorVersion, initialDescriptorVersion)

            # now update something and  wait for the next DescriptionModificationReport
            coll = observableproperties.SingleValueCollector(sdcClient,
                                                             'description_modification_report')
            newDeterminationPeriod = 3.14159
            with sdcDevice.mdib.transaction_manager() as mgr:
                descr = mgr.get_descriptor(descriptorHandle)
                descr.DeterminationPeriod = newDeterminationPeriod
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            deviceMdib = sdcDevice.mdib
            expectedDescriptorVersion = initialDescriptorVersion + 1

            # verify that devices mdib contains the updated descriptor_container
            # plus an updated state wit correct DescriptorVersion
            descriptor_container = deviceMdib.descriptions.handle.get_one(descriptorHandle)
            state_container = deviceMdib.states.descriptorHandle.get_one(descriptorHandle)
            self.assertEqual(descriptor_container.DescriptorVersion, expectedDescriptorVersion)
            self.assertEqual(descriptor_container.DeterminationPeriod, newDeterminationPeriod)
            self.assertEqual(state_container.DescriptorVersion, expectedDescriptorVersion)

            # verify that client got updates
            descriptor_container = clientMdib.descriptions.handle.get_one(descriptorHandle)
            state_container = clientMdib.states.descriptorHandle.get_one(descriptorHandle)
            self.assertEqual(descriptor_container.DescriptorVersion, expectedDescriptorVersion)
            self.assertEqual(descriptor_container.DeterminationPeriod, newDeterminationPeriod)
            self.assertEqual(state_container.DescriptorVersion, expectedDescriptorVersion)

            # test creating a descriptor
            # coll: wait for the next DescriptionModificationReport
            coll = observableproperties.SingleValueCollector(sdcClient, 'description_modification_report')
            new_handle = 'a_generated_descriptor'
            node_name = namespaces.domTag('NumericMetricDescriptor')
            cls = sdcDevice.mdib.sdc_definitions.get_descriptor_container_class(node_name)
            with sdcDevice.mdib.transaction_manager() as mgr:
                newDescriptorContainer = cls(nsmapper=sdcDevice.mdib.nsmapper,
                                             handle=new_handle,
                                             parent_handle=descriptor_container.parent_handle,
                                             )
                newDescriptorContainer.Type = pmtypes.CodedValue('12345')
                newDescriptorContainer.Unit = pmtypes.CodedValue('hector')
                newDescriptorContainer.Resolution = 0.42
                mgr.add_descriptor(newDescriptorContainer)
            coll.result(
                timeout=NOTIFICATION_TIMEOUT)  # long timeout, sometimes high load on jenkins makes these tests fail
            cl_descriptorContainer = clientMdib.descriptions.handle.get_one(new_handle, allow_none=True)
            self.assertEqual(cl_descriptorContainer.handle, new_handle)

            # test deleting a descriptor
            coll = observableproperties.SingleValueCollector(sdcClient,
                                                             'description_modification_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                mgr.remove_descriptor(new_handle)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            cl_descriptorContainer = clientMdib.descriptions.handle.get_one(new_handle, allow_none=True)
            self.assertIsNone(cl_descriptorContainer)

    def test_AlertConditionModification(self):
        sdcClient = self.sdc_client
        sdcDevice = self.sdc_device
        alertDescriptorHandle = '0xD3C00100'
        limitAlertDescriptorHandle = '0xD3C00108'

        clientMdib = ClientMdibContainer(sdcClient)
        clientMdib.init_mdib()

        coll = observableproperties.SingleValueCollector(sdcClient, 'description_modification_report')
        # update descriptors
        with sdcDevice.mdib.transaction_manager() as mgr:
            alertDescriptor = mgr.get_descriptor(alertDescriptorHandle)
            limitAlertDescriptor = mgr.get_descriptor(limitAlertDescriptorHandle)

            # update descriptors
            alertDescriptor.SafetyClassification = pmtypes.SafetyClassification.MED_C
            limitAlertDescriptor.SafetyClassification = pmtypes.SafetyClassification.MED_B
            limitAlertDescriptor.AutoLimitSupported = True
        coll.result(timeout=NOTIFICATION_TIMEOUT)  # wait for update in client
        # verify that descriptor updates are transported to client
        clientAlertDescriptor = clientMdib.descriptions.handle.get_one(alertDescriptorHandle)
        self.assertEqual(clientAlertDescriptor.SafetyClassification, pmtypes.SafetyClassification.MED_C)

        clientLimitAlertDescriptor = clientMdib.descriptions.handle.get_one(limitAlertDescriptorHandle)
        self.assertEqual(clientLimitAlertDescriptor.SafetyClassification, pmtypes.SafetyClassification.MED_B)
        self.assertEqual(clientLimitAlertDescriptor.AutoLimitSupported, True)

        # set alert state presence to true
        time.sleep(0.01)
        coll = observableproperties.SingleValueCollector(sdcClient, 'episodic_alert_report')
        with sdcDevice.mdib.transaction_manager() as mgr:
            alertState = mgr.get_state(alertDescriptorHandle)

            limitAlertState = mgr.get_state(limitAlertDescriptorHandle)

            alertState.Presence = True
            alertState.ActualPriority = pmtypes.AlertConditionPriority.HIGH
            limitAlertState.ActualPriority = pmtypes.AlertConditionPriority.MEDIUM
            limitAlertState.Limits = pmtypes.Range(upper=3)

        coll.result(timeout=NOTIFICATION_TIMEOUT)  # wait for update in client
        # verify that state updates are transported to client
        clientAlertState = clientMdib.states.descriptorHandle.get_one(alertDescriptorHandle)
        self.assertEqual(clientAlertState.ActualPriority, pmtypes.AlertConditionPriority.HIGH)
        self.assertEqual(clientAlertState.Presence, True)

        # verify that alert system state is also updated
        alertSystemDescr = clientMdib.descriptions.handle.get_one(clientAlertDescriptor.parent_handle)
        alertSystemState = clientMdib.states.descriptorHandle.get_one(alertSystemDescr.handle)
        self.assertTrue(alertDescriptorHandle in alertSystemState.PresentPhysiologicalAlarmConditions)
        self.assertGreater(alertSystemState.SelfCheckCount, 0)

        clientLimitAlertState = clientMdib.states.descriptorHandle.get_one(limitAlertDescriptorHandle)
        self.assertEqual(clientLimitAlertState.ActualPriority, pmtypes.AlertConditionPriority.MEDIUM)
        self.assertEqual(clientLimitAlertState.Limits, pmtypes.Range(upper=3))
        self.assertEqual(clientLimitAlertState.Presence, False)
        self.assertEqual(clientLimitAlertState.MonitoredAlertLimits,
                         pmtypes.AlertConditionMonitoredLimits.ALL_OFF)  # default

    def test_metadata_modification(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            with sdcDevice.mdib.transaction_manager() as mgr:
                # set Metadata
                mdsDescriptorHandle = sdcDevice.mdib.descriptions.NODETYPE.get_one(
                    namespaces.domTag('MdsDescriptor')).handle
                mdsDescriptor = mgr.get_descriptor(mdsDescriptorHandle)
                mdsDescriptor.MetaData.Manufacturer.append(pmtypes.LocalizedText(u'Draeger GmbH'))
                mdsDescriptor.MetaData.ModelName.append(pmtypes.LocalizedText(u'pySDC'))
                mdsDescriptor.MetaData.SerialNumber.append('pmDCBA-4321')
                mdsDescriptor.MetaData.ModelNumber = '1.09'

            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            cl_mdsDescriptor = clientMdib.descriptions.NODETYPE.get_one(namespaces.domTag('MdsDescriptor'))
            self.assertEqual(cl_mdsDescriptor.MetaData.ModelNumber, '1.09')
            self.assertEqual(cl_mdsDescriptor.MetaData.Manufacturer[-1].text, u'Draeger GmbH')

    def test_remove_add_mds(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            full_mdib = copy.deepcopy(sdcDevice.mdib.reconstruct_mdib_with_context_states())
            sdcDevice.stop_realtime_sample_loop()
            time.sleep(0.1)
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()
            dev_descriptor_count1 = len(sdcDevice.mdib.descriptions.objects)
            dev_state_count1 = len(sdcDevice.mdib.states.objects)
            dev_state_count1_handles = set([s.descriptorHandle for s in sdcDevice.mdib.states.objects])
            descr_handles = list(sdcDevice.mdib.descriptions.handle.keys())
            state_descriptorHandles = list(sdcDevice.mdib.states.descriptorHandle.keys())
            contextState_handles = list(sdcDevice.mdib.context_states.handle.keys())
            coll = observableproperties.SingleValueCollector(sdcClient, 'description_modification_report')
            with sdcDevice.mdib.transaction_manager() as mgr:
                mdsDescriptor = sdcDevice.mdib.descriptions.NODETYPE.get_one(namespaces.domTag('MdsDescriptor'))
                mgr.remove_descriptor(mdsDescriptor.handle)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            # verify that all state versions were saved
            descr_handles_lookup1 = copy.copy(sdcDevice.mdib.descriptions.handle_version_lookup)
            state_descriptorHandles_lookup1 = copy.copy(sdcDevice.mdib.states.handle_version_lookup)
            contextState_descriptorHandles_lookup1 = copy.copy(sdcDevice.mdib.context_states.handle_version_lookup)
            for h in descr_handles:
                self.assertTrue(h in descr_handles_lookup1)
            for h in state_descriptorHandles:
                self.assertTrue(h in state_descriptorHandles_lookup1)
            for h in contextState_handles:
                self.assertTrue(h in contextState_descriptorHandles_lookup1)

            # verify that client mdib has same number of objects as device mdib
            dev_descriptor_count2 = len(sdcDevice.mdib.descriptions.objects)
            dev_state_count2 = len(sdcDevice.mdib.states.objects)
            cl_descriptor_count2 = len(clientMdib.descriptions.objects)
            cl_state_count2 = len(clientMdib.states.objects)
            self.assertTrue(dev_descriptor_count2 < dev_descriptor_count1)
            self.assertEqual(dev_descriptor_count2, 0)
            self.assertEqual(dev_descriptor_count2, cl_descriptor_count2)
            self.assertEqual(dev_state_count2, cl_state_count2)

            # now add mds again:
            with sdcDevice.mdib.transaction_manager() as mgr:
                sdcDevice.mdib.add_mds_node(full_mdib)
            time.sleep(5)  # difficult to say which observable is updated as the last one, therefore sleep
            # verify that all objects have a state version at least incremented by one
            for handle, version in descr_handles_lookup1.items():
                obj = sdcDevice.mdib.descriptions.handle.get_one(handle)
                self.assertGreater(obj.DescriptorVersion, version)
            for handle, version in state_descriptorHandles_lookup1.items():
                obj = sdcDevice.mdib.states.descriptorHandle.get_one(handle, allow_none=True)
                if obj:
                    self.assertGreater(obj.StateVersion, version,
                                       msg='state {}: {} not greater than {}'.format(obj, obj.StateVersion, version))
            for handle, version in contextState_descriptorHandles_lookup1.items():
                obj = sdcDevice.mdib.context_states.handle.get_one(handle)
                print('checking object {} state={} expected={}'.format(obj, obj.StateVersion, version + 1))
                self.assertGreater(obj.StateVersion, version,
                                   msg='state {}: {} not greater than {}'.format(obj, obj.StateVersion, version + 1))

            dev_descriptor_count3 = len(sdcDevice.mdib.descriptions.objects)
            dev_state_count3 = len(sdcDevice.mdib.states.objects)
            cl_descriptor_count3 = len(clientMdib.descriptions.objects)
            cl_state_count3 = len(clientMdib.states.objects)
            self.assertEqual(dev_descriptor_count3, dev_descriptor_count1)
            self.assertEqual(dev_descriptor_count3, cl_descriptor_count3)
            if sdcDevice is self.sdc_device:
                self.assertEqual(dev_state_count3, dev_state_count1)
            else:
                self.assertEqual(dev_state_count3, dev_state_count1 - 1)  # scostate is not sent in draft6
            self.assertEqual(dev_state_count3, cl_state_count3)

    def test_clientmdib_observables(self):
        for sdcClient, sdcDevice in self._all_cl_dev:
            clientMdib = ClientMdibContainer(sdcClient)
            clientMdib.init_mdib()

            coll = observableproperties.SingleValueCollector(clientMdib,
                                                             'metrics_by_handle')  # wait for the next EpisodicMetricReport
            descriptorHandle = '0x34F00100'
            firstValue = 12
            with sdcDevice.mdib.transaction_manager(set_determination_time=False) as mgr:
                st = mgr.get_state(descriptorHandle)
                if st.MetricValue is None:
                    st.mk_metric_value()
                st.MetricValue.Value = firstValue
                st.MetricValue.Validity = 'Vld'
                st.MetricValue.DeterminationTime = time.time()
                st.PhysiologicalRange = [pmtypes.Range(1, 2, 3, 4, 5), pmtypes.Range(10, 20, 30, 40, 50)]
            data = coll.result(timeout=NOTIFICATION_TIMEOUT)
            self.assertTrue(descriptorHandle in data.keys())
            self.assertEqual(st.MetricValue.Value, data[descriptorHandle].MetricValue.Value)  # compare some data

            coll = observableproperties.SingleValueCollector(clientMdib,
                                                             'alert_by_handle')  # wait for the next EpisodicAlertReport
            descriptorHandle = '0xD3C00108'  # an AlertConditionDescriptorHandle
            with sdcDevice.mdib.transaction_manager(set_determination_time=False) as mgr:
                st = mgr.get_state(descriptorHandle)
                st.Presence = True
                st.Rank = 3
                st.DeterminationTime = time.time()
            data = coll.result(timeout=NOTIFICATION_TIMEOUT)
            self.assertTrue(descriptorHandle in data.keys())
            self.assertEqual(st.Rank, data[descriptorHandle].Rank)  # compare some data

            coll = observableproperties.SingleValueCollector(clientMdib, 'updated_descriptor_by_handle')
            descriptorHandle = '0x34F00100'
            with sdcDevice.mdib.transaction_manager(set_determination_time=False) as mgr:
                descr = mgr.get_descriptor(descriptorHandle)
                descr.DeterminationPeriod = 42
            data = coll.result(timeout=NOTIFICATION_TIMEOUT)
            self.assertTrue(descriptorHandle in data.keys())
            self.assertEqual(descr.DeterminationPeriod, data[descriptorHandle].DeterminationPeriod)  # compare some data

            coll = observableproperties.SingleValueCollector(clientMdib,
                                                             'waveform_by_handle')  # wait for the next WaveformReport
            # waveforms are already sent, no need to trigger anything
            data = coll.result(timeout=NOTIFICATION_TIMEOUT)
            self.assertGreater(len(data.keys()), 0)  # at least one real time sample array

    def test_isConnected_unfriendly(self):
        """ Test device stop without sending subscription end messages"""
        self.log_watcher.setPaused(True)
        time.sleep(1)
        for sdcClient, sdcDevice in self._all_cl_dev:
            self.assertEqual(sdcClient.is_connected, True)
        collectors = []
        for sdcClient, sdcDevice in self._all_cl_dev:
            coll = observableproperties.SingleValueCollector(sdcClient,
                                                             'is_connected')  # waiter for the next state transition
            collectors.append(coll)
            sdcDevice.stop_all(send_subscription_end=False)
        for coll in collectors:
            is_connected = coll.result(timeout=15)
            self.assertEqual(is_connected, False)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all(unsubscribe=False)  # without unsubscribe, is faster and would make no sense anyway

    def test_isConnected_friendly(self):
        """ Test device stop with sending subscription end messages"""
        self.log_watcher.setPaused(True)
        time.sleep(1)
        for sdcClient, sdcDevice in self._all_cl_dev:
            self.assertEqual(sdcClient.is_connected, True)
        collectors = []
        for sdcClient, sdcDevice in self._all_cl_dev:
            coll = observableproperties.SingleValueCollector(sdcClient,
                                                             'is_connected')  # waiter for the next state transition
            collectors.append(coll)
            sdcDevice.stop_all(send_subscription_end=True)
        for coll in collectors:
            is_connected = coll.result(timeout=15)
            self.assertEqual(is_connected, False)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all(unsubscribe=False)  # without unsubscribe, is faster and would make no sense anyway

    def test_invalid_request(self):
        """MDPWS R0012: If a HOSTED SERVICE receives a MESSAGE that is inconsistent with its WSDL description, the HOSTED
        SERVICE SHOULD generate a SOAP Fault with a Code Value of 'Sender', unless a 'MustUnderstand' or
        'VersionMismatch' Fault is generated
        """
        self.log_watcher.setPaused(True)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.get_service_client._validate = False  # want to send an invalid request
            try:
                method = 'Nonsense'
                envelope = sdcClient.get_service_client._msg_factory._mk_get_method_envelope(
                    sdcClient.get_service_client.endpoint_reference.address,
                    sdcClient.get_service_client.porttype,
                    method)
                sdcClient.get_service_client._call_get_method(envelope, 'Nonsense')
            except HTTPReturnCodeError as ex:
                self.assertEqual(ex.status, 400)
                fault_xml = ex.reason
                self.assertTrue(b'Fault' in fault_xml)
                rec = ReceivedSoapFault(fault_xml)
                self.assertTrue(rec.body_node[0].tag.endswith('Fault'))
                self.assertEqual(rec.code, 's12:Sender')

            else:
                self.assertTrue(False, 'HTTPReturnCodeError not raised')

    def test_extension(self):
        def are_equivalent(node1, node2):
            if node1.tag != node2.tag or node1.attrib != node2.attrib or node1.text != node2.text:
                return False
            for ch1, ch2 in zip(node1, node2):
                if not are_equivalent(ch1, ch2):
                    return False
            return True

        for sdcClient, sdcDevice in self._all_cl_dev:
            cl_mdib = ClientMdibContainer(sdcClient)
            cl_mdib.init_mdib()
            for cl_descriptor in cl_mdib.descriptions.objects:
                dev_descriptor = sdcDevice.mdib.descriptions.handle.get_one(cl_descriptor.Handle)
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
    # @classmethod
    # def setUpClass(cls):
    #     mklogger()

    def setUp(self):
        basic_logging_setup()

        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        self.sdcDevice_1 = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<dev1> ')

        # common http server for both devices, borrow ssl context from device
        self.httpserver = DeviceHttpServerThread(
            my_ipaddress='0.0.0.0', ssl_context=self.sdcDevice_1._ssl_context,
            supported_encodings=compression.CompressionHandler.available_encodings[:],
            log_prefix='hppt_srv')
        self.httpserver.start()
        self.httpserver.started_evt.wait(timeout=5)

        self.sdcDevice_1.start_all(shared_http_server=self.httpserver)
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdcDevice_1.set_location(location, self._locValidators)
        self.provideRealtimeData(self.sdcDevice_1)

        self.sdcDevice_2 = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<dev2> ')
        self.sdcDevice_2.start_all(shared_http_server=self.httpserver)
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdcDevice_2.set_location(location, self._locValidators)
        self.provideRealtimeData(self.sdcDevice_2)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdcDevice_1.get_xaddrs()
        self.sdcClient_1 = SdcClient(xAddr[0],
                                     sdc_definitions=self.sdcDevice_1.mdib.sdc_definitions,
                                     ssl_context=None,
                                     validate=CLIENT_VALIDATE,
                                     log_prefix='<cl1> ')
        self.sdcClient_1.start_all()

        xAddr = self.sdcDevice_2.get_xaddrs()
        self.sdcClient_2 = SdcClient(xAddr[0],
                                     sdc_definitions=self.sdcDevice_2.mdib.sdc_definitions,
                                     ssl_context=None,
                                     validate=CLIENT_VALIDATE,
                                     log_prefix='<cl2> ')
        self.sdcClient_2.start_all()

        self._all_cl_dev = ((self.sdcClient_1, self.sdcDevice_1),
                            (self.sdcClient_2, self.sdcDevice_2))

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all()
            sdcDevice.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    @staticmethod
    def provideRealtimeData(sdcDevice):
        paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

        flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=1.2, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

        co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveformperiod=1.0, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05506', co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

        # make SinusGenerator (0x34F05501) the annotator source
        annotation = pmtypes.Annotation(pmtypes.CodedValue('a', 'b'))  # what is CodedValue for startOfInspirationCycle?
        sdcDevice.mdib.register_annotation_generator(annotation,
                                                     trigger_handle='0x34F05501',
                                                     annotated_handles=('0x34F05500', '0x34F05501', '0x34F05506'))

    def test_BasicConnect(self):
        # simply check that correct top node is returned
        for sdcClient, _ in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            node = cl_getService.get_md_description_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdDescriptionResponse')))

            node = cl_getService.get_mdib_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdibResponse')))

            node = cl_getService.get_md_state_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdStateResponse')))

            contextService = sdcClient.client('Context')
            node = contextService.get_context_states_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetContextStatesResponse')))


class Test_Client_SomeDevice_chunked(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<Final> ',
                                                    chunked_messages=True)
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        nsmapper = self.sdc_device.mdib.nsmapper
        nsmapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._locValidators)
        self.provideRealtimeData(self.sdc_device)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE,
                                    log_prefix='<Final> ',
                                    chunked_requests=True)
        self.sdc_client.start_all()

        self._all_cl_dev = [(self.sdc_client, self.sdc_device)]

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all()
            sdcDevice.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    @staticmethod
    def provideRealtimeData(sdcDevice):
        paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

        flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=1.2, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

        co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveformperiod=1.0, sampleperiod=0.01)
        sdcDevice.mdib.register_waveform_generator('0x34F05506', co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

        # make SinusGenerator (0x34F05501) the annotator source
        annotation = pmtypes.Annotation(pmtypes.CodedValue('a', 'b'))  # what is CodedValue for startOfInspirationCycle?
        sdcDevice.mdib.register_annotation_generator(annotation,
                                                     trigger_handle='0x34F05501',
                                                     annotated_handles=('0x34F05500', '0x34F05501', '0x34F05506'))

    def test_BasicConnect(self):
        # simply check that correct top node is returned
        for sdcClient, _ in self._all_cl_dev:
            cl_getService = sdcClient.client('Get')
            node = cl_getService.get_md_description_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdDescriptionResponse')))

            node = cl_getService.get_mdib_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdibResponse')))

            node = cl_getService.get_md_state_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetMdStateResponse')))

            contextService = sdcClient.client('Context')
            node = contextService.get_context_states_node()
            self.assertEqual(node.tag, str(namespaces.msgTag('GetContextStatesResponse')))

        for _, sdcDevice in self._all_cl_dev:
            sdcDevice.stop_all()


class TestClientSomeDeviceReferenceParametersDispatch(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        specific_components=SdcDeviceComponents(subscriptions_manager_class=SubscriptionsManagerReferenceParam)
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml', log_prefix='<Final> ',
                                                    specific_components=specific_components,
                                                    chunked_messages=True)
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        nsmapper = self.sdc_device.mdib.nsmapper
        nsmapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._locValidators)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE,
                                    log_prefix='<Final> ',
                                    chunked_requests=True)
        self.sdc_client.start_all()

        self._all_cl_dev = [(self.sdc_client, self.sdc_device)]

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        for sdcClient, sdcDevice in self._all_cl_dev:
            sdcClient.stop_all()
            sdcDevice.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_BasicConnect(self):
        # simply check that correct top node is returned
        cl_getService = self.sdc_client.client('Get')
        node = cl_getService.get_md_description_node()
        self.assertEqual(node.tag, str(namespaces.msgTag('GetMdDescriptionResponse')))

        node = cl_getService.get_mdib_node()
        self.assertEqual(node.tag, str(namespaces.msgTag('GetMdibResponse')))

        node = cl_getService.get_md_state_node()
        self.assertEqual(node.tag, str(namespaces.msgTag('GetMdStateResponse')))

        contextService = self.sdc_client.client('Context')
        node = contextService.get_context_states_node()
        self.assertEqual(node.tag, str(namespaces.msgTag('GetContextStatesResponse')))

    def test_renew_getStatus(self):
        """ If renew and get_status work, then reference parameters based dispatching works. """
        for s in self.sdc_client._subscription_mgr.subscriptions.values():
            remaining_seconds = s.renew(1)  # one minute
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins
            remaining_seconds = s.get_status()
            self.assertAlmostEqual(remaining_seconds, 60, delta=5.0)  # huge diff allowed due to jenkins

