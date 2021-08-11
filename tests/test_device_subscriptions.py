import unittest
import os
import time
import logging
import logging.handlers
from tests import mockstuff
from sdc11073 import observableproperties
import sdc11073
from sdc11073.sdcdevice import waveforms
from sdc11073 import namespaces
from sdc11073 import pmtypes
from sdc11073.pysoap.msgfactory import SoapMessageFactory

mdibFolder = os.path.dirname(__file__)

ReceivedSoap12Envelope = sdc11073.pysoap.soapenvelope.ReceivedSoap12Envelope
Soap12Envelope = sdc11073.pysoap.soapenvelope.Soap12Envelope

#pylint: disable=protected-access

CLIENT_VALIDATE = True

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}


class DummySoapClient(object):
    roundtrip_time = observableproperties.ObservableProperty()
    def __init__(self):
        self.sentReports = []
        self.netloc = None
    
    def post_soap_envelope(self, soapEnvelopeRequest, response_factory=None, schema=None): #pylint: disable=unused-argument
        self.sentReports.append(soapEnvelopeRequest)
        self.roundtrip_time = 0.001 # dummy
        
    def post_soap_envelope_to(self, path, soapEnvelopeRequest, response_factory=None, schema=None, msg=''): #pylint: disable=unused-argument
        self.sentReports.append(soapEnvelopeRequest)
        self.roundtrip_time = 0.001 # dummy

        
class TestDeviceSubscriptions(unittest.TestCase):
    
    def setUp(self):

        ''' validate test data'''
        here = os.path.dirname(__file__)
        self.mdib = sdc11073.mdib.DeviceMdibContainer.from_mdib_file(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        
        self._model = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='Chinakracher GmbH',
                                                                 manufacturer_url='www.chinakracher.com',
                                                                 model_name='BummHuba',
                                                                 model_number='1.0',
                                                                 model_url='www.chinakracher.com/bummhuba/model',
                                                                 presentation_url='www.chinakracher.com/bummhuba/presentation')
        self._device = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendly_name='Big Bang Practice',
                                                                   firmware_version='0.99',
                                                                   serial_number='87kabuuum889')

        self.wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsDiscovery.start()
        my_uuid = None # let device create one
        mdib_d10 = sdc11073.mdib.DeviceMdibContainer.from_mdib_file(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        self.sdcDevice_d10 = sdc11073.sdcdevice.SdcDevice(self.wsDiscovery, my_uuid, self._model, self._device, mdib_d10)
        self.sdcDevice_d10.start_all(periodic_reports_interval=1.0)
        self._allDevices = (self.sdcDevice_d10,)


    def tearDown(self):
        self.wsDiscovery.stop()
        for d in self._allDevices:
            if d:
                d.stop_all()

    def _verify_proper_namespaces(self, report):
        """We want some namespaces declared only once for small report sizes."""
        import re
        xml_string = report.as_xml().decode('utf-8')
        for ns in (namespaces.Prefixes.PM.namespace,
                   namespaces.Prefixes.MSG.namespace,
                   namespaces.Prefixes.EXT.namespace,
                   namespaces.Prefixes.XSI.namespace,):
            occurances = [i.start() for i in re.finditer(ns, xml_string)]
            self.assertLessEqual(len(occurances), 1)

    def test_waveformSubscription(self):
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.Waveform, sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testSubscr)
            
            tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
            st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
            si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.01)
            
            sdcDevice.mdib.register_waveform_generator(HANDLES[0], tr)
            sdcDevice.mdib.register_waveform_generator(HANDLES[1], st)
            sdcDevice.mdib.register_waveform_generator(HANDLES[2], si)
    
            time.sleep(3)
            self.assertGreater(len(testSubscr.reports), 20)
            report = testSubscr.reports[-1] # a 
            self._verify_proper_namespaces(report)
            in_report = ReceivedSoap12Envelope.from_xml_string(report.as_xml())
            expected_action = sdcDevice.mdib.sdc_definitions.Actions.Waveform
            self.assertEqual(in_report.address.action, expected_action)


    def test_episodicMetricReportSubscription(self):
        ''' verify that a subscription response is valid'''
        notify_to = 'http://localhost:123'
        end_to = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpoint_references_list=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            types_list=['Get'],
            service_id=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription._ClSubscription(SoapMessageFactory(None, None),
                                                                       dpws_hosted=hosted,
                                                                       actions=[sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport],
                                                                       notification_url=notify_to,
                                                                       end_to_url=end_to,
                                                                       ident='')
            subscrRequest = clSubscr._mk_subscribe_envelope(subscribe_epr='http://otherdevice:123/bla', expire_minutes=59)
            subscrRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            self._verify_proper_namespaces(subscrRequest)

            http_header = {}
            # avoid instantiation of new soap client by pretenting there is one already
            sdcDevice.subscriptions_manager.soap_clients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptions_manager.on_subscribe_request(http_header,
                                                                          ReceivedSoap12Envelope.from_xml_string(subscrRequest.as_xml()),
                                                                          'http://abc.com:123/def')
            self._verify_proper_namespaces(response)
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            clSubscr._handle_subscribe_response(ReceivedSoap12Envelope.from_xml_string(response.as_xml()))
            
            # verify that devices subscription contains the subscription identifier of the client Subscription object
            devSubscr = list(sdcDevice.subscriptions_manager._subscriptions.objects)[0]
            self.assertEqual(devSubscr.notify_to_address, notify_to)
            self.assertEqual(devSubscr.notify_ref_nodes[0].text, clSubscr.notify_to_identifier.text)
            self.assertEqual(devSubscr.end_to_address, end_to)
            self.assertEqual(devSubscr.end_to_ref_nodes[0].text, clSubscr.end_to_identifier.text)
            
            # verify that client subscription object contains the subscription identifier of the device Subscription object
            self.assertEqual(clSubscr.dev_reference_param[0].tag, devSubscr.my_identifier.tag )
            self.assertEqual(clSubscr.dev_reference_param[0].text, devSubscr.my_identifier.text )
    
            # check renew
            renewRequest = clSubscr._mk_renew_envelope(expire_minutes=59)
            renewRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print (renewRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptions_manager.on_renew_request(ReceivedSoap12Envelope.from_xml_string(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
    
            # check getstatus
            getStatusRequest = clSubscr._mk_get_status_envelope()
            getStatusRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print (getStatusRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptions_manager.on_get_status_request(ReceivedSoap12Envelope.from_xml_string(getStatusRequest.as_xml()))
            print (response.as_xml(pretty=True))
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)


    def test_episodicMetricReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport, sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testSubscr)
            
            descriptorHandle = '0x34F00100'#'0x34F04380'
            firstValue = 12
            with sdcDevice.mdib.transaction_manager() as mgr:
                #st = mgr.getMetricState(descriptorHandle)
                st = mgr.get_state(descriptorHandle)
                if st.metricValue is None:
                    st.mk_metric_value()
                st.metricValue.Value = firstValue
                st.Validity = 'Vld'
            self.assertEqual(len(testSubscr.reports), 1)
            response = testSubscr.reports[0]
            self._verify_proper_namespaces(response)
            print (response.as_xml(pretty=True))
            response.validate_body(sdcDevice.mdib.biceps_schema.message_schema)
            
            # verify that header contains the identifier of client subscription
            env  = ReceivedSoap12Envelope.from_xml_string(response.as_xml())
            idents = env.header_node.findall(namespaces.wseTag('Identifier'))
            self.assertEqual(len(idents), 1)
            self.assertEqual(idents[0].text, mockstuff.TestDevSubscription.notifyRef)


    def test_episodicContextReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.EpisodicContextReport, sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testSubscr)
            patientContextDescriptor = sdcDevice.mdib.descriptions.NODETYPE.get_one(namespaces.domTag('PatientContextDescriptor'))
            descriptorHandle = patientContextDescriptor.handle
            with sdcDevice.mdib.transaction_manager() as mgr:
                st = mgr.get_state(descriptorHandle)
                st.CoreData.PatientType = pmtypes.PatientType.ADULT
            self.assertEqual(len(testSubscr.reports), 1)
            response = testSubscr.reports[0]
            self._verify_proper_namespaces(response)
            response.validate_body(sdcDevice.mdib.biceps_schema.message_schema)


    def test_notifyOperation(self):
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.OperationInvokedReport, sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testSubscr)
            class DummyOperation:
                pass
            dummy_operation = DummyOperation()
            dummy_operation.handle = 'something'
            sdcDevice.subscriptions_manager.notify_operation(dummy_operation,
                                                             123,
                                                             pmtypes.InvocationState.FINISHED,
                                                             sdcDevice.mdib.nsmapper,
                                                             sequence_id='urn:uuid:abc',
                                                             mdib_version=1234,
                                                             error=pmtypes.InvocationError.UNSPECIFIED,
                                                             error_message='')
            self.assertEqual(len(testSubscr.reports), 1)


    def test_invalid_GetStatus_Renew(self):
        ''' verify that a subscription response is 'Fault' response in case of invalid request'''
        notify_to = 'http://localhost:123'
        end_to = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpoint_references_list=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            types_list=['Get'],
            service_id=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription._ClSubscription(SoapMessageFactory(None, None),
                                                                       dpws_hosted=hosted,
                                                                       actions=[sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport],
                                                                       notification_url=notify_to,
                                                                       end_to_url=end_to,
                                                                       ident='')
            subscrRequest = clSubscr._mk_subscribe_envelope(subscribe_epr='http://otherdevice/bla:123', expire_minutes=59)
            subscrRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print (subscrRequest.as_xml(pretty=True))

            http_header = {}
            # avoid instantiation of new soap client by pretending there is one already
            sdcDevice.subscriptions_manager.soap_clients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptions_manager.on_subscribe_request(http_header,
                                                                          ReceivedSoap12Envelope.from_xml_string(subscrRequest.as_xml()),
                                                                          'http://abc.com:123/def')
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            clSubscr._handle_subscribe_response(ReceivedSoap12Envelope.from_xml_string(response.as_xml()))
    
            # check renew
            clSubscr.dev_reference_param[0].text = 'bla'# make ident invalid
            renewRequest = clSubscr._mk_renew_envelope(expire_minutes=59)
            renewRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print (renewRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptions_manager.on_renew_request(ReceivedSoap12Envelope.from_xml_string(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            self.assertEqual(response.body_node[0].tag, namespaces.s12Tag('Fault'))
            response.validate_body(sdcDevice.mdib.biceps_schema.soap12_schema)
    
            getStatusRequest = clSubscr._mk_get_status_envelope()
            getStatusRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print (getStatusRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptions_manager.on_renew_request(ReceivedSoap12Envelope.from_xml_string(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            self.assertEqual(response.body_node[0].tag, namespaces.s12Tag('Fault'))
            response.validate_body(sdcDevice.mdib.biceps_schema.soap12_schema)

    def test_periodicMetricReportSubscription(self):
        ''' verify that a subscription response is valid'''
        notify_to = 'http://localhost:123'
        end_to = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpoint_references_list=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            types_list=['Get'],
            service_id=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription._ClSubscription(SoapMessageFactory(None, None),
                                                                       dpws_hosted=hosted,
                                                                       actions=[
                                                                           sdcDevice.mdib.sdc_definitions.Actions.PeriodicMetricReport],
                                                                       notification_url=notify_to,
                                                                       end_to_url=end_to,
                                                                       ident='')
            subscrRequest = clSubscr._mk_subscribe_envelope(subscribe_epr='http://otherdevice:123/bla', expire_minutes=59)
            subscrRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print(subscrRequest.as_xml(pretty=True))

            http_header = {}
            # avoid instantiation of new soap client by pretenting there is one already
            sdcDevice.subscriptions_manager.soap_clients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptions_manager.on_subscribe_request(http_header,
                                                                         ReceivedSoap12Envelope.from_xml_string(
                                                                             subscrRequest.as_xml()),
                                                                         'http://abc.com:123/def')
            self._verify_proper_namespaces(response)
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            clSubscr._handle_subscribe_response(ReceivedSoap12Envelope.from_xml_string(response.as_xml()))

            # verify that devices subscription contains the subscription identifier of the client Subscription object
            devSubscr = list(sdcDevice.subscriptions_manager._subscriptions.objects)[0]
            self.assertEqual(devSubscr.notify_to_address, notify_to)
            self.assertEqual(devSubscr.notify_ref_nodes[0].text, clSubscr.notify_to_identifier.text)
            self.assertEqual(devSubscr.end_to_address, end_to)
            self.assertEqual(devSubscr.end_to_ref_nodes[0].text, clSubscr.end_to_identifier.text)

            # verify that client subscription object contains the subscription identifier of the device Subscription object
            self.assertEqual(clSubscr.dev_reference_param[0].tag, devSubscr.my_identifier.tag)
            self.assertEqual(clSubscr.dev_reference_param[0].text, devSubscr.my_identifier.text)

            # check renew
            renewRequest = clSubscr._mk_renew_envelope(expire_minutes=59)
            renewRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print(renewRequest.as_xml(pretty=True))

            response = sdcDevice.subscriptions_manager.on_renew_request(
                ReceivedSoap12Envelope.from_xml_string(renewRequest.as_xml()))
            print(response.as_xml(pretty=True))
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)

            # check getstatus
            getStatusRequest = clSubscr._mk_get_status_envelope()
            getStatusRequest.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)
            print(getStatusRequest.as_xml(pretty=True))

            response = sdcDevice.subscriptions_manager.on_get_status_request(
                ReceivedSoap12Envelope.from_xml_string(getStatusRequest.as_xml()))
            print(response.as_xml(pretty=True))
            response.validate_body(sdcDevice.mdib.biceps_schema.eventing_schema)

    def test_periodicMetricReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        logging.getLogger('sdc.device').setLevel(logging.DEBUG)
        for sdcDevice in self._allDevices:
            testEpisodicSubscr = mockstuff.TestDevSubscription(
                sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport,
                sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testEpisodicSubscr)

            testPeriodicSubscr = mockstuff.TestDevSubscription(
                sdcDevice.mdib.sdc_definitions.Actions.PeriodicMetricReport,
                sdcDevice.mdib.biceps_schema)
            sdcDevice.subscriptions_manager._subscriptions.add_object(testPeriodicSubscr)

            descriptorHandle = '0x34F00100'  # '0x34F04380'
            firstValue = 12
            with sdcDevice.mdib.transaction_manager() as mgr:
                # st = mgr.getMetricState(descriptorHandle)
                st = mgr.get_state(descriptorHandle)
                if st.metricValue is None:
                    st.mk_metric_value()
                st.metricValue.Value = firstValue
                st.metricValue.Validity = 'Vld'
            with sdcDevice.mdib.transaction_manager() as mgr:
                # st = mgr.getMetricState(descriptorHandle)
                st = mgr.get_state(descriptorHandle)
                if st.metricValue is None:
                    st.mk_metric_value()
                st.metricValue.Value = firstValue + 1
                st.metricValue.Validity = 'Qst'

            time.sleep(2)
            self.assertEqual(len(testEpisodicSubscr.reports), 2)
            for response in testEpisodicSubscr.reports:
                self._verify_proper_namespaces(response)
                print(response.as_xml(pretty=True).decode('UTF-8'))

            self.assertEqual(len(testPeriodicSubscr.reports), 1)
            response = testPeriodicSubscr.reports[0]
            print(response.as_xml(pretty=True).decode('UTF-8'))
            response.validate_body(sdcDevice.mdib.biceps_schema.message_schema)

            # verify that header contains the identifier of client subscription
            env = ReceivedSoap12Envelope.from_xml_string(response.as_xml())
            idents = env.header_node.findall(namespaces.wseTag('Identifier'))
            self.assertEqual(len(idents), 1)
            self.assertEqual(idents[0].text, mockstuff.TestDevSubscription.notifyRef)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestDeviceSubscriptions)



if __name__ == '__main__':
    def mklogger(logFolder):
        applog = logging.getLogger('sdc')
        applog.setLevel(logging.DEBUG)
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        applog.addHandler(ch)
        ch2 = logging.handlers.RotatingFileHandler(os.path.join(logFolder,'sdcdevice.log'),
                                                   maxBytes=100000000,
                                                   backupCount=100)
        ch2.setLevel(logging.DEBUG)
        ch2.setFormatter(formatter)
        # add ch to logger
        applog.addHandler(ch2)
        
        # reduce log level for some loggers
        tmp = logging.getLogger('sdc.discover')
        tmp.setLevel(logging.WARN)
        tmp = logging.getLogger('sdc.client.subscr')
        tmp.setLevel(logging.INFO)
        tmp = logging.getLogger('sdc.client.mdib')
        tmp.setLevel(logging.INFO)
        tmp = logging.getLogger('sdc.client.wf')
        tmp.setLevel(logging.INFO)
        tmp = logging.getLogger('sdc.client.Set')
        tmp.setLevel(logging.INFO)
        tmp = logging.getLogger('sdc.device')
        tmp.setLevel(logging.INFO)
        tmp = logging.getLogger('sdc.device.subscrMgr')
        tmp.setLevel(logging.DEBUG)
        return applog

    mklogger('c:/tmp')
    
#    unittest.TextTestRunner(verbosity=2).run(suite())
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_subscriptions.TestDeviceSubscriptions.test_s31_Subscribe'))
#     unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_subscriptions.TestDeviceSubscriptions.test_waveformSubscription'))
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_subscriptions.TestDeviceSubscriptions.test_episodicContextReportEvent'))
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_subscriptions.TestDeviceSubscriptions.test_episodicMetricReportSubscription'))
    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_subscriptions.TestDeviceSubscriptions.test_invalid_GetStatus_Renew'))
