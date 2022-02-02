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


class TestDeviceSubscriptions(unittest.TestCase):
    
    def setUp(self):

        ''' validate test data'''
        here = os.path.dirname(__file__)
        self.mdib = sdc11073.mdib.DeviceMdibContainer.fromMdibFile(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        
        self._model = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='Chinakracher GmbH',
                                                                 manufacturerUrl='www.chinakracher.com',
                                                                 modelName='BummHuba',
                                                                 modelNumber='1.0',
                                                                 modelUrl='www.chinakracher.com/bummhuba/model',
                                                                 presentationUrl='www.chinakracher.com/bummhuba/presentation')
        self._device = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendlyName='Big Bang Practice',
                                                                   firmwareVersion='0.99',
                                                                   serialNumber='87kabuuum889')

        self.wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsDiscovery.start()
        my_uuid = None # let device create one
        mdib_d10 = sdc11073.mdib.DeviceMdibContainer.fromMdibFile(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        self.sdcDevice_d10 = sdc11073.sdcdevice.SdcDevice(self.wsDiscovery, my_uuid, self._model, self._device, mdib_d10, logLevel=logging.DEBUG)
        self.sdcDevice_d10.startAll(periodic_reports_interval=1.0)
        self._allDevices = (self.sdcDevice_d10,)


    def tearDown(self):
        self.wsDiscovery.stop()
        for d in self._allDevices:
            if d:
                d.stopAll()

    def _verify_proper_namespaces(self, report):
        """We want some namespaces declared only once for small report sizes."""
        import re
        xml_string = report.as_xml().decode('utf-8')
        for ns in (namespaces.Prefix_Namespace.PM.namespace,
                   namespaces.Prefix_Namespace.MSG.namespace,
                   namespaces.Prefix_Namespace.EXT.namespace,
                   namespaces.Prefix_Namespace.XSI.namespace,):
            occurances = [i.start() for i in re.finditer(ns, xml_string)]
            self.assertLessEqual(len(occurances), 1)

    def test_waveformSubscription(self):
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.Waveform)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testSubscr)
            
            tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
            st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
            si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.01)
            
            sdcDevice.mdib.registerWaveformGenerator(HANDLES[0], tr)
            sdcDevice.mdib.registerWaveformGenerator(HANDLES[1], st)
            sdcDevice.mdib.registerWaveformGenerator(HANDLES[2], si)
    
            time.sleep(3)
            self.assertGreater(len(testSubscr.reports), 20)
            report = testSubscr.reports[-1] # a 
            self._verify_proper_namespaces(report)
            in_report = ReceivedSoap12Envelope.fromXMLString(report.as_xml())
            expected_action = sdcDevice.mdib.sdc_definitions.Actions.Waveform
            self.assertEqual(in_report.address.action, expected_action)


    def test_episodicMetricReportSubscription(self):
        ''' verify that a subscription response is valid'''
        notifyTo = 'http://localhost:123'
        endTo = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpointReferencesList=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            typesList=['Get'],
            serviceId=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription.ClSubscription(dpwsHosted=hosted,
                                                                       actions=[sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport],
                                                                       notification_url=notifyTo,
                                                                       endTo_url=endTo,
                                                                       ident='',
                                                                       xml_validator=None)
            subscrRequest = clSubscr._mkSubscribeEnvelope(subscribe_epr='http://otherdevice:123/bla', expire_minutes=59)
            subscrRequest.validate_envelope(sdcDevice._handler.xml_validator)
            self._verify_proper_namespaces(subscrRequest)

            httpHeader = {}
            # avoid instantiation of new soap client by pretenting there is one already
            sdcDevice.subscriptionsManager.soapClients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptionsManager.onSubscribeRequest(httpHeader,
                                                                          ReceivedSoap12Envelope.fromXMLString(subscrRequest.as_xml()),
                                                                          'http://abc.com:123/def')
            self._verify_proper_namespaces(response)
            response.validate_envelope(sdcDevice._handler.xml_validator)
            clSubscr._handleSubscribeResponse(ReceivedSoap12Envelope.fromXMLString(response.as_xml()))
            
            # verify that devices subscription contains the subscription identifier of the client Subscription object
            devSubscr = list(sdcDevice.subscriptionsManager._subscriptions.objects)[0]
            self.assertEqual(devSubscr.notifyToAddress, notifyTo)
            self.assertEqual(devSubscr.notifyRefNodes[0].text, clSubscr.notifyTo_identifier.text)
            self.assertEqual(devSubscr.endToAddress, endTo)
            self.assertEqual(devSubscr.endToRefNodes[0].text, clSubscr.end_to_identifier.text)
            
            # verify that client subscription object contains the subscription identifier of the device Subscription object
            self.assertEqual(clSubscr.dev_reference_param[0].tag, devSubscr.my_identifier.tag )
            self.assertEqual(clSubscr.dev_reference_param[0].text, devSubscr.my_identifier.text )
    
            # check renew
            renewRequest = clSubscr._mkRenewEnvelope(expire_minutes=59)
            renewRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print (renewRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptionsManager.onRenewRequest(ReceivedSoap12Envelope.fromXMLString(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            response.validate_envelope(sdcDevice._handler.xml_validator)

            # check getstatus
            getStatusRequest = clSubscr._mkGetStatusEnvelope()
            getStatusRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print (getStatusRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptionsManager.onGetStatusRequest(ReceivedSoap12Envelope.fromXMLString(getStatusRequest.as_xml()))
            print (response.as_xml(pretty=True))
            response.validate_envelope(sdcDevice._handler.xml_validator)


    def test_episodicMetricReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testSubscr)
            
            descriptorHandle = '0x34F00100'#'0x34F04380'
            firstValue = 12
            with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                st = mgr.getMetricState(descriptorHandle)
                if st.metricValue is None:
                    st.mkMetricValue()
                st.metricValue.Value = firstValue
                st.Validity = 'Vld'
            self.assertEqual(len(testSubscr.reports), 1)
            response = testSubscr.reports[0]
            self._verify_proper_namespaces(response)
            print (response.as_xml(pretty=True))
            response.validate_envelope(sdcDevice._handler.xml_validator)

            # verify that header contains the identifier of client subscription
            env  = ReceivedSoap12Envelope.fromXMLString(response.as_xml())
            idents = env.headerNode.findall(namespaces.wseTag('Identifier'))
            self.assertEqual(len(idents), 1)
            self.assertEqual(idents[0].text, mockstuff.TestDevSubscription.notifyRef)


    def test_episodicContextReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.EpisodicContextReport)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testSubscr)
            patientContextDescriptor = sdcDevice.mdib.descriptions.NODETYPE.getOne(namespaces.domTag('PatientContextDescriptor'))
            descriptorHandle = patientContextDescriptor.handle
            with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                st = mgr.getContextState(descriptorHandle)
                st.PatientType = pmtypes.PatientType.ADULT
            self.assertEqual(len(testSubscr.reports), 1)
            response = testSubscr.reports[0]
            self._verify_proper_namespaces(response)
            response.validate_envelope(sdcDevice._handler.xml_validator)


    def test_notifyOperation(self):
        for sdcDevice in self._allDevices:
            testSubscr = mockstuff.TestDevSubscription(sdcDevice.mdib.sdc_definitions.Actions.OperationInvokedReport)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testSubscr)
            sdcDevice.subscriptionsManager.notifyOperation('urn:uuid:abc', 1234,
                                                            transactionId=123, 
                                                            operationHandleRef='something', 
                                                            operationState='Fin', 
                                                            error='Unspec', 
                                                            errorMessage='')
            self.assertEqual(len(testSubscr.reports), 1)


    def test_invalid_GetStatus_Renew(self):
        ''' verify that a subscription response is 'Fault' response in case of invalid request'''
        notifyTo = 'http://localhost:123'
        endTo = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpointReferencesList=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            typesList=['Get'],
            serviceId=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription.ClSubscription(dpwsHosted=hosted,
                                                                      actions=[sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport],
                                                                      notification_url=notifyTo,
                                                                      endTo_url=endTo,
                                                                      ident='',
                                                                      xml_validator=None)
            subscrRequest = clSubscr._mkSubscribeEnvelope(subscribe_epr='http://otherdevice/bla:123', expire_minutes=59)
            subscrRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print (subscrRequest.as_xml(pretty=True))

            httpHeader = {}
            # avoid instantiation of new soap client by pretending there is one already
            sdcDevice.subscriptionsManager.soapClients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptionsManager.onSubscribeRequest(httpHeader,
                                                                          ReceivedSoap12Envelope.fromXMLString(subscrRequest.as_xml()),
                                                                          'http://abc.com:123/def')
            response.validate_envelope(sdcDevice._handler.xml_validator)
            clSubscr._handleSubscribeResponse(ReceivedSoap12Envelope.fromXMLString(response.as_xml()))
    
            # check renew
            clSubscr.dev_reference_param[0].text = 'bla'# make ident invalid
            renewRequest = clSubscr._mkRenewEnvelope(expire_minutes=59)
            renewRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print (renewRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptionsManager.onRenewRequest(ReceivedSoap12Envelope.fromXMLString(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            self.assertEqual(response.bodyNode[0].tag, namespaces.s12Tag('Fault'))
            response.validate_envelope(sdcDevice._handler.xml_validator)

            getStatusRequest = clSubscr._mkGetStatusEnvelope()
            getStatusRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print (getStatusRequest.as_xml(pretty=True))
    
            response = sdcDevice.subscriptionsManager.onRenewRequest(ReceivedSoap12Envelope.fromXMLString(renewRequest.as_xml()))
            print (response.as_xml(pretty=True))
            self.assertEqual(response.bodyNode[0].tag, namespaces.s12Tag('Fault'))
            response.validate_envelope(sdcDevice._handler.xml_validator)

    def test_periodicMetricReportSubscription(self):
        ''' verify that a subscription response is valid'''
        notifyTo = 'http://localhost:123'
        endTo = 'http://localhost:124'
        hosted = sdc11073.pysoap.soapenvelope.DPWSHosted(
            endpointReferencesList=[sdc11073.pysoap.soapenvelope.WsaEndpointReferenceType('http://1.2.3.4:6000')],
            typesList=['Get'],
            serviceId=123)
        for sdcDevice in self._allDevices:
            clSubscr = sdc11073.sdcclient.subscription.ClSubscription(dpwsHosted=hosted,
                                                                      actions=[
                                                                           sdcDevice.mdib.sdc_definitions.Actions.PeriodicMetricReport],
                                                                      notification_url=notifyTo,
                                                                      endTo_url=endTo,
                                                                      ident='',
                                                                      xml_validator=None)
            subscrRequest = clSubscr._mkSubscribeEnvelope(subscribe_epr='http://otherdevice:123/bla', expire_minutes=59)
            subscrRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print(subscrRequest.as_xml(pretty=True))

            httpHeader = {}
            # avoid instantiation of new soap client by pretenting there is one already
            sdcDevice.subscriptionsManager.soapClients['localhost:123'] = 'dummy'
            response = sdcDevice.subscriptionsManager.onSubscribeRequest(httpHeader,
                                                                         ReceivedSoap12Envelope.fromXMLString(
                                                                             subscrRequest.as_xml()),
                                                                         'http://abc.com:123/def')
            self._verify_proper_namespaces(response)
            response.validate_envelope(sdcDevice._handler.xml_validator)
            clSubscr._handleSubscribeResponse(ReceivedSoap12Envelope.fromXMLString(response.as_xml()))

            # verify that devices subscription contains the subscription identifier of the client Subscription object
            devSubscr = list(sdcDevice.subscriptionsManager._subscriptions.objects)[0]
            self.assertEqual(devSubscr.notifyToAddress, notifyTo)
            self.assertEqual(devSubscr.notifyRefNodes[0].text, clSubscr.notifyTo_identifier.text)
            self.assertEqual(devSubscr.endToAddress, endTo)
            self.assertEqual(devSubscr.endToRefNodes[0].text, clSubscr.end_to_identifier.text)

            # verify that client subscription object contains the subscription identifier of the device Subscription object
            self.assertEqual(clSubscr.dev_reference_param[0].tag, devSubscr.my_identifier.tag)
            self.assertEqual(clSubscr.dev_reference_param[0].text, devSubscr.my_identifier.text)

            # check renew
            renewRequest = clSubscr._mkRenewEnvelope(expire_minutes=59)
            renewRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print(renewRequest.as_xml(pretty=True))

            response = sdcDevice.subscriptionsManager.onRenewRequest(
                ReceivedSoap12Envelope.fromXMLString(renewRequest.as_xml()))
            print(response.as_xml(pretty=True))
            response.validate_envelope(sdcDevice._handler.xml_validator)

            # check getstatus
            getStatusRequest = clSubscr._mkGetStatusEnvelope()
            getStatusRequest.validate_envelope(sdcDevice._handler.xml_validator)
            print(getStatusRequest.as_xml(pretty=True))

            response = sdcDevice.subscriptionsManager.onGetStatusRequest(
                ReceivedSoap12Envelope.fromXMLString(getStatusRequest.as_xml()))
            print(response.as_xml(pretty=True))
            response.validate_envelope(sdcDevice._handler.xml_validator)

    def test_periodicMetricReportEvent(self):
        ''' verify that an event message is sent to subscriber and that message is valid'''
        # directly inject a subscription event, this test is not about starting subscriptions
        logging.getLogger('sdc.device').setLevel(logging.DEBUG)
        for sdcDevice in self._allDevices:
            testEpisodicSubscr = mockstuff.TestDevSubscription(
                sdcDevice.mdib.sdc_definitions.Actions.EpisodicMetricReport)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testEpisodicSubscr)

            testPeriodicSubscr = mockstuff.TestDevSubscription(
                sdcDevice.mdib.sdc_definitions.Actions.PeriodicMetricReport)
            sdcDevice.subscriptionsManager._subscriptions.addObject(testPeriodicSubscr)

            descriptorHandle = '0x34F00100'  # '0x34F04380'
            firstValue = 12
            with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                st = mgr.getMetricState(descriptorHandle)
                if st.metricValue is None:
                    st.mkMetricValue()
                st.metricValue.Value = firstValue
                st.metricValue.Validity = 'Vld'
            with sdcDevice.mdib.mdibUpdateTransaction() as mgr:
                st = mgr.getMetricState(descriptorHandle)
                if st.metricValue is None:
                    st.mkMetricValue()
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
            response.validate_envelope(sdcDevice._handler.xml_validator)

            # verify that header contains the identifier of client subscription
            env = ReceivedSoap12Envelope.fromXMLString(response.as_xml())
            idents = env.headerNode.findall(namespaces.wseTag('Identifier'))
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
