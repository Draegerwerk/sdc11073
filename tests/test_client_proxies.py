import unittest
import sdc11073
import logging
from sdc11073.pysoap.soapenvelope import DPWSHosted, WsaEndpointReferenceType
from sdc11073 import definitions_sdc

#pylint: disable=protected-access
DEV_ADDRESS = '169.254.0.200:10000'
CLIENT_VALIDATE = True

class TestClientProxies(unittest.TestCase):
    
    def setUp(self):
        self.sdcClient_final =  sdc11073.sdcclient.SdcClient(DEV_ADDRESS,
                                                             deviceType=definitions_sdc.SDC_v1_Definitions.MedicalDeviceType,
                                                             validate=CLIENT_VALIDATE,
                                                             my_ipaddress='169.254.0.3',
                                                             logLevel=logging.DEBUG)
        self._allclients = (self.sdcClient_final,)
        self.hosted = DPWSHosted(endpointReferencesList=[WsaEndpointReferenceType('http://1.2.3.4:6000')],
                                                        typesList=['xyz'], 
                                                        serviceId='abc')


    def test_Get_GetMdib(self):
        for sdcClient in self._allclients:
            getServiceClient = sdcClient._mkHostedServiceClient(porttype='Get', soapClient=None, hosted=self.hosted)
            soapEnvelope = getServiceClient._mkGetMethodEnvelope(method='GetMdib')
            print (soapEnvelope.as_xml(pretty=True))
            
            soapEnvelope.validateBody(sdcClient._bicepsSchema.bmmSchema)
            
        
    def test_Set_setNumericValue(self):
        for sdcClient in self._allclients:
            setServiceClient = sdcClient._mkHostedServiceClient(porttype='Set', soapClient=None, hosted=self.hosted)
            soapEnvelope = setServiceClient._mkRequestedNumericValueEnvelope(operationHandle='123', requestedNumericValue=42.42)
            print (soapEnvelope.as_xml(pretty=True))
            soapEnvelope.validateBody(sdcClient._bicepsSchema.bmmSchema)


    def test_Set_setString(self):
        for sdcClient in self._allclients:
            setServiceClient = sdcClient._mkHostedServiceClient(porttype='Set', soapClient=None, hosted=self.hosted)
            soapEnvelope = setServiceClient._mkRequestedStringEnvelope( operationHandle='123', requestedString ='aaa42.42')
            print (soapEnvelope.as_xml(pretty=True))
            soapEnvelope.validateBody(sdcClient._bicepsSchema.bmmSchema)


    def test_Set_setAlertState(self):
        for sdcClient in self._allclients:
            descriptorClass = sdcClient.sdc_definitions.dc.getContainerClass(sdc11073.namespaces.domTag('AlertSignalDescriptor'))
            descr = descriptorClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    nodeName='Helga',
                                    handle='123',
                                    parentHandle='456',
                                    )
            stateClass = sdcClient.sdc_definitions.sc.getContainerClass(sdc11073.namespaces.domTag('AlertSignalState'))
            alertState = stateClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    descriptorContainer=descr)

            setServiceClient = sdcClient._mkHostedServiceClient(porttype='Set', soapClient=None, hosted=self.hosted)
            for state in 'On', 'Off', 'Psd':
                alertState.ActivationState = state
                soapEnvelope = setServiceClient._mkSetAlertEnvelope( operationHandle='op123',
                                                                           proposedAlertStates=[alertState])
                print (soapEnvelope.as_xml(pretty=True))
                soapEnvelope.validateBody(sdcClient._bicepsSchema.bmmSchema)

    def test_Set_setMetricState(self):
        for sdcClient in self._allclients:
            descriptorClass = sdcClient.sdc_definitions.dc.getContainerClass(sdc11073.namespaces.domTag('NumericMetricDescriptor'))
            descr = descriptorClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    nodeName='Helga',
                                    handle='123',
                                    parentHandle='456',
                                    )
            stateClass = sdcClient.sdc_definitions.sc.getContainerClass(sdc11073.namespaces.domTag('NumericMetricState'))
            metricState = stateClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                     descriptorContainer=descr)
            setServiceClient = sdcClient._mkHostedServiceClient(porttype='Set', soapClient=None, hosted=self.hosted)
            for state in 'On', 'Off', 'Shtdn', 'Fail':
                metricState.ActivationState = state
                metricState.BodySite = [sdc11073.pmtypes.CodedValue('abc', 'def')]
                soapEnvelope = setServiceClient._mkSetMetricStateEnvelope( operationHandle='op123',
                                                                                 proposedMetricStates=[metricState])
                print (soapEnvelope.as_xml(pretty=True))
                soapEnvelope.validateBody(sdcClient._bicepsSchema.bmmSchema)

    def test_sortIpaddresses(self):
        #              to be sorted            refIp     expected result
        test_data = [(['1.2.3.5', '1.2.3.4'], '1.2.3.1', ['1.2.3.4', '1.2.3.5']), 
                     (['1.2.3.3', '1.2.3.4'], '1.2.3.6', ['1.2.3.4', '1.2.3.3']),
                     (['1.2.3.1', '1.2.3.5'], '1.2.3.4', ['1.2.3.5', '1.2.3.1']),]
        for (addresses, refIp, expected) in test_data:
            result = sdc11073.sdcclient.sdcclientimpl.sortIPAddresses(addresses, refIp)
            self.assertEqual(result, expected)

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestClientProxies)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
