import unittest

import sdc11073
from sdc11073 import definitions_sdc
from sdc11073.pmtypes import AlertActivation, ComponentActivation
from sdc11073.pysoap.soapenvelope import DPWSHosted, WsaEndpointReferenceType

# pylint: disable=protected-access
DEV_ADDRESS = 'http://169.254.0.200:10000'
CLIENT_VALIDATE = True


class TestClientProxies(unittest.TestCase):

    def setUp(self):
        self.sdcClient_final = sdc11073.sdcclient.SdcClient(DEV_ADDRESS,
                                                            sdc_definitions=definitions_sdc.SDC_v1_Definitions,
                                                            ssl_context=None,
                                                            validate=CLIENT_VALIDATE)
        self._allclients = (self.sdcClient_final,)
        self.hosted = DPWSHosted(endpoint_references_list=[WsaEndpointReferenceType('http://1.2.3.4:6000')],
                                 types_list=['xyz'],
                                 service_id='abc')

    def test_Get_GetMdib(self):
        for sdcClient in self._allclients:
            getServiceClient = sdcClient._mk_hosted_service_client(port_type='GetService', soap_client=None,
                                                                   hosted=self.hosted)
            soapEnvelope = getServiceClient._msg_factory.mk_getmdib_envelope(
                getServiceClient.endpoint_reference.address, getServiceClient.porttype)

            print(soapEnvelope.as_xml(pretty=True))

            soapEnvelope.validate_body(sdcClient._biceps_schema.message_schema)

    def test_Set_setNumericValue(self):
        for sdcClient in self._allclients:
            setServiceClient = sdcClient._mk_hosted_service_client(port_type='SetService', soap_client=None,
                                                                   hosted=self.hosted)
            soapEnvelope = setServiceClient._mk_requested_numeric_value_envelope(operation_handle='123',
                                                                                 requested_numeric_value=42.42)
            print(soapEnvelope.as_xml(pretty=True))
            soapEnvelope.validate_body(sdcClient._biceps_schema.message_schema)

    def test_Set_setString(self):
        for sdcClient in self._allclients:
            setServiceClient = sdcClient._mk_hosted_service_client(port_type='SetService', soap_client=None,
                                                                   hosted=self.hosted)
            soapEnvelope = setServiceClient._mk_requested_string_envelope(operation_handle='123',
                                                                          requested_string='aaa42.42')
            print(soapEnvelope.as_xml(pretty=True))
            soapEnvelope.validate_body(sdcClient._biceps_schema.message_schema)

    def test_Set_setAlertState(self):
        for sdcClient in self._allclients:
            descriptorClass = sdcClient.sdc_definitions.get_descriptor_container_class(
                sdc11073.namespaces.domTag('AlertSignalDescriptor'))
            descr = descriptorClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    handle='123',
                                    parent_handle='456',
                                    )
            stateClass = sdcClient.sdc_definitions.get_state_container_class(
                sdc11073.namespaces.domTag('AlertSignalState'))
            alertState = stateClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    descriptor_container=descr)

            setServiceClient = sdcClient._mk_hosted_service_client(port_type='SetService', soap_client=None,
                                                                   hosted=self.hosted)
            for state in list(AlertActivation):
                alertState.ActivationState = state
                soapEnvelope = setServiceClient._mk_set_alert_envelope(operation_handle='op123',
                                                                       proposed_alert_states=[alertState])
                print(soapEnvelope.as_xml(pretty=True))
                soapEnvelope.validate_body(sdcClient._biceps_schema.message_schema)

    def test_Set_setMetricState(self):
        for sdcClient in self._allclients:
            descriptorClass = sdcClient.sdc_definitions.get_descriptor_container_class(
                sdc11073.namespaces.domTag('NumericMetricDescriptor'))
            descr = descriptorClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                    handle='123',
                                    parent_handle='456',
                                    )
            stateClass = sdcClient.sdc_definitions.get_state_container_class(
                sdc11073.namespaces.domTag('NumericMetricState'))
            metricState = stateClass(nsmapper=sdc11073.namespaces.DocNamespaceHelper(),
                                     descriptor_container=descr)
            setServiceClient = sdcClient._mk_hosted_service_client(port_type='SetService', soap_client=None,
                                                                   hosted=self.hosted)
            for state in list(ComponentActivation):
                metricState.ActivationState = state
                metricState.BodySite = [sdc11073.pmtypes.CodedValue('abc', 'def')]
                soapEnvelope = setServiceClient._mk_set_metric_state_envelope(operation_handle='op123',
                                                                              proposed_metric_states=[metricState])
                print(soapEnvelope.as_xml(pretty=True))
                soapEnvelope.validate_body(sdcClient._biceps_schema.message_schema)

    def test_sortIpaddresses(self):
        #              to be sorted            refIp     expected result
        test_data = [(['1.2.3.5', '1.2.3.4'], '1.2.3.1', ['1.2.3.4', '1.2.3.5']),
                     (['1.2.3.3', '1.2.3.4'], '1.2.3.6', ['1.2.3.4', '1.2.3.3']),
                     (['1.2.3.1', '1.2.3.5'], '1.2.3.4', ['1.2.3.5', '1.2.3.1']), ]
        for (addresses, refIp, expected) in test_data:
            result = sdc11073.sdcclient.sdcclientimpl.sort_ip_addresses(addresses, refIp)
            self.assertEqual(result, expected)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestClientProxies)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
