# -*- coding: utf-8 -*-
import unittest
import uuid

from lxml import etree as etree_

from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.location import SdcLocation
from sdc11073.namespaces import Prefixes
from sdc11073.namespaces import msgTag, domTag, nsmap
from sdc11073.pmtypes import AlertConditionPriority
from sdc11073.pysoap.soapenvelope import WsAddress, Soap12Envelope, ReceivedSoap12Envelope
from sdc11073.wsdiscovery import WSDiscoveryWhitelist
from sdc11073.sdcdevice.httpserver import RequestData
from sdc11073.loghelper import basic_logging_setup

from tests import mockstuff

_sdc_ns = Prefixes.SDC.namespace


class TestDeviceServices(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()
        ''' validate test data'''
        print('############### setUp {}... ##############'.format(self._testMethodName))
        self.wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsDiscovery.start()
        my_uuid = None  # let device create one
        self.sdc_device = mockstuff.SomeDevice.from_mdib_file(self.wsDiscovery, my_uuid, '70041_MDIB_Final.xml')
        self.sdc_device.start_all()
        self._alldevices = (self.sdc_device,)
        print('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        for d in self._alldevices:
            if d:
                d.stop_all()
        self.wsDiscovery.stop()
        print('############### tearDown {} done ##############'.format(self._testMethodName))

    def _mkGetRequest(self, sdcDevice, porttype, method, path):
        if sdcDevice is self.sdc_device:
            ns = sdcDevice.mdib.sdc_definitions.DPWS_SDCNamespace
        else:
            ns = sdcDevice.mdib.sdc_definitions.MessageModelNamespace
        action = '{}/{}/{}'.format(ns, porttype, method)
        body_node = etree_.Element(msgTag(method))
        soapEnvelope = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.MSG))
        identifier = uuid.uuid4().urn
        soapEnvelope.add_header_object(WsAddress(message_id=identifier,
                                                 action=action,
                                                 addr_to=path))
        soapEnvelope.add_body_element(body_node)

        soapEnvelope.validate_body(sdcDevice.mdib.biceps_schema.message_schema)
        return soapEnvelope

    def test_dispatch(self):
        dispatcher = self.sdc_device._http_server_thread.dispatcher

        getService = self.sdc_device.hosted_services.get_service
        # path = getService.hosting_service.path_element
        path = self.sdc_device.path_prefix + '/Get'
        getEnv = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdib', path)
        http_header = {}
        #response_string = dispatcher.on_post(path, http_header, getEnv.as_xml())
        response_string = dispatcher.on_post(RequestData(http_header, path, getEnv.as_xml()))
        self.assertTrue('/{}/GetMdibResponse'.format(getService.port_type_string).encode('utf-8') in response_string)

        contextService = self.sdc_device.hosted_services.context_service
        path = self.sdc_device.path_prefix + '/StateEvent'
        getEnv = self._mkGetRequest(self.sdc_device, contextService.port_type_string, 'GetContextStates',
                                    path)
        http_header = {}
        #response_string = dispatcher.on_post(path, http_header, getEnv.as_xml())
        response_string = dispatcher.on_post(RequestData(http_header, path,  getEnv.as_xml()))
        self.assertTrue(
            '/{}/GetContextStatesResponse'.format(contextService.port_type_string).encode('utf-8') in response_string)

    def test_getMdib(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdib', path)
        http_header = {}
        request = RequestData(http_header, path)
        request.envelope = ReceivedSoap12Envelope(get_env.as_xml())
        response = getService._on_get_mdib(request)
        response.validate_body(self.sdc_device.mdib.biceps_schema.message_schema)

    def test_getMdState(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdState', path)
        http_header = {}
        request = RequestData(http_header, path)
        request.envelope = ReceivedSoap12Envelope(get_env.as_xml())
        response = getService.hosting_service.on_post(request)
        response.validate_body(self.sdc_device.mdib.biceps_schema.message_schema)

    def test_getMdDescription(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdDescription', path)
        http_header = {}
        request = RequestData(http_header, path)
        request.envelope = ReceivedSoap12Envelope(get_env.as_xml())
        response = getService.hosting_service.on_post(request)
        response.validate_body(self.sdc_device.mdib.biceps_schema.message_schema)

    def test_changeAlarmPrio(self):
        """ This is a test for defect SDCSIM-129
        The order of children of """
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        with self.sdc_device.mdib.transaction_manager() as tr:
            alarmConditionDescriptor = tr.get_descriptor('0xD3C00109')
            alarmConditionDescriptor.Priority = AlertConditionPriority.LOW
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdDescription', path)
        #receivedEnv = ReceivedSoap12Envelope(getEnv.as_xml())
        http_header = {}
        request = RequestData(http_header, path)
        request.envelope = ReceivedSoap12Envelope(get_env.as_xml())
        response = getService.hosting_service.on_post(request)
        response.validate_body(self.sdc_device.mdib.biceps_schema.message_schema)

    def test_getContextStates(self):
        facility = 'HOSP42'
        poc = 'Care Unit 1'
        bed = 'my bed'
        loc = SdcLocation(fac=facility, poc=poc, bed=bed)
        self.sdc_device.mdib.set_location(loc)
        contextService = self.sdc_device.hosted_services.context_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, contextService.port_type_string, 'GetContextStates',
                                    path)
        http_header = {}
        request = RequestData(http_header, path)
        request.envelope = ReceivedSoap12Envelope(get_env.as_xml())
        response = contextService.hosting_service.on_post(request)
        print(response.as_xml(pretty=True))
        response.validate_body(self.sdc_device.mdib.biceps_schema.message_schema)
        _ns = self.sdc_device.mdib.nsmapper  # shortcut
        query = '*/{}[@{}="{}"]'.format(_ns.doc_name(Prefixes.MSG, 'ContextState'),
                                        _ns.doc_name(Prefixes.XSI, 'type'),
                                        _ns.doc_name(Prefixes.PM, 'LocationContextState'))
        locationContextNodes = response.body_node.xpath(query, namespaces=_ns.doc_ns_map)
        self.assertEqual(len(locationContextNodes), 1)
        identificationNode = locationContextNodes[0].find(domTag('Identification'))
        self.assertEqual(identificationNode.get('Extension'), '{}///{}//{}'.format(facility, poc, bed))

        locationDetailNode = locationContextNodes[0].find(domTag('LocationDetail'))
        self.assertEqual(locationDetailNode.get('PoC'), poc)
        self.assertEqual(locationDetailNode.get('Bed'), bed)
        self.assertEqual(locationDetailNode.get('Facility'), facility)
        print(response.as_xml(pretty=True))

    def test_wsdl(self):
        """
        check porttype and action namespaces in wsdl
        """
        dev = self.sdc_device
        for hosted in dev.hosted_services.dpws_hosted_services:
            wsdl = etree_.fromstring(hosted._wsdl_string)
            inputs = wsdl.xpath('//wsdl:input', namespaces=nsmap)  # {'wsdl':'http://schemas.xmlsoap.org/wsdl/'})
            outputs = wsdl.xpath('//wsdl:output', namespaces=nsmap)  # {'wsdl':'http://schemas.xmlsoap.org/wsdl/'})
            self.assertGreater(len(inputs), 0)
            self.assertGreater(len(outputs), 0)
            for src in (inputs, outputs):
                for i in inputs:
                    action_keys = [k for k in i.attrib.keys() if k.endswith('Action')]
                    for k in action_keys:
                        action = i.attrib[k]
                        self.assertTrue(action.startswith(SDC_v1_Definitions.ActionsNamespace))

    def test_metadata(self):
        """
        verifies that
        - 7 hosted services exist ( one per port type)
        - every port type has BICEPS Message Model as namespace
        """
        dev = self.sdc_device
        metadata_node = dev._mk_metadata_node()
        print(etree_.tostring(metadata_node))
        dpws_hosted = metadata_node.xpath('//dpws:Hosted',
                                          namespaces={'dpws': 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01'})
        self.assertEqual(len(dpws_hosted), 4)  #
        for h in dpws_hosted:
            dpws_types = h.xpath('dpws:Types', namespaces={'dpws': 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01'})
            for t in dpws_types:
                txt = t.text
                port_types = txt.split()
                for p in port_types:
                    ns, value = p.split(':')
                    self.assertEqual(metadata_node.nsmap[ns], _sdc_ns)
