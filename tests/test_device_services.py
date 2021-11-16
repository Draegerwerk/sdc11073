# -*- coding: utf-8 -*-
import unittest
import uuid

from lxml import etree as etree_

from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.location import SdcLocation
from sdc11073.namespaces import Prefixes
from sdc11073.namespaces import msgTag, domTag, nsmap
from sdc11073.pmtypes import AlertConditionPriority
from sdc11073.pysoap.soapenvelope import Soap12Envelope
from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.addressing import Address
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
        self.msg_reader = self.sdc_device.msg_reader

        print('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        for d in self._alldevices:
            if d:
                d.stop_all()
        self.wsDiscovery.stop()
        print('############### tearDown {} done ##############'.format(self._testMethodName))

    def _mkGetRequest(self, sdcDevice, porttype, method, path) -> CreatedMessage:
        if sdcDevice is self.sdc_device:
            ns = sdcDevice.mdib.sdc_definitions.DPWS_SDCNamespace
        else:
            ns = sdcDevice.mdib.sdc_definitions.MessageModelNamespace
        action = '{}/{}/{}'.format(ns, porttype, method)
        body_node = etree_.Element(msgTag(method))
        soapEnvelope = Soap12Envelope(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.MSG))
        soapEnvelope.set_address(Address(action=action, addr_to=path))
        soapEnvelope.payload_element = body_node

        return CreatedMessage(soapEnvelope, sdcDevice.msg_factory)

    def test_dispatch(self):
        dispatcher = self.sdc_device._http_server_thread.dispatcher

        getService = self.sdc_device.hosted_services.get_service
        # path = getService.hosting_service.path_element
        path = self.sdc_device.path_prefix + '/Get'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdib', path)
        http_header = {}
        response_string = dispatcher.on_post(RequestData(http_header, path, 'foo', self.sdc_device.msg_factory.serialize_message(get_env)))
        self.assertTrue('/{}/GetMdibResponse'.format(getService.port_type_string).encode('utf-8') in response_string)

        contextService = self.sdc_device.hosted_services.context_service
        path = self.sdc_device.path_prefix + '/StateEvent'
        get_env = self._mkGetRequest(self.sdc_device, contextService.port_type_string, 'GetContextStates',
                                    path)
        http_header = {}
        response_string = dispatcher.on_post(RequestData(http_header, path, 'foo', self.sdc_device.msg_factory.serialize_message(get_env)))
        self.assertTrue(
            '/{}/GetContextStatesResponse'.format(contextService.port_type_string).encode('utf-8') in response_string)

    def test_getMdib(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdib', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(self.sdc_device.msg_factory.serialize_message(get_env))
        response = getService._on_get_mdib(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_getMdState(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdState', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(self.sdc_device.msg_factory.serialize_message(get_env))
        response = getService.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_getMdDescription(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdDescription', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(self.sdc_device.msg_factory.serialize_message(get_env))
        response = getService.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_changeAlarmPrio(self):
        getService = self.sdc_device.hosted_services.get_service
        path = '123'
        with self.sdc_device.mdib.transaction_manager() as tr:
            alarmConditionDescriptor = tr.get_descriptor('0xD3C00109')
            alarmConditionDescriptor.Priority = AlertConditionPriority.LOW
        get_env = self._mkGetRequest(self.sdc_device, getService.port_type_string, 'GetMdDescription', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(self.sdc_device.msg_factory.serialize_message(get_env))
        response = getService.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

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
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(self.sdc_device.msg_factory.serialize_message(get_env))
        response = contextService.hosting_service.on_post(request)
        _ns = self.sdc_device.mdib.nsmapper  # shortcut
        query = '{}[@{}="{}"]'.format(_ns.doc_name(Prefixes.MSG, 'ContextState'),
                                        _ns.doc_name(Prefixes.XSI, 'type'),
                                        _ns.doc_name(Prefixes.PM, 'LocationContextState'))
        locationContextNodes = response.p_msg.payload_element.xpath(query, namespaces=_ns.doc_ns_map)
        self.assertEqual(len(locationContextNodes), 1)
        identificationNode = locationContextNodes[0].find(domTag('Identification'))
        self.assertEqual(identificationNode.get('Extension'), '{}///{}//{}'.format(facility, poc, bed))

        locationDetailNode = locationContextNodes[0].find(domTag('LocationDetail'))
        self.assertEqual(locationDetailNode.get('PoC'), poc)
        self.assertEqual(locationDetailNode.get('Bed'), bed)
        self.assertEqual(locationDetailNode.get('Facility'), facility)

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
