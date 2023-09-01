import unittest

from lxml import etree as etree_

from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types.pm_types import AlertConditionPriority
from sdc11073.pysoap.msgfactory import CreatedMessage
from sdc11073.pysoap.soapenvelope import Soap12Envelope
from sdc11073.dispatch.request import RequestData
from sdc11073.wsdiscovery import WSDiscovery
from tests import mockstuff, utils

_sdc_ns = ns_hlp.SDC.namespace


class TestDeviceServices(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()
        """ validate test data"""
        print('############### setUp {}... ##############'.format(self._testMethodName))
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        my_uuid = None  # let device create one
        self.sdc_device = mockstuff.SomeDevice.from_mdib_file(self.wsd, my_uuid, '70041_MDIB_Final.xml')
        self.sdc_device.start_all()
        self.msg_reader = self.sdc_device.msg_reader

        print('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        self.sdc_device.stop_all()
        self.wsd.stop()
        print('############### tearDown {} done ##############'.format(self._testMethodName))

    def _mk_get_request(self, sdc_device, port_type, method, path) -> CreatedMessage:
        name_space = sdc_device.mdib.sdc_definitions.ActionsNamespace
        nsm = self.sdc_device.mdib.nsmapper  # shortcut

        action = '{}/{}/{}'.format(name_space, port_type, method)
        body_node = etree_.Element(nsm.MSG.tag(method))
        soap_envelope = Soap12Envelope(nsm.partial_map(nsm.S12, nsm.WSA, nsm.MSG))
        soap_envelope.set_header_info_block(HeaderInformationBlock(action=action, addr_to=path))
        soap_envelope.payload_element = body_node

        return CreatedMessage(soap_envelope, sdc_device.msg_factory)

    def test_dispatch(self):
        dispatcher = self.sdc_device._http_server.dispatcher

        get_service = self.sdc_device.hosted_services.get_service
        path = self.sdc_device.path_prefix + '/Get'
        get_env = self._mk_get_request(self.sdc_device, get_service.port_type_name.localname, 'GetMdib', path)
        http_header = {}
        peer_name = 'foo'
        sub_dispatcher = dispatcher.get_instance(self.sdc_device.path_prefix)
        response = sub_dispatcher.do_post(http_header, path, peer_name,
                                          self.sdc_device.msg_factory.serialize_message(get_env))
        code, reason, response_string = response
        self.assertTrue(f'/{get_service.port_type_name.localname}/GetMdibResponse'.encode() in response_string)

        context_service = self.sdc_device.hosted_services.context_service
        path = self.sdc_device.path_prefix + '/StateEvent'
        get_env = self._mk_get_request(self.sdc_device, context_service.port_type_name.localname, 'GetContextStates',
                                     path)
        http_header = {}
        response = sub_dispatcher.do_post(http_header, path, peer_name,
                                          self.sdc_device.msg_factory.serialize_message(get_env))
        code, reason, response_string = response

        self.assertTrue(
            f'/{context_service.port_type_name.localname}/GetContextStatesResponse'.encode() in response_string)

    def test_getMdib(self):
        get_service = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mk_get_request(self.sdc_device, get_service.port_type_name.localname, 'GetMdib', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(
            self.sdc_device.msg_factory.serialize_message(get_env))
        response = get_service._on_get_mdib(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_getMdState(self):
        get_service = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mk_get_request(self.sdc_device, get_service.port_type_name.localname, 'GetMdState', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(
            self.sdc_device.msg_factory.serialize_message(get_env))
        response = get_service.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_getMdDescription(self):
        get_service = self.sdc_device.hosted_services.get_service
        path = '123'
        get_env = self._mk_get_request(self.sdc_device, get_service.port_type_name.localname, 'GetMdDescription', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(
            self.sdc_device.msg_factory.serialize_message(get_env))
        response = get_service.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_changeAlarmPrio(self):
        get_service = self.sdc_device.hosted_services.get_service
        path = '123'
        with self.sdc_device.mdib.transaction_manager() as tr:
            alarmConditionDescriptor = tr.get_descriptor('0xD3C00109')
            alarmConditionDescriptor.Priority = AlertConditionPriority.LOW
        get_env = self._mk_get_request(self.sdc_device, get_service.port_type_name.localname, 'GetMdDescription', path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(
            self.sdc_device.msg_factory.serialize_message(get_env))
        response = get_service.hosting_service.on_post(request)
        msg_node = response.p_msg.payload_element
        self.assertEqual(msg_node.attrib['MdibVersion'], str(self.sdc_device.mdib.mdib_version))
        self.assertEqual(msg_node.attrib['SequenceId'], str(self.sdc_device.mdib.sequence_id))

    def test_getContextStates(self):
        loc = utils.random_location()
        self.sdc_device.set_location(loc)
        context_service = self.sdc_device.hosted_services.context_service
        path = '123'
        get_env = self._mk_get_request(self.sdc_device, context_service.port_type_name.localname, 'GetContextStates',
                                     path)
        http_header = {}
        request = RequestData(http_header, path, 'foo')
        request.message_data = self.msg_reader.read_received_message(
            self.sdc_device.msg_factory.serialize_message(get_env))
        response = context_service.hosting_service.on_post(request)
        _ns = self.sdc_device.mdib.nsmapper  # shortcut
        query = '{}[@{}="{}"]'.format(_ns.MSG.doc_name('ContextState'),
                                      _ns.XSI.doc_name('type'),
                                      _ns.PM.doc_name('LocationContextState'))
        locationContextNodes = response.p_msg.payload_element.xpath(query, namespaces=_ns.ns_map)
        self.assertEqual(len(locationContextNodes), 1)
        identificationNode = locationContextNodes[0].find(pm.Identification)
        self.assertEqual(identificationNode.get('Extension'), '{}/{}/{}/{}/{}/{}'.format(loc.fac, loc.bldng, loc.flr, loc.poc, loc.rm,loc.bed))

        locationDetailNode = locationContextNodes[0].find(pm.LocationDetail)
        self.assertEqual(locationDetailNode.get('PoC'), loc.poc)
        self.assertEqual(locationDetailNode.get('Bed'), loc.bed)
        self.assertEqual(locationDetailNode.get('Facility'), loc.fac)

    def test_wsdl(self):
        """
        check port type and action namespaces in wsdl
        """
        dev = self.sdc_device
        _ns = dev.mdib.nsmapper  # shortcut
        for hosted in dev.hosted_services.dpws_hosted_services.values():
            wsdl = etree_.fromstring(hosted._wsdl_string)
            inputs = wsdl.xpath(f'//{_ns.WSDL.doc_name("input")}', namespaces=_ns.ns_map)
            outputs = wsdl.xpath(f'//{_ns.WSDL.doc_name("output")}', namespaces=_ns.ns_map)
            self.assertGreater(len(inputs), 0)
            self.assertGreater(len(outputs), 0)
            for src in (inputs, outputs):
                for i in inputs:
                    action_keys = [k for k in i.attrib.keys() if k.endswith('Action')]
                    for k in action_keys:
                        action = i.attrib[k]
                        self.assertTrue(action.startswith(SdcV1Definitions.ActionsNamespace))
