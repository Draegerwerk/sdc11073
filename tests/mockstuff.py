import threading
import logging
import os.path
import urllib
from sdc11073.pysoap.soapenvelope import Soap12Envelope, DPWSThisModel, DPWSThisDevice, WsAddress
from sdc11073.sdcdevice.subscriptionmgr import _DevSubscription
from sdc11073.mdib import DeviceMdibContainer
from sdc11073 import namespaces
from sdc11073 import pmtypes

from sdc11073.sdcdevice import  SdcDevice
from lxml import etree as etree_
portsLock = threading.Lock()
_ports = 10000

_mockhttpservers = {}

_logger = logging.getLogger('sdc.mock')

def resetModule():
    global _ports
    _mockhttpservers.clear()
    _ports = 10000

def _findServer(netloc):
    dev_addr = netloc.split(':')
    dev_addr = tuple([dev_addr[0], int(dev_addr[1])]) # make port number an integer
    for key, srv in _mockhttpservers.items():
        if tuple(key) == dev_addr:
            return srv
    raise KeyError('{} is not in {}'.format(dev_addr, _mockhttpservers.keys() ))



class MockWsDiscovery(object):
    def __init__(self, ipaddresses):
        self._ipaddresses = ipaddresses
    
    def get_active_addresses(self):
        return self._ipaddresses

    def clear_service(self, epr):
        _logger.info ('clear_service "{}"'.format(epr))



class TestDevSubscription(_DevSubscription):
    """ Can be used instead of real Subscription objects"""
    mode = 'SomeMode'
    notify_to = 'http://self.com:123'
    identifier = '0815'
    expires = 60
    notifyRef = 'a ref string'
    def __init__(self, filter_, schema_validators):
        notify_ref_node = etree_.Element(namespaces.wseTag('References'))
        identNode = etree_.SubElement(notify_ref_node, namespaces.wseTag('Identifier'))
        identNode.text = self.notifyRef
        base_urls = [ urllib.parse.SplitResult('https', 'www.example.com:222', 'no_uuid', query=None, fragment=None)]

        super().__init__(mode=self.mode,
                         notify_to_address=self.notify_to,
                         notify_ref_node=notify_ref_node,
                         end_to_address=None,
                         end_to_ref_node=None,
                         expires=self.expires,
                         max_subscription_duration=42,
                         filter_=filter_,
                         ssl_context=None,
                         schema_validators=schema_validators,
                         accepted_encodings=None,
                         base_urls=base_urls)
        self.reports = []
        self.message_schema = schema_validators.message_schema
        
        
    def send_notification_report(self, msg_factory, body_node, action, doc_nsmap):
        addr = WsAddress(addr_to=self.notify_to_address,
                         action=action,
                         addr_from=None,
                         reply_to=None,
                         fault_to=None,
                         reference_parameters_node=None)
        rep = msg_factory.mk_notification_report(addr, body_node, self.notify_ref_nodes, doc_nsmap)

#        soapEnvelope = Soap12Envelope(doc_nsmap)
#        soapEnvelope.add_body_element(body_node)
#        rep = msg_factory._mk_notification_report(soapEnvelope, action)
        try:
            rep.validate_body(self.message_schema)
        except:
            print (rep.as_xml(pretty=True))
            raise
        self.reports.append(rep)


class SomeDevice(SdcDevice):
    """A device used for unit tests

    """
    def __init__(self, wsdiscovery, mdib_xml_string, my_uuid=None,
                 validate=True, ssl_context=None, log_prefix='', specific_components=None,
                 chunked_messages=False):
        model = DPWSThisModel(manufacturer='Draeger CoC Systems',
                              manufacturer_url='www.draeger.com',
                              model_name='SomeDevice',
                              model_number='1.0',
                              model_url='www.draeger.com/whatever/you/want/model',
                              presentation_url='www.draeger.com/whatever/you/want/presentation')
        device = DPWSThisDevice(friendly_name='Py SomeDevice',
                                firmware_version='0.99',
                                serial_number='12345')
        device_mdib_container = DeviceMdibContainer.from_string(mdib_xml_string, log_prefix=log_prefix)
        # set Metadata
        mdsDescriptor = device_mdib_container.descriptions.NODETYPE.get_one(namespaces.domTag('MdsDescriptor'))
        mdsDescriptor.MetaData.Manufacturer.append(pmtypes.LocalizedText(u'Dräger'))
        mdsDescriptor.MetaData.ModelName.append(pmtypes.LocalizedText(model.model_name[None]))
        mdsDescriptor.MetaData.SerialNumber.append('ABCD-1234')
        mdsDescriptor.MetaData.ModelNumber = '0.99'
        super(SomeDevice, self).__init__(wsdiscovery, model, device, device_mdib_container, my_uuid, validate,
                                         ssl_context=ssl_context, log_prefix=log_prefix,
                                         specific_components=specific_components,
                                         chunked_messages=chunked_messages)

    @classmethod
    def from_mdib_file(cls, wsdiscovery, my_uuid, mdib_xml_path,
                 validate=True, ssl_context=None, log_level=logging.INFO, log_prefix='',
                       specific_components=None, chunked_messages=False):
        """
        An alternative constructor for the class
        """
        if not os.path.isabs(mdib_xml_path):
            here = os.path.dirname(__file__)
            mdib_xml_path = os.path.join(here, mdib_xml_path)

        with open(mdib_xml_path, 'rb') as f:
            mdib_xml_string = f.read()
        return cls(wsdiscovery, mdib_xml_string, my_uuid, validate, ssl_context, log_prefix=log_prefix,
                   specific_components=specific_components,
                   chunked_messages=chunked_messages)
