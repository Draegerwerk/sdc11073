from __future__ import annotations

import logging
import os.path
import threading
from urllib.parse import SplitResult
from decimal import Decimal
from typing import TYPE_CHECKING, Union

from lxml import etree as etree_

from sdc11073.mdib import DeviceMdibContainer
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.sdcdevice import SdcDevice
from sdc11073.sdcdevice.subscriptionmgr import BicepsSubscription
from sdc11073.xml_types import pm_types, pm_qnames as pm
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.dpws_types import ThisModelType, ThisDeviceType
from sdc11073.xml_types.eventing_types import Subscribe

if TYPE_CHECKING:
    import uuid
    from sdc11073.pysoap.soapclientpool import SoapClientPool

portsLock = threading.Lock()
_ports = 10000

_mockhttpservers = {}

_logger = logging.getLogger('sdc.mock')


def dec_list(*args):
    return [Decimal(x) for x in args]


def resetModule():
    global _ports
    _mockhttpservers.clear()
    _ports = 10000


def _findServer(netloc):
    dev_addr = netloc.split(':')
    dev_addr = tuple([dev_addr[0], int(dev_addr[1])])  # make port number an integer
    for key, srv in _mockhttpservers.items():
        if tuple(key) == dev_addr:
            return srv
    raise KeyError('{} is not in {}'.format(dev_addr, _mockhttpservers.keys()))


class MockWsDiscovery(object):
    def __init__(self, ipaddresses):
        self._ipaddresses = ipaddresses

    def get_active_addresses(self):
        return self._ipaddresses

    def clear_service(self, epr):
        _logger.info('clear_service "{}"'.format(epr))


class TestDevSubscription(BicepsSubscription):
    """ Can be used instead of real Subscription objects"""
    mode = 'SomeMode'
    notify_to = 'http://self.com:123'
    identifier = '0815'
    expires = 60
    notifyRef = 'a ref string'

    def __init__(self, filter_,
                 soap_client_pool: SoapClientPool,
                 msg_factory):
        notify_ref_node = etree_.Element(ns_hlp.wseTag('References'))
        identNode = etree_.SubElement(notify_ref_node, ns_hlp.wseTag('Identifier'))
        identNode.text = self.notifyRef
        base_urls = [SplitResult('https', 'www.example.com:222', 'no_uuid', query=None, fragment=None)]
        accepted_encodings = ['foo']  # not needed here
        subscribe_request = Subscribe()
        subscribe_request.set_filter(' '.join(filter_))
        subscribe_request.Delivery.NotifyTo.Address = self.notify_to
        subscribe_request.Delivery.NotifyTo.ReferenceParameters = [notify_ref_node]
        subscribe_request.Delivery.NotifyTo.Mode = self.mode
        max_subscription_duration = 42
        subscr_mgr = None
        super().__init__(subscr_mgr, subscribe_request, accepted_encodings, base_urls, max_subscription_duration, soap_client_pool,
                         msg_factory=msg_factory, log_prefix='test')
        self.reports = []

    def send_notification_report(self, body_node, action):
        info_block = HeaderInformationBlock(action=action,
                                            addr_to=self.notify_to_address,
                                            reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(info_block, body_node)
        self.reports.append(message)

    async def async_send_notification_report(self, body_node, action):
        info_block = HeaderInformationBlock(action=action,
                                            addr_to=self.notify_to_address,
                                            reference_parameters=self.notify_ref_params)
        message = self._mk_notification_message(info_block, body_node)
        self.reports.append(message)

    async def async_send_notification_end_message(self, code='SourceShuttingDown',
                                                  reason='Event source going off line.'):
        pass


class SomeDevice(SdcDevice):
    """A device used for unit tests

    """

    def __init__(self, wsdiscovery, mdib_xml_string,
                 epr: Union[str, uuid.UUID, None] = None,
                 validate=True, ssl_context=None, log_prefix='',
                 default_components=None, specific_components=None,
                 chunked_messages=False):
        model = ThisModelType(manufacturer='Draeger CoC Systems',
                              manufacturer_url='www.draeger.com',
                              model_name='SomeDevice',
                              model_number='1.0',
                              model_url='www.draeger.com/whatever/you/want/model',
                              presentation_url='www.draeger.com/whatever/you/want/presentation')
        device = ThisDeviceType(friendly_name='Py SomeDevice',
                                firmware_version='0.99',
                                serial_number='12345')

        device_mdib_container = DeviceMdibContainer.from_string(mdib_xml_string, log_prefix=log_prefix)
        # set Metadata
        mdsDescriptors = device_mdib_container.descriptions.NODETYPE.get(pm.MdsDescriptor)
        for mdsDescriptor in mdsDescriptors:
            if mdsDescriptor.MetaData is not None:
                mdsDescriptor.MetaData.Manufacturer.append(pm_types.LocalizedText(u'Dräger'))
                mdsDescriptor.MetaData.ModelName.append(pm_types.LocalizedText(model.ModelName[0].text))
                mdsDescriptor.MetaData.SerialNumber.append('ABCD-1234')
                mdsDescriptor.MetaData.ModelNumber = '0.99'
        super().__init__(wsdiscovery, model, device, device_mdib_container, epr, validate,
                         ssl_context=ssl_context, log_prefix=log_prefix,
                         default_components=default_components,
                         specific_components=specific_components,
                         chunked_messages=chunked_messages)

    @classmethod
    def from_mdib_file(cls, wsdiscovery, my_uuid, mdib_xml_path,
                       validate=True, ssl_context=None, log_prefix='',
                       default_components=None, specific_components=None,
                       chunked_messages=False):
        """
        An alternative constructor for the class
        """
        if not os.path.isabs(mdib_xml_path):
            here = os.path.dirname(__file__)
            mdib_xml_path = os.path.join(here, mdib_xml_path)

        with open(mdib_xml_path, 'rb') as f:
            mdib_xml_string = f.read()
        return cls(wsdiscovery, mdib_xml_string, my_uuid, validate, ssl_context, log_prefix=log_prefix,
                   default_components=default_components, specific_components=specific_components,
                   chunked_messages=chunked_messages)
