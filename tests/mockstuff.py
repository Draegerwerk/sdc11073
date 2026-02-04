"""The module implements classes needed for testing.

It contains simplified replacements for WsDiscovery and Subscription on devices side.
It also contains SdcProvider implementations that simplify the instantiation of a device.
"""

from __future__ import annotations

import logging
import pathlib
import threading
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import SplitResult

from lxml import etree

from sdc11073.mdib import ProviderMdib
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.provider import SdcProvider
from sdc11073.provider.subscriptionmgr import BicepsSubscription
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.xml_types.eventing_types import Subscribe

if TYPE_CHECKING:
    import ipaddress
    import uuid

    import sdc11073.certloader
    from sdc11073.provider.components import SdcProviderComponents
    from sdc11073.provider.providerimpl import WsDiscoveryProtocol
    from sdc11073.pysoap.msgfactory import MessageFactory
    from sdc11073.pysoap.soapclientpool import SoapClientPool
    from sdc11073.xml_utils import LxmlElement
ports_lock = threading.Lock()
_ports = 10000

_mockhttpservers = {}

_logger = logging.getLogger('sdc.mock')


def dec_list(*args: float | str) -> list[Decimal]:
    """Convert a list of numbers to decimal."""
    return [Decimal(x) for x in args]


class MockWsDiscovery:
    """Implementation of a minimal WsDiscovery interface.

    The class does nothing except logging.
    """

    def __init__(self, ipaddress: str | ipaddress.IPv4Address):
        self._ipaddress = ipaddress

    def get_active_addresses(self) -> str:
        """Return the ip address."""
        return [self._ipaddress]

    def clear_service(self, epr: str):
        """Clear services."""
        _logger.info('clear_service "%r"', epr)


class TestDevSubscription(BicepsSubscription):
    """Can be used instead of real Subscription objects."""

    mode = 'SomeMode'
    notify_to = 'http://self.com:123'
    identifier = '0815'
    expires = 60
    notify_ref = 'a ref string'

    def __init__(self, filter_: list[str], soap_client_pool: SoapClientPool, msg_factory: MessageFactory):
        notify_ref_node = etree.Element(ns_hlp.WSE.tag('References'))
        ident_node = etree.SubElement(notify_ref_node, ns_hlp.WSE.tag('Identifier'))
        ident_node.text = self.notify_ref
        base_urls = [SplitResult('https', 'www.example.com:222', 'no_uuid', query=None, fragment=None)]
        accepted_encodings = ['foo']  # not needed here
        subscribe_request = Subscribe()
        subscribe_request.set_filter(' '.join(filter_))
        subscribe_request.Delivery.NotifyTo.Address = self.notify_to
        subscribe_request.Delivery.NotifyTo.ReferenceParameters = [notify_ref_node]
        subscribe_request.Delivery.NotifyTo.Mode = self.mode
        max_subscription_duration = 42
        subscr_mgr = None
        super().__init__(
            subscr_mgr,
            subscribe_request,
            accepted_encodings,
            base_urls,
            max_subscription_duration,
            soap_client_pool,
            msg_factory=msg_factory,
            log_prefix='test',
        )
        self.reports = []

    def send_notification_report(self, body_node: LxmlElement, action: str):
        """Send notification to subscriber."""
        info_block = HeaderInformationBlock(
            action=action,
            addr_to=self.notify_to_address,
            reference_parameters=self.notify_ref_params,
        )
        message = self._mk_notification_message(info_block, body_node)
        self.reports.append(message)

    async def async_send_notification_report(self, body_node: LxmlElement, action: str):
        """Send notification to subscriber."""
        info_block = HeaderInformationBlock(
            action=action,
            addr_to=self.notify_to_address,
            reference_parameters=self.notify_ref_params,
        )
        message = self._mk_notification_message(info_block, body_node)
        self.reports.append(message)

    async def async_send_notification_end_message(
        self,
        code: str = 'SourceShuttingDown',
        reason: str = 'Event source going off line.',
    ):
        """Do nothing.

        Implementation not needed for tests.
        """


class SomeDevice(SdcProvider):
    """A device used for unit tests. Some values are predefined."""

    def __init__(  # noqa: PLR0913
        self,
        wsdiscovery: WsDiscoveryProtocol,
        mdib_xml_data: bytes,
        epr: str | uuid.UUID | None = None,
        validate: bool = True,
        ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None,
        max_subscription_duration: int = 15,
        log_prefix: str = '',
        default_components: SdcProviderComponents | None = None,
        specific_components: SdcProviderComponents | None = None,
        chunk_size: int = 0,
        alternative_hostname: str | None = None,
    ):
        model = ThisModelType(
            manufacturer='Example Manufacturer',
            manufacturer_url='www.example-manufacturer.com',
            model_name='SomeDevice',
            model_number='1.0',
            model_url='www.example-manufacturer.com/whatever/you/want/model',
            presentation_url='www.example-manufacturer.com/whatever/you/want/presentation',
        )
        device = ThisDeviceType(friendly_name='Py SomeDevice', firmware_version='0.99', serial_number='12345')

        device_mdib_container = ProviderMdib.from_string(mdib_xml_data, log_prefix=log_prefix)
        device_mdib_container.instance_id = 1  # set the optional value
        # set Metadata
        mds_descriptors = device_mdib_container.descriptions.NODETYPE.get(pm.MdsDescriptor)
        for mds_descriptor in mds_descriptors:
            if mds_descriptor.MetaData is not None:
                mds_descriptor.MetaData.Manufacturer.append(pm_types.LocalizedText('Example Manufacturer'))
                mds_descriptor.MetaData.ModelName.append(pm_types.LocalizedText(model.ModelName[0].text))
                mds_descriptor.MetaData.SerialNumber.append('ABCD-1234')
                mds_descriptor.MetaData.ModelNumber = '0.99'
        super().__init__(
            wsdiscovery,
            model,
            device,
            device_mdib_container,
            epr,
            validate,
            ssl_context_container=ssl_context_container,
            max_subscription_duration=max_subscription_duration,
            log_prefix=log_prefix,
            default_components=default_components,
            specific_components=specific_components,
            chunk_size=chunk_size,
            alternative_hostname=alternative_hostname,
        )

    @classmethod
    def from_mdib_file(  # noqa: PLR0913
        cls,
        wsdiscovery: WsDiscoveryProtocol,
        epr: str | uuid.UUID | None,
        mdib_xml_path: str | pathlib.Path,
        validate: bool = True,
        ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None,
        max_subscription_duration: int = 15,
        log_prefix: str = '',
        default_components: SdcProviderComponents | None = None,
        specific_components: SdcProviderComponents | None = None,
        chunk_size: int = 0,
        alternative_hostname: str | None = None,
    ) -> SomeDevice:
        """Construct class with path to a mdib file."""
        mdib_xml_path = pathlib.Path(mdib_xml_path)
        if not mdib_xml_path.is_absolute():
            mdib_xml_path = pathlib.Path(__file__).parent.joinpath(mdib_xml_path)
        return cls(
            wsdiscovery,
            mdib_xml_path.read_bytes(),
            epr,
            validate,
            ssl_context_container,
            max_subscription_duration=max_subscription_duration,
            log_prefix=log_prefix,
            default_components=default_components,
            specific_components=specific_components,
            chunk_size=chunk_size,
            alternative_hostname=alternative_hostname,
        )
