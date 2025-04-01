"""Implementation of discovery proxy client.

A discovery proxy prototype has been implemented based on
https://confluence.hl7.org/display/GP/Topic%3A+Discovery+Proxy+Actors

This client connects to that proxy.
"""

from __future__ import annotations

import os
import pathlib
import random
import time
from typing import TYPE_CHECKING
from uuid import UUID

from sdc11073.certloader import mk_ssl_contexts_from_folder
from sdc11073.consumer.request_handler_deferred import EmptyResponse
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch import MessageConverterMiddleware
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase
from sdc11073.location import SdcLocation
from sdc11073.loghelper import LoggerAdapter, basic_logging_setup, get_logger_adapter
from sdc11073.mdib import ProviderMdib
from sdc11073.namespaces import EventingActions
from sdc11073.namespaces import default_ns_helper as nsh
from sdc11073.provider import SdcProvider
from sdc11073.pysoap.msgfactory import CreatedMessage, MessageFactory
from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage
from sdc11073.pysoap.soapclient import Fault, SoapClient
from sdc11073.wsdiscovery.wsdimpl import Service
from sdc11073.xml_types import eventing_types, pm_types, wsd_types
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lxml.etree import QName

    from sdc11073.certloader import SSLContextContainer
    from sdc11073.dispatch.request import RequestData
    from sdc11073.xml_types.basetypes import MessageType

message_factory = MessageFactory(SdcV1Definitions, None, logger=get_logger_adapter('sdc.disco.msg'))
message_reader = MessageReader(SdcV1Definitions, None, logger=get_logger_adapter('sdc.disco.msg'))

ADDRESS_ALL = 'urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01'  # format acc to RFC 2141


def _mk_wsd_soap_message(header_info: HeaderInformationBlock, payload: MessageType) -> CreatedMessage:
    # use discovery specific namespaces
    return message_factory.mk_soap_message(
        header_info,
        payload,
        ns_list=[nsh.S12, nsh.WSA, nsh.WSD],
        use_defaults=False,
    )


class DiscoProxyClient:
    """Discovery proxy consumer."""

    def __init__(
        self,
        disco_proxy_address: str,
        my_address: str,
        ssl_context_container: SSLContextContainer | None = None,
    ):
        self._proxy_address = disco_proxy_address
        self._my_address = my_address
        self._ssl_context_container = ssl_context_container
        self._logger = get_logger_adapter('sdc.disco')
        self._local_services: dict[str, Service] = {}
        self._remote_services: dict[str, Service] = {}
        ssl_context = None if ssl_context_container is None else ssl_context_container.client_context
        self._soap_client = SoapClient(
            disco_proxy_address,
            socket_timeout=5,
            logger=get_logger_adapter('sdc.disco.client'),
            ssl_context=ssl_context,
            sdc_definitions=SdcV1Definitions,
            msg_reader=message_reader,
        )
        self._http_server = HttpServerThreadBase(
            my_address,
            ssl_context_container.server_context if ssl_context_container else None,
            logger=get_logger_adapter('sdc.disco.httpsrv'),
            supported_encodings=['gzip'],
        )

        self._msg_converter = MessageConverterMiddleware(message_reader, message_factory, self._logger, self)
        self._my_server_port = None
        self.subscribe_response = None

    def start(self, subscribe: bool = True):
        """Subscribe."""
        # first start http server, the services need to know the ip port number
        self._http_server.start()

        event_is_set = self._http_server.started_evt.wait(timeout=15.0)
        if not event_is_set:
            self._logger.error('Cannot start device, start event of http server not set.')
            raise RuntimeError('Cannot start device, start event of http server not set.')
        self._my_server_port = self._http_server.my_port
        self._http_server.dispatcher.register_instance('', self._msg_converter)

        if subscribe:
            self.send_subscribe()

    def stop(self, unsubscribe: bool = False):
        """Unsubscribe."""
        # it seems that unsubscribe is not supported
        if unsubscribe:
            self.send_unsubscribe()
        self._http_server.stop()

    def get_active_addresses(self) -> list[str]:
        """Get active addresses."""
        # TODO: do not return list  # noqa: FIX002, TD002, TD003
        return [self._my_address]

    def search_services(
        self,
        types: Iterable[QName] | None = None,
        scopes: wsd_types.ScopesType | None = None,
    ) -> list[Service]:
        """Send a Probe message.

        Update known services with found services. Return list of services found in probe response.
        """
        payload = wsd_types.ProbeType()
        payload.Types = types
        if scopes is not None:
            payload.Scopes = scopes

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)
        received_message = self._soap_client.post_message_to('', created_message)
        probe_response = wsd_types.ProbeMatchesType.from_node(received_message.p_msg.msg_node)
        result = []
        for probe_match in probe_response.ProbeMatch:
            service = Service(
                types=probe_match.Types,
                scopes=probe_match.Scopes,
                x_addrs=probe_match.XAddrs,
                epr=probe_match.EndpointReference.Address,
                instance_id='',
                metadata_version=probe_match.MetadataVersion,
            )
            self._remote_services[service.epr] = service
            result.append(service)
        return result

    def send_resolve(self, epr: str) -> wsd_types.ResolveMatchesType:
        """Send resolve."""
        payload = wsd_types.ResolveType()
        payload.EndpointReference.Address = epr
        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, payload)
        received_message = self._soap_client.post_message_to('', created_message)
        return wsd_types.ResolveMatchesType.from_node(received_message.p_msg.msg_node)

    def clear_remote_services(self):
        """Clear remotely discovered services."""
        self._remote_services.clear()

    def publish_service(self, epr: str, types: list[QName], scopes: wsd_types.ScopesType, x_addrs: list[str]):
        """Publish services."""
        metadata_version = 1
        instance_id = str(random.randint(1, 0xFFFFFFFF))
        service = Service(types, scopes, x_addrs, epr, instance_id, metadata_version=metadata_version)
        self._logger.info('publishing %r', service)
        self._local_services[epr] = service

        service.increment_message_number()
        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        payload = wsd_types.HelloType()
        payload.Types = service.types
        payload.Scopes = service.scopes
        payload.XAddrs = service.x_addrs
        payload.EndpointReference.Address = service.epr

        inf = HeaderInformationBlock(action=payload.action, addr_to=ADDRESS_ALL)

        created_message = _mk_wsd_soap_message(inf, payload)
        created_message.p_msg.add_header_element(
            app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'), ns_map=nsh.partial_map(nsh.WSD)),
        )
        created_message = _mk_wsd_soap_message(inf, payload)
        self._soap_client.post_message_to('', created_message)

    def send_subscribe(self) -> ReceivedMessage:
        """Send subscribe message."""
        subscribe_request = eventing_types.Subscribe()
        subscribe_request.Delivery.NotifyTo.Address = f'https://{self._my_address}:{self._my_server_port}'
        subscribe_request.Expires = 3600
        subscribe_request.set_filter('', dialect='http://discoproxy')
        inf = HeaderInformationBlock(action=subscribe_request.action, addr_to=ADDRESS_ALL)
        created_message = _mk_wsd_soap_message(inf, subscribe_request)
        received_message = self._soap_client.post_message_to('', created_message)
        response_action = received_message.action
        if response_action == EventingActions.SubscribeResponse:
            self.subscribe_response = eventing_types.SubscribeResponse.from_node(received_message.p_msg.msg_node)
        elif response_action == Fault.NODETYPE:
            fault = Fault.from_node(received_message.p_msg.msg_node)
            self._logger.error(  # noqa: PLE1205
                'subscribe: Fault  response : {}',
                fault,
            )
        return received_message

    def send_unsubscribe(self):
        """Send an unsubscribe request to the provider and handle the response."""
        if not self.subscribe_response:
            return
        subscribe_response, self.subscribe_response = self.subscribe_response, None
        request = eventing_types.Unsubscribe()
        dev_reference_param = subscribe_response.SubscriptionManager.ReferenceParameters
        subscription_manager_address = subscribe_response.SubscriptionManager.Address
        inf = HeaderInformationBlock(
            action=request.action,
            addr_to=subscription_manager_address,
            reference_parameters=dev_reference_param,
        )
        message = message_factory.mk_soap_message(inf, payload=request)
        received_message_data = self._soap_client.post_message_to('', message, msg='unsubscribe')
        response_action = received_message_data.action
        # check response: response does not contain explicit status. If action== UnsubscribeResponse all is fine.
        if response_action == EventingActions.UnsubscribeResponse:
            self._logger.info(  # noqa: PLE1205
                'unsubscribe: end of subscription {} was confirmed.',
                self.notification_url,
            )
        elif response_action == Fault.NODETYPE:
            fault = Fault.from_node(received_message_data.p_msg.msg_node)
            self._logger.error(  # noqa: PLE1205
                'unsubscribe: Fault  response : {}',
                fault,
            )

        else:
            self._logger.error(  # noqa: PLE1205
                'unsubscribe: unexpected response action: {}',
                received_message_data.p_msg.raw_data,
            )
            msg = f'unsubscribe: unexpected response action: {received_message_data.p_msg.raw_data}'
            raise ValueError(msg)

    def clear_service(self, epr: str):
        """Clear services."""
        service = self._local_services[epr]
        self._send_bye(service)
        del self._local_services[epr]

    def _send_bye(self, service: Service):
        self._logger.debug('sending bye on %s', service)

        bye = wsd_types.ByeType()
        bye.EndpointReference.Address = service.epr

        inf = HeaderInformationBlock(action=bye.action, addr_to=ADDRESS_ALL)

        app_sequence = wsd_types.AppSequenceType()
        app_sequence.InstanceId = int(service.instance_id)
        app_sequence.MessageNumber = service.message_number

        created_message = _mk_wsd_soap_message(inf, bye)
        created_message.p_msg.add_header_element(
            app_sequence.as_etree_node(nsh.WSD.tag('AppSequence'), ns_map=nsh.partial_map(nsh.WSD)),
        )
        created_message = _mk_wsd_soap_message(inf, bye)
        received_message = self._soap_client.post_message_to('', created_message)  # noqa: F841
        # hello_response = wsd_types.HelloType.from_node(received_message.p_msg.msg_node) # noqa: ERA001
        # print(hello_response)# noqa: ERA001

    def on_post(self, request_data: RequestData) -> CreatedMessage:
        """On post."""
        print('on_post')
        if request_data.message_data.action == wsd_types.HelloType.action:
            hello = wsd_types.HelloType.from_node(request_data.message_data.p_msg.msg_node)
            service = Service(
                types=hello.Types,
                scopes=hello.Scopes,
                x_addrs=hello.XAddrs,
                epr=hello.EndpointReference.Address,
                instance_id='',  # TODO: needed in any way?  # noqa: FIX002, TD002, TD003
                metadata_version=hello.MetadataVersion,
            )
            self._remote_services[service.epr] = service
            self._logger.info('hello epr = %s, xaddrs =%r', service.epr, service.x_addrs)

        elif request_data.message_data.action == wsd_types.ByeType.action:
            bye = wsd_types.ByeType.from_node(request_data.message_data.p_msg.msg_node)
            epr = bye.EndpointReference.Address
            self._logger.info('bye epr = %s, xaddrs =%r', epr, bye.XAddrs)
            if epr in self._remote_services:
                del self._remote_services[epr]
                self._logger.info('removed %s from known remote services')
            else:
                self._logger.info('unknown remote service %s', epr)
        return EmptyResponse()

    def on_get(self, _: RequestData) -> CreatedMessage:
        """On get."""
        print('on_get')
        return EmptyResponse()


if __name__ == '__main__':

    def mk_provider(
        wsd: DiscoProxyClient,
        mdib_path: str,
        uuid_str: str,
        ssl_contexts: SSLContextContainer,
    ) -> SdcProvider:
        """Create sdc provider."""
        my_mdib = ProviderMdib.from_mdib_file(mdib_path)
        print(f'UUID for this device is {uuid_str}')
        dpws_model = ThisModelType(
            manufacturer='sdc11073',
            manufacturer_url='www.sdc11073.com',
            model_name='TestDevice',
            model_number='1.0',
            model_url='www.sdc11073.com/model',
            presentation_url='www.sdc11073.com/model/presentation',
        )

        dpws_device = ThisDeviceType(friendly_name='TestDevice', firmware_version='Version1', serial_number='12345')
        specific_components = None
        return SdcProvider(
            wsd,
            dpws_model,
            dpws_device,
            my_mdib,
            UUID(uuid_str),
            ssl_context_container=ssl_contexts,
            specific_components=specific_components,
            max_subscription_duration=15,
        )

    def log_services(log: LoggerAdapter, the_services: list[Service]):
        """Print the found services."""
        log.info('found %d services:', len(the_services))
        for the_service in the_services:
            log.info('found service: %r', the_service)

    def main():
        """Execute disco proxy."""
        # example code how to use the DiscoProxyClient.
        # It assumes a discovery proxy is reachable on disco_ip address.
        basic_logging_setup()
        logger = get_logger_adapter('sdc.disco.main')
        ca_folder = r'C:\tmp\ORNET_REF_Certificates'
        ssl_passwd = 'dummypass'  # noqa: S105
        disco_ip = '192.168.30.5:33479'
        my_ip = '192.168.30.106'
        my_uuid_str = '12345678-6f55-11ea-9697-123456789bcd'
        mdib_path = os.getenv('ref_mdib') or str(  # noqa:SIM112
            pathlib.Path(__file__).parent.joinpath('mdib_test_sequence_2_v4(temp).xml'),
        )
        ref_fac = os.getenv('ref_fac') or 'r_fac'  # noqa:SIM112
        ref_poc = os.getenv('ref_poc') or 'r_poc'  # noqa:SIM112
        ref_bed = os.getenv('ref_bed') or 'r_bed'  # noqa:SIM112
        loc = SdcLocation(ref_fac, ref_poc, ref_bed)

        ssl_contexts = mk_ssl_contexts_from_folder(
            ca_folder,
            cyphers_file=None,
            private_key='user_private_key_encrypted.pem',
            certificate='user_certificate_root_signed.pem',
            ca_public_key='root_certificate.pem',
            ssl_passwd=ssl_passwd,
        )

        proxy = DiscoProxyClient(disco_ip, my_ip, ssl_contexts)
        proxy.start()
        try:
            services = proxy.search_services()
            log_services(logger, services)

            # now publish a device
            logger.info('location for this device is %s', loc)
            logger.info('start provider...')

            sdc_provider = mk_provider(proxy, mdib_path, my_uuid_str, ssl_contexts)
            sdc_provider.start_all()

            validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
            sdc_provider.set_location(loc, validators)

            services = proxy.search_services()
            log_services(logger, services)
            for service in services:
                result = proxy.send_resolve(service.epr)
                logger.info('resolvematches: %r', result.ResolveMatch)

            time.sleep(5)
            logger.info('stop provider...')
            sdc_provider.stop_all()

            services = proxy.search_services()
            log_services(logger, services)

        finally:
            proxy.stop()

    main()
