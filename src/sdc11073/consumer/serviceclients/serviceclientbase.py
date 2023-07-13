from __future__ import annotations

import weakref
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from sdc11073 import loghelper
from sdc11073.exceptions import ApiUsageError

if TYPE_CHECKING:
    from concurrent.futures import Future

    from lxml.etree import QName

    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.consumer.manipulator import RequestManipulatorProtocol
    from sdc11073.consumer.operations import OperationsManagerProtocol
    from sdc11073.dispatch import DispatchKey
    from sdc11073.mdib.consumermdib import ConsumerMdib
    from sdc11073.namespaces import PrefixNamespace
    from sdc11073.pysoap.msgfactory import CreatedMessage
    from sdc11073.pysoap.msgreader import MdibVersionGroupReader, MessageReader, ReceivedMessage
    from sdc11073.pysoap.soapclient import SoapClientProtocol
    from sdc11073.pysoap.soapenvelope import ReceivedSoapMessage
    from sdc11073.xml_types.addressing_types import EndpointReferenceType
    from sdc11073.xml_types.basetypes import MessageType
    from sdc11073.xml_types.mex_types import HostedServiceType


class GetRequestResult:
    """Like ReceivedMessage, but plus result (StateContainers, DescriptorContainers, ...)."""

    def __init__(self, received_message: ReceivedMessage, result: MessageType):
        self._received_message = received_message
        self._result = result

    @property
    def msg_reader(self) -> MessageReader:
        """Return the responsible message reader."""
        return self._received_message.msg_reader

    @property
    def p_msg(self) -> ReceivedSoapMessage:
        """Return the received ReceivedSoapMessage."""
        return self._received_message.p_msg

    @property
    def mdib_version_group(self) -> MdibVersionGroupReader:
        """Return the mdib version group of the result."""
        return self._received_message.mdib_version_group

    @property
    def action(self) -> str | None:
        """Return the action of the result."""
        return self._received_message.action

    @property
    def msg_name(self) -> QName:
        """Return the QName of message body."""
        return self._received_message.q_name.localname

    @property
    def result(self) -> MessageType:
        """Return the converted message body."""
        return self._result


class HostedServiceClient:
    """Base class of clients that call hosted services of a dpws device."""

    additional_namespaces: tuple[PrefixNamespace] = ()  # for special namespaces
    # notifications is a list of notifications that a HostedServiceClient handles (for dispatching of subscribed data).
    # Derived classes will set this class variable accordingly:
    notifications: tuple[DispatchKey] = tuple()

    def __init__(self, sdc_consumer: SdcConsumer,
                 soap_client: SoapClientProtocol,
                 dpws_hosted: HostedServiceType,
                 port_type: QName):
        """Construct a HostedServiceClient.

        :param sdc_consumer:
        :param soap_client:
        :param dpws_hosted:
        :param port_type:
        """
        self.soap_client = soap_client
        self._sdc_client = sdc_consumer
        self._sdc_definitions = sdc_consumer.sdc_definitions
        self._msg_factory = sdc_consumer.msg_factory
        self.log_prefix = sdc_consumer.log_prefix
        self.dpws_hosted: HostedServiceType = dpws_hosted
        self.endpoint_reference: EndpointReferenceType = dpws_hosted.EndpointReference[0]
        self._url = urlparse(self.endpoint_reference.Address)
        self._porttype = port_type
        self._logger = loghelper.get_logger_adapter(f'sdc.client.{port_type}', self.log_prefix)
        self._operations_manager = None
        self._mdib_wref = None
        ns_helper = self._sdc_definitions.data_model.ns_helper
        self._nsmapper = ns_helper

    def register_mdib(self, mdib: ConsumerMdib | None):
        """Client sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib_wref is not None:
            raise ApiUsageError(f'Client "{self._porttype}" has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager: OperationsManagerProtocol):
        """Set the operations manager."""
        self._operations_manager = operations_manager

    def _call_operation(self, message: ReceivedMessage,
                        request_manipulator: RequestManipulatorProtocol | None = None) -> Future:
        return self._operations_manager.call_operation(self, message, request_manipulator)

    def get_available_subscriptions(self) -> tuple[DispatchKey]:
        """Return the notifications that a service offers.

        Each returned DispatchKey contains the action for the subscription and the message type that corresponds to it.
        """
        return self.notifications

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} "{self._porttype}" endpoint = {self.endpoint_reference}'

    def post_message(self, created_message: CreatedMessage,
                     msg: str | None = None,
                     request_manipulator: RequestManipulatorProtocol | None = None,
                     validate: bool = True) -> ReceivedMessage:
        """Post the created message to provider."""
        msg = msg or created_message.p_msg.payload_element.tag.split('}')[-1]

        response = self.soap_client.post_message_to(self._url.path, created_message, msg=msg,
                                                    request_manipulator=request_manipulator,
                                                    validate=validate)
        if response is None:
            raise ValueError('expect a response, got None')
        return response
