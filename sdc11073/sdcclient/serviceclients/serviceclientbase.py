import urllib
import weakref
from concurrent.futures import Future
from typing import Any

from ... import loghelper
from ...namespaces import default_ns_helper
from ...exceptions import ApiUsageError
from ...pysoap.msgreader import ReceivedMessage


class GetRequestResult:
    """Like ReceivedMessage, but plus result (StateContainers, DescriptorContainers, ...)"""

    def __init__(self, received_message: ReceivedMessage, result: Any):
        self._received_message = received_message
        self._result = result

    @property
    def msg_reader(self):
        return self._received_message.msg_reader

    @property
    def p_msg(self):
        return self._received_message.p_msg

    @property
    def mdib_version_group(self):
        return self._received_message.mdib_version_group

    @property
    def action(self):
        return self._received_message.action

    @property
    def msg_name(self):
        return self._received_message.q_name.localname

    @property
    def result(self):
        return self._result


class HostedServiceClient:
    """ Base class of clients that call hosted services of a dpws device."""
    subscribeable_actions = tuple()

    def __init__(self, sdc_client, soap_client, dpws_hosted, port_type):
        """

        :param sdc_client:
        :param soap_client:
        :param dpws_hosted:
        :param port_type:
        """
        self.soap_client = soap_client
        self._sdc_client = sdc_client
        self._sdc_definitions = sdc_client.sdc_definitions
        self._msg_factory = sdc_client._msg_factory
        self.log_prefix = sdc_client.log_prefix
        self.endpoint_reference = dpws_hosted.endpoint_references[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self._porttype = port_type
        self._logger = loghelper.get_logger_adapter(f'sdc.client.{port_type}', self.log_prefix)
        self._operations_manager = None
        self._mdib_wref = None
        self.supported_actions = [getattr(self._sdc_definitions.Actions, action) for action in self.subscribeable_actions]
        self._nsmapper = default_ns_helper

    def register_mdib(self, mdib):
        """ Client sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib_wref is not None:
            raise ApiUsageError(f'Client "{self._porttype}" has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager):
        self._operations_manager = operations_manager

    def _call_operation(self, envelope, request_manipulator=None) -> Future:
        return self._operations_manager.call_operation(self, envelope, request_manipulator)

    def get_subscribable_actions(self):
        """ action strings only predefined"""
        return self.supported_actions

    def __repr__(self):
        return f'{self.__class__.__name__} "{self._porttype}" endpoint = {self.endpoint_reference}'

    def post_message(self, created_message, msg=None, request_manipulator=None):
        msg = msg or created_message.p_msg.payload_element.tag.split('}')[-1]

        return self.soap_client.post_message_to(self._url.path, created_message, msg=msg,
                                                request_manipulator=request_manipulator)

