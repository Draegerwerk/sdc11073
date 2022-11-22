import urllib
import weakref
from concurrent.futures import Future
from typing import Any

from ... import loghelper
from ...namespaces import default_ns_helper  #DocNamespaceHelper
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
    def instance_id(self):
        return self._received_message.instance_id

    @property
    def sequence_id(self):
        return self._received_message.sequence_id

    @property
    def mdib_version(self):
        return self._received_message.mdib_version

    @property
    def action(self):
        return self._received_message.action

    @property
    def msg_name(self):
        return self._received_message.msg_name

    @property
    def result(self):
        return self._result


class HostedServiceClient:
    """ Base class of clients that call hosted services of a dpws device."""
    subscribeable_actions = tuple()

    def __init__(self, soap_client, msg_factory, dpws_hosted, porttype, sdc_definitions, biceps_parser,
                 log_prefix=''):
        """
        :param simple_xml_hosted_node: a "Hosted" node in a simplexml document
        """
        self.endpoint_reference = dpws_hosted.endpoint_references[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self.porttype = porttype
        self._logger = loghelper.get_logger_adapter(f'sdc.client.{porttype}', log_prefix)
        self._operations_manager = None
        self._sdc_definitions = sdc_definitions
        self._biceps_parser = biceps_parser
        self.soap_client = soap_client
        self.log_prefix = log_prefix
        self._mdib_wref = None
        self._msg_factory = msg_factory
        self.predefined_actions = {}  # calculated actions for subscriptions
        self._nsmapper = default_ns_helper  # DocNamespaceHelper()
        for action in self.subscribeable_actions:
            self.predefined_actions[action] = self._msg_factory.get_action_string(porttype, action)

    def register_mdib(self, mdib):
        """ Client sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib_wref is not None:
            raise ApiUsageError(f'Client "{self.porttype}" has already an registered mdib')
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager):
        self._operations_manager = operations_manager

    def _call_operation(self, envelope, request_manipulator=None) -> Future:
        return self._operations_manager.call_operation(self, envelope, request_manipulator)

    def get_subscribable_actions(self):
        """ action strings only predefined"""
        return self.predefined_actions.values()

    def __repr__(self):
        return f'{self.__class__.__name__} "{self.porttype}" endpoint = {self.endpoint_reference}'

    def post_message(self, created_message, msg, request_manipulator=None):
        return self.soap_client.post_message_to(self._url.path, created_message, msg=msg,
                                                request_manipulator=request_manipulator)

    def _call_get_method(self, message, method, request_manipulator=None):
        self._logger.info('calling {} on {}:{}', method, self._url.netloc, self._url.path)
        message_data = self.post_message(message, msg=f'get {method}', request_manipulator=request_manipulator)
        return message_data
