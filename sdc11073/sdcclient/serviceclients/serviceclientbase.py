import urllib
import weakref
from concurrent.futures import Future
from typing import Any

from ... import loghelper
from ...namespaces import DocNamespaceHelper
from ...pysoap.msgreader import ReceivedMessageData
from ...pysoap.soapenvelope import ExtendedDocumentInvalid


class GetRequestResult:
    """Like ReceivedMessageData, but plus result (StateContainers, DescriptorContainers, ...)"""

    def __init__(self, message_data: ReceivedMessageData, result: Any):
        self._message_data = message_data
        self._result = result

    @property
    def msg_reader(self):
        return self._message_data.msg_reader

    @property
    def p_msg(self):
        return self._message_data.p_msg

    @property
    def instance_id(self):
        return self._message_data.instance_id

    @property
    def sequence_id(self):
        return self._message_data.sequence_id

    @property
    def mdib_version(self):
        return self._message_data.mdib_version

    @property
    def action(self):
        return self._message_data.action

    @property
    def msg_name(self):
        return self._message_data.msg_name

    @property
    def result(self):
        return self._result


class HostedServiceClient:
    """ Base class of clients that call hosted services of a dpws device."""
    VALIDATE_MEX = False  # workaraound as long as validation error due to missing dpws schema is not solved
    subscribeable_actions = tuple()

    def __init__(self, soap_client, msg_factory, dpws_hosted, porttype, validate, sdc_definitions, biceps_parser,
                 log_prefix=''):
        """
        :param simple_xml_hosted_node: a "Hosted" node in a simplexml document
        """
        self.endpoint_reference = dpws_hosted.endpoint_references[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self.porttype = porttype
        self._logger = loghelper.get_logger_adapter(f'sdc.client.{porttype}', log_prefix)
        self._operations_manager = None
        self._validate = validate
        self._sdc_definitions = sdc_definitions
        self._biceps_parser = biceps_parser
        self.soap_client = soap_client
        self.log_prefix = log_prefix
        self._mdib_wref = None
        self._msg_factory = msg_factory
        self.predefined_actions = {}  # calculated actions for subscriptions
        self._nsmapper = DocNamespaceHelper()
        for action in self.subscribeable_actions:
            self.predefined_actions[action] = self._msg_factory.get_action_string(porttype, action)

    @property
    def _bmm_schema(self):
        return None if not self._validate else self._biceps_parser.message_schema

    @property
    def _mex_schema(self):
        return None if not self._validate else self._biceps_parser.mex_schema

    def register_mdib(self, mdib):
        """ Client sometimes must know the mdib data (e.g. Set service, activate method)."""
        if mdib is not None and self._mdib_wref is not None:
            raise RuntimeError(f'Client "{self.porttype}" has already an registered mdib')
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
        message.validate_body(self._bmm_schema)
        message_data = self.post_message(message, msg=f'get {method}', request_manipulator=request_manipulator)
        try:
            message_data.p_msg.validate_body(self._bmm_schema)
        except ExtendedDocumentInvalid as ex:
            self._logger.error('Validation error: {}', ex)
        except TypeError as ex:
            self._logger.error('Could not validate Body, Type Error :{}', ex)
        except Exception as ex:
            self._logger.error('Validation error: "{}" msg_node={}', ex, message_data.p_msg.msg_node)
        return message_data
