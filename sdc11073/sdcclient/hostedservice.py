import urllib
import weakref

from .. import loghelper
from ..namespaces import DocNamespaceHelper
from ..pysoap.soapenvelope import ExtendedDocumentInvalid


class HostedServiceClient:
    """ Base class of clients that call hosted services of a dpws device."""
    VALIDATE_MEX = False  # workaraound as long as validation error due to missing dpws schema is not solved
    subscribeable_actions = tuple()

    def __init__(self, soap_client, msg_factory, dpws_hosted, porttype, validate, sdc_definitions, biceps_parser,
                 log_prefix=''):
        '''
        @param simple_xml_hosted_node: a "Hosted" node in a simplexml document
        '''
        self.endpoint_reference = dpws_hosted.endpoint_references[0]
        self._url = urllib.parse.urlparse(self.endpoint_reference.address)
        self.porttype = porttype
        self._logger = loghelper.get_logger_adapter('sdc.client.{}'.format(porttype), log_prefix)
        self._operations_manager = None
        self._validate = validate
        self._sdc_definitions = sdc_definitions
        self._biceps_parser = biceps_parser
        self.soap_client = soap_client
        self.log_prefix = log_prefix
        self._mdib_wref = None
        self._msg_factory = msg_factory
        self.predefined_actions = {}  # calculated actions for subscriptions
        for action in self.subscribeable_actions:
            self.predefined_actions[action] = self._msg_factory.get_action_string(porttype, action)

    @property
    def _bmm_schema(self):
        return None if not self._validate else self._biceps_parser.message_schema

    @property
    def _mex_schema(self):
        return None if not self._validate else self._biceps_parser.mex_schema

    def register_mdib(self, mdib):
        ''' Client sometimes must know the mdib data (e.g. Set service, activate method).'''
        if mdib is not None and self._mdib_wref is not None:
            raise RuntimeError('Client "{}" has already an registered mdib'.format(self.porttype))
        self._mdib_wref = None if mdib is None else weakref.ref(mdib)

    def set_operations_manager(self, operations_manager):
        self._operations_manager = operations_manager

    def _call_operation(self, envelope, request_manipulator=None):
        return self._operations_manager.call_operation(self, envelope, request_manipulator)

    def get_subscribable_actions(self):
        """ action strings only predefined"""
        return self.predefined_actions.values()

    def __repr__(self):
        return '{} "{}" endpoint = {}'.format(self.__class__.__name__, self.porttype, self.endpoint_reference)

    def post_soap_envelope(self, envelope, msg, request_manipulator=None):
        return self.soap_client.post_soap_envelope_to(self._url.path, envelope, msg=msg,
                                                      request_manipulator=request_manipulator)

    def _call_get_method(self, envelope, method, request_manipulator=None):
        self._logger.info('calling {} on {}:{}', method, self._url.netloc, self._url.path)
        envelope.validate_body(self._bmm_schema)
        result_envelope = self.post_soap_envelope(envelope, msg='get {}'.format(method),
                                                  request_manipulator=request_manipulator)
        try:
            result_envelope.validate_body(self._bmm_schema)
        except ExtendedDocumentInvalid as ex:
            self._logger.error('Validation error: {}', ex)
        except TypeError as ex:
            self._logger.error('Could not validate Body, Type Error :{}', ex)
        except Exception as ex:
            self._logger.error('Validation error: "{}" msg_node={}', ex, result_envelope.msg_node)
        return result_envelope


class GetServiceClient(HostedServiceClient):

    def get_md_description_node(self, requested_handles=None, request_manipulator=None):
        """
        @param requested_handles: None if all descriptors shall be requested, otherwise a list of handles
        """
        envelope = self._msg_factory.mk_getmddescription_envelope(
            self.endpoint_reference.address, self.porttype, requested_handles)

        result_envelope = self._call_get_method(envelope, 'GetMdDescription',
                                                request_manipulator=request_manipulator)
        return result_envelope.msg_node

    def get_mdib(self, request_manipulator=None):
        envelope = self._msg_factory.mk_getmdib_envelope(self.endpoint_reference.address, self.porttype)

        result_envelope = self._call_get_method(envelope, 'GetMdib', request_manipulator=request_manipulator)
        return result_envelope

    def get_mdib_node(self, request_manipulator=None):
        return self.get_mdib(request_manipulator).msg_node

    def get_md_state(self, requested_handles=None, request_manipulator=None):
        """
        @param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        envelope = self._msg_factory.mk_getmdstate_envelope(self.endpoint_reference.address,
                                                            self.porttype, requested_handles)
        result_envelope = self._call_get_method(envelope, 'GetMdState',
                                                request_manipulator=request_manipulator)
        return result_envelope

    def get_md_state_node(self, requested_handles=None, request_manipulator=None):
        """
        @param requested_handles: None if all states shall be requested, otherwise a list of handles
        """
        return self.get_md_state(requested_handles, request_manipulator=request_manipulator).msg_node


class SetServiceClient(HostedServiceClient):
    subscribeable_actions = ('OperationInvokedReport',)

    def set_numeric_value(self, operation_handle, requested_numeric_value, request_manipulator=None):
        """ call SetNumericValue Method of device
        @param operation_handle: a string
        @param requested_numeric_value: int or float or a string representing a decimal number
        @return a Future object
        """
        self._logger.info('set_numeric_value operation_handle={} requested_numeric_value={}',
                          operation_handle, requested_numeric_value)
        envelope = self._mk_requested_numeric_value_envelope(operation_handle, requested_numeric_value)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_string(self, operation_handle, requested_string, request_manipulator=None):
        """ call SetString Method of device
        @param operation_handle: a string
        @param requested_string: a string
        @return a Future object
        """
        self._logger.info('set_string operation_handle={} requested_string={}',
                          operation_handle, requested_string)
        envelope = self._mk_requested_string_envelope(operation_handle, requested_string)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_alert_state(self, operation_handle, proposed_alert_state, request_manipulator=None):
        """The SetAlertState method corresponds to the SetAlertStateOperation objects in the MDIB and allows the modification of an alert.
        It can handle a single proposed AlertState as argument (only for backwards compatibility) and a list of them.
        @param operation_handle: handle name as string
        @param proposed_alert_state: domainmodel.AbstractAlertState instance or a list of them
        """
        self._logger.info('set_alert_state operation_handle={} requestedAlertState={}',
                          operation_handle, proposed_alert_state)
        if hasattr(proposed_alert_state, 'NODETYPE'):
            # this is a state container. make it a list
            proposed_alert_state = [proposed_alert_state]
        envelope = self._mk_set_alert_envelope(operation_handle, proposed_alert_state)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_metric_state(self, operation_handle, proposed_metric_states, request_manipulator=None):
        """The SetMetricState method corresponds to the SetMetricStateOperation objects in the MDIB and allows the modification of metric states.
        @param operation_handle: handle name as string
        @param proposed_metric_states: a list of domainmodel.AbstractMetricState instance or derived class
        """
        self._logger.info('set_metric_state operation_handle={} requestedMetricState={}',
                          operation_handle, proposed_metric_states)
        envelope = self._mk_set_metric_state_envelope(operation_handle, proposed_metric_states)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def activate(self, operation_handle, value, request_manipulator=None):
        """ an activate call does not return the result of the operation directly. Instead you get an transaction id,
        and will receive the status of this transaction as notification ("OperationInvokedReport").
        This method returns a "future" object. The future object has a result as soon as a final transaction state is received.
        @param operation_handle: a string
        @param value: a string
        @return: a concurrent.futures.Future object
        """
        # make message body
        self._logger.info('activate handle={} value={}', operation_handle, value)
        envelope = self._msg_factory.mk_activate_envelope(self.endpoint_reference.address,
                                                          self.porttype,
                                                          operation_handle,
                                                          value)
        envelope.validate_body(self._bmm_schema)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def set_component_state(self, operation_handle, proposed_component_states, request_manipulator=None):
        """
        The set_component_state method corresponds to the SetComponentStateOperation objects in the MDIB and allows to insert or modify context states.
        @param operation_handle: handle name as string
        @param proposed_component_states: a list of domainmodel.AbstractDeviceComponentState instances or derived class
        :return: a concurrent.futures.Future
        """
        tmp = ', '.join(['{}(descriptorHandle={})'.format(st.__class__.__name__, st.descriptorHandle)
                         for st in proposed_component_states])
        self._logger.info('set_component_state {}', tmp)
        envelope = self._msg_factory.mk_setcomponentstate_envelope(self.endpoint_reference.address, self.porttype,
                                                                   operation_handle, proposed_component_states)
        self._logger.debug('set_component_state sends {}', lambda: envelope.as_xml(pretty=True))
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def _mk_requested_numeric_value_envelope(self, operation_handle, requested_numeric_value):
        """create soap envelope, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_requestednumericvalue_envelope(
            self.endpoint_reference.address, self.porttype, operation_handle, requested_numeric_value)

    def _mk_requested_string_envelope(self, operation_handle, requested_string):
        """create soap envelope, but do not send it. Used for unit testing"""
        return self._msg_factory.mk_requestedstring_envelope(
            self.endpoint_reference.address, self.porttype, operation_handle, requested_string)

    def _mk_set_alert_envelope(self, operation_handle, proposed_alert_states):
        return self._msg_factory.mk_setalert_envelope(
            self.endpoint_reference.address, self.porttype, operation_handle, proposed_alert_states)

    def _mk_set_metric_state_envelope(self, operation_handle, proposed_metric_states):
        """create soap envelope, but do not send it. Used for unit testing
        :param proposedMetricState: a list of AbstractMetricStateContainer or derived classes """
        return self._msg_factory.mk_setmetricstate_envelope(
            self.endpoint_reference.address, self.porttype, operation_handle, proposed_metric_states)


class CTreeServiceClient(HostedServiceClient):

    def get_descriptor_node(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        envelope = self._msg_factory.mk_getdescriptor_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        result_envelope = self._call_get_method(
            envelope, 'GetMdState', request_manipulator=request_manipulator)
        return result_envelope.msg_node

    def get_containment_tree_nodes(self, handles, request_manipulator=None):
        """

        :param handles: a list of strings
        :return: a list of etree nodes
        """
        envelope = self._msg_factory.mk_getcontainmenttree_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        result_envelope = self._call_get_method(
            envelope, 'GetContainmentTree', request_manipulator=request_manipulator)
        return result_envelope.msg_node


class StateEventClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicMetricReport',
                             'EpisodicAlertReport',
                             'EpisodicComponentReport',
                             'EpisodicOperationalStateReport',
                             'PeriodicMetricReport',
                             'PeriodicAlertReport',
                             'PeriodicComponentReport',
                             'PeriodicOperationalStateReport'
                             )


class DescriptionEventClient(HostedServiceClient):
    subscribeable_actions = ('DescriptionModificationReport',)


class ContextServiceClient(HostedServiceClient):
    subscribeable_actions = ('EpisodicContextReport', 'PeriodicContextReport')

    def mk_proposed_context_object(self, descriptor_handle, handle=None):
        """
        Helper method that create a state that can be used in set_context_state operation
        :param descriptor_handle: the descriptor for which a state shall be created or updated
        :param handle: if None, a new object with default values is created (INSERT operation).
                       Otherwise a copy of an existing state with this handle is returned.
        :return: a context state instance
        """
        mdib = self._mdib_wref()
        if mdib is None:
            raise RuntimeError('no mdib information')
        context_descriptor_container = mdib.descriptions.handle.get_one(descriptor_handle)
        if handle is None:
            cls = self._sdc_definitions.sc.get_container_class(context_descriptor_container.STATE_QNAME)
            obj = cls(nsmapper=DocNamespaceHelper(), descriptor_container=context_descriptor_container)
            obj.Handle = descriptor_handle  # this indicates that this is a new context state
        else:
            _obj = mdib.context_states.handle.get_one(handle)
            obj = _obj.mk_copy()
        return obj

    def set_context_state(self, operation_handle, proposed_context_states, request_manipulator=None):
        """
        @return: a concurrent.futures.Future object
        """
        tmp = ', '.join(['{}(descriptorHandle={}, handle={})'.format(st.__class__.__name__,
                                                                     st.descriptorHandle,
                                                                     st.Handle)
                         for st in proposed_context_states])
        self._logger.info('set_context_state {}', tmp)
        envelope = self._msg_factory.mk_setcontextstate_envelope(
            self.endpoint_reference.address, self.porttype, operation_handle, proposed_context_states)
        return self._call_operation(envelope, request_manipulator=request_manipulator)

    def get_context_states_node(self, handles=None, request_manipulator=None):
        """
        @param handles: a list of handles
        """
        envelope = self._msg_factory.mk_getcontextstates_envelope(
            self.endpoint_reference.address, self.porttype, handles)
        result_envelope = self._call_get_method(
            envelope, 'GetContextStates', request_manipulator=request_manipulator)
        result_envelope.validate_body(self._bmm_schema)
        return result_envelope.msg_node

    def get_context_state_by_identification(self, identifications, context_type=None, request_manipulator=None):
        """
        :param identifications: list of identifiers (type: InstanceIdentifier from pmtypes)
        :param context_type: Type to query
        :return:
        """
        envelope = self._msg_factory.mk_getcontextstates_by_identification_envelope(
            self.endpoint_reference.address, self.porttype, identifications)
        result_envelope = self._call_get_method(
            envelope, 'GetContextStatesByIdentification', request_manipulator=request_manipulator)
        result_envelope.validate_body(self._bmm_schema)
        return result_envelope.msg_node


class WaveformClient(HostedServiceClient):
    subscribeable_actions = ('Waveform',)
