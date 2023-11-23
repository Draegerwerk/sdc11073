from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from sdc11073.dispatch import DispatchKey
from sdc11073.namespaces import PrefixesEnum

from .porttypebase import (
    ServiceWithOperations,
    WSDLMessageDescription,
    WSDLOperationBinding,
    mk_wsdl_one_way_operation,
    mk_wsdl_two_way_operation,
    msg_prefix,
)
from .stateeventserviceimpl import fill_episodic_report_body, fill_periodic_report_body

if TYPE_CHECKING:
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from sdc11073.provider.periodicreports import PeriodicStates


class ContextService(ServiceWithOperations):
    port_type_name = PrefixesEnum.SDC.tag('ContextService')
    WSDLMessageDescriptions = (WSDLMessageDescription('SetContextState',
                                                      (f'{msg_prefix}:SetContextState',)),
                               WSDLMessageDescription('SetContextStateResponse',
                                                      (f'{msg_prefix}:SetContextStateResponse',)),
                               WSDLMessageDescription('GetContextStates',
                                                      (f'{msg_prefix}:GetContextStates',)),
                               WSDLMessageDescription('GetContextStatesResponse',
                                                      (f'{msg_prefix}:GetContextStatesResponse',)),
                               WSDLMessageDescription('EpisodicContextReport',
                                                      (f'{msg_prefix}:EpisodicContextReport',)),
                               WSDLMessageDescription('PeriodicContextReport',
                                                      (f'{msg_prefix}:PeriodicContextReport',)),
                               )
    WSDLOperationBindings = (WSDLOperationBinding('SetContextState', 'literal', 'literal'),  # ToDo: generate wsdl:fault
                             WSDLOperationBinding('GetContextStates', 'literal', 'literal'),
                             WSDLOperationBinding('EpisodicContextReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicContextReport', None, 'literal'),
                             )

    def register_hosting_service(self, hosting_service):
        super().register_hosting_service(hosting_service)
        actions = self._mdib.sdc_definitions.Actions
        msg_names = self._mdib.sdc_definitions.data_model.msg_names
        hosting_service.register_post_handler(DispatchKey(actions.SetContextState, msg_names.SetContextState),
                                              self._on_set_context_state)
        hosting_service.register_post_handler(DispatchKey(actions.GetContextStates, msg_names.GetContextStates),
                                              self._on_get_context_states)

    def _on_set_context_state(self, request_data):
        data_model = self._sdc_definitions.data_model
        msg_node = request_data.message_data.p_msg.msg_node
        set_context_state = data_model.msg_types.SetContextState.from_node(msg_node)
        response = data_model.msg_types.SetContextStateResponse()
        return self._handle_operation_request(request_data, set_context_state, response)

    def _on_get_context_states(self, request_data):
        data_model = self._sdc_definitions.data_model
        pm_names = data_model.pm_names
        self._logger.debug('_on_get_context_states')
        msg_node = request_data.message_data.p_msg.msg_node
        get_context_state = data_model.msg_types.GetContextStates.from_node(msg_node)
        requested_handles = get_context_state.HandleRef
        if len(requested_handles) > 0:
            self._logger.info('_on_get_context_states requested Handles:{}', requested_handles)
        with self._mdib.mdib_lock:
            if len(requested_handles) == 0:
                # MessageModel: If the HANDLE reference list is empty, all states in the MDIB SHALL be included in the result list.
                context_state_containers = list(self._mdib.context_states.objects)
            else:
                context_state_containers_lookup = OrderedDict()  # lookup to avoid double entries
                for handle in requested_handles:
                    # If a HANDLE reference does match a multi state HANDLE,
                    # the corresponding multi state SHALL be included in the result list
                    tmp = self._mdib.context_states.handle.get_one(handle, allow_none=True)
                    if tmp:
                        tmp = [tmp]
                    if not tmp:
                        # If a HANDLE reference does match a descriptor HANDLE,
                        # all states that belong to the corresponding descriptor SHALL be included in the result list
                        tmp = self._mdib.context_states.descriptor_handle.get(handle)
                    if not tmp:
                        # R5042: If a HANDLE reference from the msg:GetContextStates/msg:HandleRef list does match an
                        # MDS descriptor, then all context states that are part of this MDS SHALL be included in the result list.
                        descr = self._mdib.descriptions.handle.get_one(handle, allow_none=True)
                        if descr:
                            if pm_names.MdsDescriptor == descr.NODETYPE:
                                tmp = list(self._mdib.context_states.objects)
                    if tmp:
                        for state in tmp:
                            context_state_containers_lookup[state.Handle] = state
                context_state_containers = context_state_containers_lookup.values()

        response = data_model.msg_types.GetContextStatesResponse()
        response.ContextState.extend(context_state_containers)
        response.set_mdib_version_group(self._mdib.mdib_version_group)
        response_envelope = self._sdc_device.msg_factory.mk_reply_soap_message(request_data, response)
        return response_envelope

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_two_way_operation(port_type, operation_name='SetContextState')
        mk_wsdl_two_way_operation(port_type, operation_name='GetContextStates')
        mk_wsdl_one_way_operation(port_type, operation_name='EpisodicContextReport')
        mk_wsdl_one_way_operation(port_type, operation_name='PeriodicContextReport')

    def send_episodic_context_report(self, states: list[AbstractStateContainer],
                                     mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.EpisodicContextReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_episodic_report_body(report, states)
        ns_map = nsh.partial_map(nsh.PM, nsh.MSG, nsh.XSI, nsh.EXT, nsh.XML)
        body_node = report.as_etree_node(report.NODETYPE, ns_map)

        self._logger.debug('sending episodic context report {}', states)
        subscription_mgr.send_to_subscribers(body_node, report.action.value, mdib_version_group)

    def send_periodic_context_report(self, periodic_states_list: list[PeriodicStates],
                                     mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.PeriodicContextReport()
        report.set_mdib_version_group(mdib_version_group)
        actual_mdib_version = periodic_states_list[-1].mdib_version
        report.MdibVersion = actual_mdib_version
        fill_periodic_report_body(report, periodic_states_list)
        self._logger.debug('sending periodic context report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)
