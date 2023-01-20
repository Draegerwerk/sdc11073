from __future__ import annotations

from typing import TYPE_CHECKING, List

from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .servicesbase import msg_prefix

if TYPE_CHECKING:
    from ...mdib.descriptorcontainers import AbstractDescriptorContainer
    from ...mdib.statecontainers import AbstractStateContainer
    from ...namespaces import NamespaceHelper


class DescriptionEventService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('DescriptionModificationReport',
                               (f'{msg_prefix}:DescriptionModificationReport',)),
    )
    WSDLOperationBindings = (WSDLOperationBinding('DescriptionModificationReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='DescriptionModificationReport')

    def send_descriptor_updates(self, updated: List[AbstractDescriptorContainer],
                                created: List[AbstractDescriptorContainer],
                                deleted: List[AbstractDescriptorContainer],
                                updated_states: List[AbstractStateContainer],
                                nsmapper: NamespaceHelper,
                                mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.DescriptionModificationReport
        body_node = self._msg_factory.mk_description_modification_report_body(
            mdib_version_group, updated, created, deleted, updated_states)
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        subscription_mgr.send_to_subscribers(body_node, action, nsmapper, 'send_descriptor_updates')
