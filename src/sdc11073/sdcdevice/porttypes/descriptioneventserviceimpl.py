from __future__ import annotations

from typing import TYPE_CHECKING, List

from .porttypebase import DPWSPortTypeBase, WSDLMessageDescription, WSDLOperationBinding, mk_wsdl_one_way_operation
from .porttypebase import msg_prefix

if TYPE_CHECKING:
    from ...mdib.descriptorcontainers import AbstractDescriptorContainer
    from ...mdib.statecontainers import AbstractStateContainer
    from lxml import etree as etree_

class DescriptionEventService(DPWSPortTypeBase):
    WSDLMessageDescriptions = (
        WSDLMessageDescription('DescriptionModificationReport',
                               (f'{msg_prefix}:DescriptionModificationReport',)),
    )
    WSDLOperationBindings = (WSDLOperationBinding('DescriptionModificationReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_one_way_operation(port_type, operation_name='DescriptionModificationReport')

    def send_descriptor_updates(self, updated: List[AbstractDescriptorContainer],
                                created: List[AbstractDescriptorContainer],
                                deleted: List[AbstractDescriptorContainer],
                                updated_states: List[AbstractStateContainer],
                                mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.DescriptionModificationReport
        # body_node = self._msg_factory.mk_description_modification_report_body(
        #     mdib_version_group, updated, created, deleted, updated_states)
        body_node = self.mk_description_modification_report_body(
            mdib_version_group, updated, created, deleted, updated_states)
        self._logger.debug('sending DescriptionModificationReport upd={} crt={} del={}', updated, created, deleted)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, 'send_descriptor_updates')

    def mk_description_modification_report_body(self, mdib_version_group, updated, created, deleted,
                                                updated_states) -> etree_.Element:
        # This method creates one ReportPart for every descriptor.
        # An optimization is possible by grouping all descriptors with the same parent handle into one ReportPart.
        # This is not implemented, and I think it is not needed.
        data_model = self._sdc_definitions.data_model
        report = data_model.msg_types.DescriptionModificationReport()
        report.set_mdib_version_group(mdib_version_group)
        DescriptionModificationType = data_model.msg_types.DescriptionModificationType

        for descriptors, modification_type in ((updated, DescriptionModificationType.UPDATE),
                                               (created, DescriptionModificationType.CREATE),
                                               (deleted, DescriptionModificationType.DELETE)):
            for descriptor in descriptors:
                # one report part for every descriptor,
                report_part = report.add_report_part()
                report_part.ModificationType = modification_type
                report_part.ParentDescriptor = descriptor.parent_handle
                report_part.SourceMds = descriptor.source_mds
                report_part.Descriptor.append(descriptor)
                states = [s for s in updated_states if s.DescriptorHandle == descriptor.Handle]
                report_part.State.extend(states)

        nsh = data_model.ns_helper
        ns_map = nsh.partial_map(nsh.MSG, nsh.PM)
        return report.as_etree_node(data_model.msg_names.DescriptionModificationReport, ns_map)
