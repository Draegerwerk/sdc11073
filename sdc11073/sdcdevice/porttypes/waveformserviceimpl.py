from __future__ import annotations

from typing import TYPE_CHECKING, List

from .porttypebase import DPWSPortTypeBase, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .porttypebase import msg_prefix

if TYPE_CHECKING:
    from ...mdib.statecontainers import AbstractStateContainer
    from ...namespaces import NamespaceHelper


class WaveformService(DPWSPortTypeBase):
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform',
                                                      (f'{msg_prefix}:WaveformStreamReport',)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='Waveform')

    def _mk_offered_subscriptions(self):
        # unclear if this is needed, it seems wsdl uses Waveform name, action uses WaveformStream
        return [self._sdc_device.mdib.sdc_definitions.Actions.Waveform]

    def send_realtime_samples_report(self, realtime_sample_states: List[AbstractStateContainer],
                                     nsmapper: NamespaceHelper,
                                     mdib_version_group):
        data_model = self._sdc_definitions.data_model
        nsh = data_model.ns_helper
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.WaveformStream()
        report.set_mdib_version_group(mdib_version_group)
        report.State.extend(realtime_sample_states)
        ns_map = nsh.partial_map(nsh.PM, nsh.MSG, nsh.XSI, nsh.EXT, nsh.XML)
        body_node =  report.as_etree_node(report.NODETYPE, ns_map)
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        subscription_mgr.send_to_subscribers(body_node, report.action, mdib_version_group, nsmapper, None)
