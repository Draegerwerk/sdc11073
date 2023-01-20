from __future__ import annotations

from typing import TYPE_CHECKING, List

from .servicesbase import DPWSPortTypeImpl, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .servicesbase import msg_prefix
if TYPE_CHECKING:
    from ...mdib.statecontainers import AbstractStateContainer
    from ...namespaces import NamespaceHelper


class WaveformService(DPWSPortTypeImpl):
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform',
                                                      (f'{msg_prefix}:WaveformStreamReport',)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        _mk_wsdl_one_way_operation(port_type, operation_name='Waveform')

    def _mk_offered_subscriptions(self):
        # unclear if this is needed, it seems wsdl uses Waveform name, action uses WaveformStream
        return  [self._sdc_device.mdib.sdc_definitions.Actions.Waveform]

    def send_realtime_samples_report(self, realtime_sample_states: List[AbstractStateContainer],
                                     nsmapper: NamespaceHelper,
                                     mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.Waveform
        body_node = self._msg_factory.mk_realtime_samples_report_body(mdib_version_group, realtime_sample_states)
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        subscription_mgr.send_to_subscribers(body_node, action, nsmapper, None)
