from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.namespaces import PrefixesEnum

from .porttypebase import (
    DPWSPortTypeBase,
    WSDLMessageDescription,
    WSDLOperationBinding,
    mk_wsdl_one_way_operation,
    msg_prefix,
)

if TYPE_CHECKING:
    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.mdib.statecontainers import AbstractStateContainer


class WaveformService(DPWSPortTypeBase):
    port_type_name = PrefixesEnum.SDC.tag('WaveformService')
    WSDLMessageDescriptions = (WSDLMessageDescription('Waveform',
                                                      (f'{msg_prefix}:WaveformStreamReport',)),)
    WSDLOperationBindings = (WSDLOperationBinding('Waveform', None, 'literal'),)

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_one_way_operation(port_type, operation_name='Waveform')

    def _mk_offered_subscriptions(self):
        # unclear if this is needed, it seems wsdl uses Waveform name, action uses WaveformStream
        return [self._sdc_device.mdib.sdc_definitions.Actions.Waveform]

    def send_realtime_samples_report(self, realtime_sample_states: list[AbstractStateContainer],
                                     mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.WaveformStream()
        report.set_mdib_version_group(mdib_version_group)
        report.State.extend(realtime_sample_states)
        self._logger.debug('sending real time samples report {}', realtime_sample_states)
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)
