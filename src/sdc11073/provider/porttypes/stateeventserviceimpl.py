from __future__ import annotations

from collections import defaultdict
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
    from sdc11073.provider.periodicreports import PeriodicStates
    from sdc11073.xml_types.msg_types import SystemErrorReportPart


class StateEventService(DPWSPortTypeBase):
    port_type_name = PrefixesEnum.SDC.tag('StateEventService')
    WSDLMessageDescriptions = (
        WSDLMessageDescription('EpisodicAlertReport',
                               (f'{msg_prefix}:EpisodicAlertReport',)),
        WSDLMessageDescription('SystemErrorReport',
                               (f'{msg_prefix}:SystemErrorReport',)),
        WSDLMessageDescription('PeriodicAlertReport',
                               (f'{msg_prefix}:PeriodicAlertReport',)),
        WSDLMessageDescription('EpisodicComponentReport',
                               (f'{msg_prefix}:EpisodicComponentReport',)),
        WSDLMessageDescription('PeriodicOperationalStateReport',
                               (f'{msg_prefix}:PeriodicOperationalStateReport',)),
        WSDLMessageDescription('PeriodicComponentReport',
                               (f'{msg_prefix}:PeriodicComponentReport',)),
        WSDLMessageDescription('EpisodicOperationalStateReport',
                               (f'{msg_prefix}:EpisodicOperationalStateReport',)),
        WSDLMessageDescription('PeriodicMetricReport',
                               (f'{msg_prefix}:PeriodicMetricReport',)),
        WSDLMessageDescription('EpisodicMetricReport',
                               (f'{msg_prefix}:EpisodicMetricReport',)),
    )

    WSDLOperationBindings = (WSDLOperationBinding('EpisodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('SystemErrorReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicAlertReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicComponentReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicOperationalStateReport', None, 'literal'),
                             WSDLOperationBinding('PeriodicMetricReport', None, 'literal'),
                             WSDLOperationBinding('EpisodicMetricReport', None, 'literal'),
                             )

    def add_wsdl_port_type(self, parent_node):
        port_type = self._mk_port_type_node(parent_node, True)
        mk_wsdl_one_way_operation(port_type, operation_name='EpisodicAlertReport')
        mk_wsdl_one_way_operation(port_type, operation_name='SystemErrorReport')
        mk_wsdl_one_way_operation(port_type, operation_name='PeriodicAlertReport')
        mk_wsdl_one_way_operation(port_type, operation_name='EpisodicComponentReport')
        mk_wsdl_one_way_operation(port_type, operation_name='PeriodicOperationalStateReport')
        mk_wsdl_one_way_operation(port_type, operation_name='PeriodicComponentReport')
        mk_wsdl_one_way_operation(port_type, operation_name='EpisodicOperationalStateReport')
        mk_wsdl_one_way_operation(port_type, operation_name='PeriodicMetricReport')
        mk_wsdl_one_way_operation(port_type, operation_name='EpisodicMetricReport')

    def send_episodic_metric_report(self, states: list[AbstractStateContainer],
                                    mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.EpisodicMetricReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_episodic_report_body(report, states)
        self._logger.debug('sending episodic metric report {}', states)
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_periodic_metric_report(self, periodic_states_list: list[PeriodicStates],
                                    mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.PeriodicMetricReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_periodic_report_body(report, periodic_states_list)
        self._logger.debug('sending periodic metric report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_episodic_alert_report(self, states: list[AbstractStateContainer],
                                   mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.EpisodicAlertReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_episodic_report_body(report, states)
        self._logger.debug('sending episodic alert report {}', states)
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_periodic_alert_report(self, periodic_states_list: list[PeriodicStates],
                                   mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.PeriodicAlertReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_periodic_report_body(report, periodic_states_list)
        self._logger.debug('sending periodic alert report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_episodic_operational_state_report(self, states: list[AbstractStateContainer],
                                               mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.EpisodicOperationalStateReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_episodic_report_body(report, states)
        self._logger.debug('sending episodic operational state report {}', states)
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_periodic_operational_state_report(self, periodic_states_list: list[PeriodicStates],
                                               mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.PeriodicOperationalStateReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_periodic_report_body(report, periodic_states_list)
        self._logger.debug('sending periodic operational state report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_episodic_component_state_report(self, states: list[AbstractStateContainer],
                                             mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.EpisodicComponentReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_episodic_report_body(report, states)
        self._logger.debug('sending episodic component report {}', states)
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_periodic_component_state_report(self, periodic_states_list: list[PeriodicStates],
                                             mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.PeriodicComponentReport()
        report.set_mdib_version_group(mdib_version_group)
        fill_periodic_report_body(report, periodic_states_list)
        self._logger.debug('sending periodic component report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

    def send_system_error_report(self, report_parts: list[SystemErrorReportPart],
                                 mdib_version_group: MdibVersionGroup):
        data_model = self._sdc_definitions.data_model
        subscription_mgr = self.hosting_service.subscriptions_manager
        report = data_model.msg_types.SystemErrorReport()
        report.ReportPart.extend(report_parts)
        report.set_mdib_version_group(mdib_version_group)
        self._logger.debug('sending SystemErrorReport')
        subscription_mgr.send_to_subscribers(report, report.action.value, mdib_version_group)

def fill_episodic_report_body(report, states):
    """Helper that splits states list into separate lists per source mds and adds them to report accordingly."""
    lookup = _separate_states_by_source_mds(states)
    for source_mds_handle, states in lookup.items():
        report_part = report.add_report_part()
        report_part.SourceMds = source_mds_handle
        report_part.values_list.extend(states)


def fill_periodic_report_body(report, report_parts):
    for tmp in report_parts:
        lookup = _separate_states_by_source_mds(tmp.states)
        for source_mds_handle, states in lookup.items():
            report_part = report.add_report_part()
            report_part.SourceMds = source_mds_handle
            report_part.values_list.extend(states)


def _separate_states_by_source_mds(states) -> dict:
    lookup = defaultdict(list)
    for state in states:
        lookup[state.source_mds].append(state)
    if None in lookup:
        raise ValueError(f'States {[st.DescriptorHandle for st in lookup[None]]} have no source mds')
    return lookup
