from __future__ import annotations

from typing import TYPE_CHECKING, List

from .porttypebase import DPWSPortTypeBase, WSDLMessageDescription, WSDLOperationBinding, _mk_wsdl_one_way_operation
from .porttypebase import msg_prefix

if TYPE_CHECKING:
    from ...mdib.statecontainers import AbstractStateContainer
    from ..periodicreports import PeriodicStates
    from ...namespaces import NamespaceHelper


class StateEventService(DPWSPortTypeBase):
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
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='SystemErrorReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicAlertReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicComponentReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicOperationalStateReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='PeriodicMetricReport')
        _mk_wsdl_one_way_operation(port_type, operation_name='EpisodicMetricReport')

    def send_episodic_metric_report(self, states: List[AbstractStateContainer],
                                    nsmapper: NamespaceHelper,
                                    mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.EpisodicMetricReport
        body_node = self._msg_factory.mk_episodic_metric_report_body(mdib_version_group, states)
        self._logger.debug('sending episodic metric report {}', states)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_episodic_metric_report')

    def send_periodic_metric_report(self, periodic_states_list: List[PeriodicStates],
                                    nsmapper: NamespaceHelper,
                                    mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.PeriodicMetricReport
        body_node = self._msg_factory.mk_periodic_metric_report_body(
            periodic_states_list[-1].mdib_version, mdib_version_group, periodic_states_list)
        self._logger.debug('sending periodic metric report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_periodic_metric_report')

    def send_episodic_alert_report(self, states: List[AbstractStateContainer],
                                   nsmapper: NamespaceHelper,
                                   mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.EpisodicAlertReport
        body_node = self._msg_factory.mk_episodic_alert_report_body(mdib_version_group, states)
        self._logger.debug('sending episodic alert report {}', states)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_episodic_alert_report')

    def send_periodic_alert_report(self, periodic_states_list: List[PeriodicStates],
                                   nsmapper: NamespaceHelper,
                                   mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.PeriodicAlertReport
        body_node = self._msg_factory.mk_periodic_alert_report_body(
            periodic_states_list[-1].mdib_version, mdib_version_group, periodic_states_list)
        self._logger.debug('sending periodic alert report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_periodic_alert_report')

    def send_episodic_operational_state_report(self, states: List[AbstractStateContainer],
                                               nsmapper: NamespaceHelper,
                                               mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.EpisodicOperationalStateReport
        body_node = self._msg_factory.mk_episodic_operational_state_report_body(mdib_version_group, states)
        self._logger.debug('sending episodic operational state report {}', states)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_episodic_operational_state_report')

    def send_periodic_operational_state_report(self, periodic_states_list: List[PeriodicStates],
                                               nsmapper: NamespaceHelper,
                                               mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.PeriodicOperationalStateReport
        body_node = self._msg_factory.mk_periodic_operational_state_report_body(
            periodic_states_list[-1].mdib_version, mdib_version_group, periodic_states_list)
        self._logger.debug('sending periodic operational state report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_periodic_operational_state_report')

    def send_episodic_component_state_report(self, states: List[AbstractStateContainer],
                                             nsmapper: NamespaceHelper,
                                             mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.EpisodicComponentReport
        body_node = self._msg_factory.mk_episodic_component_state_report_body(mdib_version_group, states)
        self._logger.debug('sending episodic component report {}', states)
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_episodic_component_state_report')

    def send_periodic_component_state_report(self, periodic_states_list: List[PeriodicStates],
                                             nsmapper: NamespaceHelper,
                                             mdib_version_group):
        subscription_mgr = self.hosting_service.subscriptions_manager
        action = self._sdc_definitions.Actions.PeriodicComponentReport
        body_node = self._msg_factory.mk_periodic_component_state_report_body(
            periodic_states_list[-1].mdib_version, mdib_version_group, periodic_states_list)
        self._logger.debug('sending periodic component report, contains last {} episodic updates',
                           len(periodic_states_list))
        subscription_mgr.send_to_subscribers(body_node, action, mdib_version_group, nsmapper, 'send_periodic_component_state_report')
