"""The module contains extensions to the functionality of the EntityConsumerMdib."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073 import observableproperties as properties

if TYPE_CHECKING:
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.pysoap.msgreader import ReceivedMessage

    from .entity_consumermdib import EntityConsumerMdib

class EntityConsumerMdibMethods:
    """Extra methods for consumer mdib that are not part of core functionality."""

    def __init__(self, consumer_mdib: EntityConsumerMdib, logger: LoggerAdapter):
        self._mdib = consumer_mdib
        self._sdc_client = consumer_mdib.sdc_client
        self._msg_reader = self._sdc_client.msg_reader
        self._logger = logger

    def bind_to_client_observables(self):
        """Connect the mdib with the notifications from consumer."""
        properties.bind(self._sdc_client, waveform_report=self._on_waveform_report)
        properties.bind(self._sdc_client, episodic_metric_report=self._on_episodic_metric_report)
        properties.bind(self._sdc_client, episodic_alert_report=self._on_episodic_alert_report)
        properties.bind(self._sdc_client, episodic_context_report=self._on_episodic_context_report)
        properties.bind(self._sdc_client, episodic_component_report=self._on_episodic_component_report)
        properties.bind(self._sdc_client, description_modification_report=self._on_description_modification_report)
        properties.bind(self._sdc_client, episodic_operational_state_report=self._on_operational_state_report)

    def _on_episodic_metric_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_episodic_metric_report')
        self._mdib.process_incoming_metric_states_report(received_message_data)

    def _on_episodic_alert_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_episodic_alert_report')
        self._mdib.process_incoming_alert_states_report(received_message_data)

    def _on_operational_state_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_operational_state_report')
        self._mdib.process_incoming_operational_state_report(received_message_data)

    def _on_waveform_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_waveform_report')
        self._mdib.process_incoming_waveform_states(received_message_data)

    def _on_episodic_context_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_episodic_context_report')
        self._mdib.process_incoming_context_report(received_message_data)

    def _on_episodic_component_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_episodic_component_report')
        self._mdib.process_incoming_component_report(received_message_data)

    def _on_description_modification_report(self, received_message_data: ReceivedMessage):
        self._logger.debug('_on_description_modification_report')
        self._mdib.process_incoming_description_modification_report(received_message_data)


