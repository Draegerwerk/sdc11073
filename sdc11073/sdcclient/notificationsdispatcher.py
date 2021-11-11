

class NotificationsDispatcherBase:
    def __init__(self, sdc_client, logger):
        self._sdc_client = sdc_client
        self._logger = logger
        self._lookup = self._mk_lookup()

    def _mk_lookup(self):
        raise NotImplementedError

    def on_notification(self, message_data):
        """

        :param message_data: ReceivedMessageData instance
        :return: None
        """
        self._sdc_client.state_event_report = message_data  # update observable

    def _on_operation_invoked_report(self, message_data):
        self._sdc_client.operation_invoked_report = message_data  # update observable

    def _on_waveform_report(self, message_data):
        self._sdc_client.waveform_report = message_data  # update observable

    def _on_episodic_metric_report(self, message_data):
        self._sdc_client.episodic_metric_report = message_data

    def _on_periodic_metric_report(self, message_data):
        self._sdc_client.periodic_metric_report = message_data

    def _on_episodic_alert_report(self, message_data):
        self._sdc_client.episodic_alert_report = message_data

    def _on_periodic_alert_report(self, message_data):
        self._sdc_client.periodic_alert_report = message_data

    def _on_episodic_component_report(self, message_data):
        self._sdc_client.episodic_component_report = message_data

    def _on_periodic_component_report(self, message_data):
        self._sdc_client.periodic_component_report = message_data

    def _on_episodic_operational_state_report(self, message_data):
        self._sdc_client.episodic_operational_state_report = message_data

    def _on_periodic_operational_state_report(self, message_data):
        self._sdc_client.periodic_operational_state_report = message_data

    def _on_episodic_context_report(self, message_data):
        self._sdc_client.episodic_context_report = message_data

    def _on_periodic_context_report(self, message_data):
        self._sdc_client.periodic_context_report = message_data

    def _on_description_report(self, message_data):
        self._sdc_client.description_modification_report = message_data


class NotificationsDispatcherByBody(NotificationsDispatcherBase):
    def _mk_lookup(self):
        return {
            'EpisodicMetricReport': self._on_episodic_metric_report,
            'EpisodicAlertReport': self._on_episodic_alert_report,
            'EpisodicComponentReport': self._on_episodic_component_report,
            'EpisodicOperationalStateReport': self._on_episodic_operational_state_report,
            'WaveformStream': self._on_waveform_report,
            'OperationInvokedReport': self._on_operation_invoked_report,
            'EpisodicContextReport': self._on_episodic_context_report,
            'DescriptionModificationReport': self._on_description_report,
            'PeriodicMetricReport': self._on_periodic_metric_report,
            'PeriodicAlertReport': self._on_periodic_alert_report,
            'PeriodicComponentReport': self._on_periodic_component_report,
            'PeriodicOperationalStateReport': self._on_periodic_operational_state_report,
            'PeriodicContextReport': self._on_periodic_context_report,
        }

    def on_notification(self, message_data):
        """ dispatch by message body"""
        super().on_notification(message_data)
        method = self._lookup.get(message_data.msg_name)
        if method is None:
            raise RuntimeError(f'unknown message {message_data.msg_name}')
        method(message_data)


class NotificationsDispatcherByAction(NotificationsDispatcherBase):
    def _mk_lookup(self):
        actions = self._sdc_client.sdc_definitions.Actions

        return {
            actions.EpisodicMetricReport: self._on_episodic_metric_report,
            actions.EpisodicAlertReport: self._on_episodic_alert_report,
            actions.EpisodicComponentReport: self._on_episodic_component_report,
            actions.EpisodicOperationalStateReport: self._on_episodic_operational_state_report,
            actions.Waveform: self._on_waveform_report,
            actions.OperationInvokedReport: self._on_operation_invoked_report,
            actions.EpisodicContextReport: self._on_episodic_context_report,
            actions.DescriptionModificationReport: self._on_description_report,
            actions.PeriodicMetricReport: self._on_periodic_metric_report,
            actions.PeriodicAlertReport: self._on_periodic_alert_report,
            actions.PeriodicComponentReport: self._on_periodic_component_report,
            actions.PeriodicOperationalStateReport: self._on_periodic_operational_state_report,
            actions.PeriodicContextReport: self._on_periodic_context_report,
        }

    def on_notification(self, message_data):
        """ dispatch by message body"""
        super().on_notification(message_data)
        action = message_data.address.action
        method = self._lookup.get(action)
        if method is None:
            raise RuntimeError(f'unknown message {action}')
        method(message_data)

