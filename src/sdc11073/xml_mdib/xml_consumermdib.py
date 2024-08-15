from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock, Thread
from typing import TYPE_CHECKING, Any, Callable

from sdc11073 import loghelper
from sdc11073 import observableproperties as properties
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.consumermdib import ConsumerMdibState
from sdc11073.xml_types import msg_qnames
from .xml_consumermdibxtra import XmlConsumerMdibMethods
from .xml_mdibbase import XmlMdibBase

if TYPE_CHECKING:
    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.pysoap.msgreader import ReceivedMessage


@dataclass
class _BufferedData:
    received_message_data: ReceivedMessage
    handler: callable


class XmlConsumerMdib(XmlMdibBase):
    # sequence_or_instance_id_changed_event is set to True every time the sequence id changes.
    # It is not reset to False any time later.
    # It is in the responsibility of the application to react on a changed sequence id.
    # Observe this property and call "reload_all" in the observer code.
    sequence_or_instance_id_changed_event: bool = properties.ObservableProperty(
        default_value=False, fire_only_on_changed_value=False)

    def __init__(self,
                 sdc_client: SdcConsumer,
                 extras_cls: type | None = None,
                 max_realtime_samples: int = 100):
        """Construct a ConsumerMdib instance.

        :param sdc_client: a SdcConsumer instance
        :param  extras_cls: extended functionality
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(sdc_client.sdc_definitions,
                         loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix))
        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = XmlConsumerMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self._state = ConsumerMdibState.invalid
        self.rt_buffers = {}  # key  is a handle, value is a ConsumerRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @property
    def sdc_client(self) -> SdcConsumer:
        """Give access to sdc client."""
        return self._sdc_client

    @property
    def is_initialized(self) -> bool:
        """Returns True if everything has been set up completely."""
        return self._state == ConsumerMdibState.initialized

    def init_mdib(self):
        """Binds own notification handlers to observables of sdc client and calls GetMdib.

        Client mdib is initialized from GetMdibResponse, and from then on updated from incoming notifications.
        :return: None
        """
        if self.is_initialized:
            raise ApiUsageError('ConsumerMdib is already initialized')
        # first start receiving notifications, then call get_mdib.
        # Otherwise, we might miss notifications.
        self._xtra.bind_to_client_observables()
        self.reload_all()
        self._sdc_client.set_mdib(self)  # pylint: disable=protected-access
        self._logger.info('initializing mdib done')

    def reload_all(self):
        """Delete all data and reloads everything."""
        self._logger.info('reload_all called')
        with self.mdib_lock:
            self._state = ConsumerMdibState.initializing  # notifications are now buffered
            self._mdib_node = None
            # self.descriptions.clear()
            # self.clear_states()
            self.sequence_id = None
            self.instance_id = None
            self.mdib_version = None

            get_service = self._sdc_client.client('Get')
            self._logger.info('initializing mdib...')
            response = get_service.get_mdib()
            self._set_root_node(response.p_msg.msg_node)

            mdib_version_group = response.mdib_version_group
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to {}',
                              mdib_version_group.mdib_version)  # noqa: PLE1205
            self.sequence_id = mdib_version_group.sequence_id
            self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)  # noqa: PLE1205
            if mdib_version_group.instance_id != self.instance_id:
                self.instance_id = mdib_version_group.instance_id
            self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)  # noqa: PLE1205

            # # retrieve context states only if there were none in mdib
            # if len(self.context_states.objects) == 0:
            #     self._get_context_states()
            # else:
            #     self._logger.info('found context states in GetMdib Result, will not call getContextStates')

            # process buffered notifications
            with self._buffered_notifications_lock:
                self._logger.debug('got _buffered_notifications_lock')
                for buffered_report in self._buffered_notifications:
                    # buffered data might contain notifications that do not fit.
                    mvg = buffered_report.received_message_data.mdib_version_group
                    if mvg.sequence_id != self.sequence_id:
                        self.logger.debug('wrong sequence id "%s"; ignore buffered report',
                                          mvg.sequence_id)
                        continue
                    if mvg.mdib_version <= self.mdib_version:
                        self.logger.debug('older mdib version "%d"; ignore buffered report',
                                          mvg.mdib_version)
                        continue
                    buffered_report.handler(buffered_report.received_message_data)
                del self._buffered_notifications[:]
                self._state = ConsumerMdibState.initialized
            self._logger.info('reload_all done')

    def process_incoming_metric_states_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_metric_states_report(received_message_data)

    def _process_incoming_metric_states_report(self, received_message: ReceivedMessage):
        """Check mdib version, if okay:
         - update mdib.
         - update observable
        Call this method only if mdib_lock is already acquired."""
        for report_part in received_message.p_msg.msg_node:
            for state in report_part:
                if state.tag == msg_qnames.MetricState:
                    self._update_state(state)

    def process_incoming_alert_states_report(self, received_message_data: ReceivedMessage):
        """Check mdib_version_group and process report it if okay."""
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_metric_states_report):
            return
        with self.mdib_lock:
            self._process_incoming_alert_states_report(received_message_data)

    def _process_incoming_alert_states_report(self, received_message: ReceivedMessage):
        """Check mdib version, if okay:
         - update mdib.
         - update observable
        Call this method only if mdib_lock is already acquired."""
        for report_part in received_message.p_msg.msg_node:
            for state in report_part:
                if state.tag == msg_qnames.MetricState:
                    self._update_state(state)

    def process_incoming_waveform_states(self, received_message_data: ReceivedMessage):
        if not self._pre_check_report_ok(received_message_data,
                                         self._process_incoming_waveform_states):
            return
        with self.mdib_lock:
            self._process_incoming_waveform_states(received_message_data)

    def _process_incoming_waveform_states(self, received_message: ReceivedMessage):
        for state in received_message.p_msg.msg_node:
            self._update_state(state)

    def _update_state(self, state_node):
        """Replace state in DOM tree and entity"""
        entity = self._entities[state_node.attrib['DescriptorHandle']]
        parent = entity.state.getparent()
        parent.replace(entity.state, state_node)
        entity.state = state_node


    def _pre_check_report_ok(self, received_message_data: ReceivedMessage,
                             handler: Callable) -> bool:
        """Check if the report can be added to mdib.

        The pre-check runs before the mdib lock is acquired.
        The report is buffered if state is 'initializing' and 'is_buffered_report' is False.
        :return: True if report can be added to mdib.
        """
        self._check_sequence_or_instance_id_changed(
            received_message_data.mdib_version_group)  # this might change self._state
        if self._state == ConsumerMdibState.invalid:
            # ignore report in these states
            print('_pre_check_report_ok: invalid')
            return False
        if self._state == ConsumerMdibState.initializing:
            print('_pre_check_report_ok: buffering')
            with self._buffered_notifications_lock:
                # check state again, it might have changed before lock was acquired
                if self._state == ConsumerMdibState.initializing:
                    self._buffered_notifications.append(_BufferedData(received_message_data, handler))
                    return False
        print('_pre_check_report_ok: ok')
        return True

    def _check_sequence_or_instance_id_changed(self, mdib_version_group):
        """Check if sequence id and instance id are still the same.

        If not,
        - set state member to invalid
        - set the observable "sequence_or_instance_id_changed_event" in a thread.
          This allows to implement an observer that can directly call reload_all without blocking the consumer."""
        if mdib_version_group.sequence_id == self.sequence_id and mdib_version_group.instance_id == self.instance_id:
            return
        if self._state == ConsumerMdibState.initialized:
            if mdib_version_group.sequence_id != self.sequence_id:
                self.logger.warning('sequence id changed from "%s" to "%s"',
                                    self.sequence_id, mdib_version_group.sequence_id)
            if mdib_version_group.instance_id != self.instance_id:
                self.logger.warning('instance id changed from "%r" to "%r"',
                                    self.instance_id, mdib_version_group.instance_id)
            self.logger.warning('mdib is no longer valid!')

            self._state = ConsumerMdibState.invalid

            def _set_observable():
                self.sequence_or_instance_id_changed_event = True

            thr = Thread(target=_set_observable)
            thr.start()
