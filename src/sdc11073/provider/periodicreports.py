"""Implementation of the handlers that send periodic reports of a provider."""

from __future__ import annotations

import threading
import time
from functools import reduce
from typing import TYPE_CHECKING, NamedTuple

from sdc11073 import intervaltimer
from sdc11073.loghelper import get_logger_adapter

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from sdc11073.mdib.mdibbase import MdibVersionGroup
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.mdib.statecontainers import AbstractStateContainer
    from sdc11073.provider.servicesfactory import HostedServices


class PeriodicStates(NamedTuple):
    """A collection of states that were changed with the same mdib version."""

    mdib_version: int
    states: list[AbstractStateContainer]


class _HandlesByType(NamedTuple):
    """Descriptor handles of a single retrievability period, grouped by type of periodic report."""

    metrics: list[str]
    components: list[str]
    alerts: list[str]
    operationals: list[str]
    contexts: list[str]


class _StatesByType(NamedTuple):
    """States of a single retrievability period, grouped by type of periodic report."""

    mdib_version: int
    metrics: list[AbstractStateContainer]
    components: list[AbstractStateContainer]
    alerts: list[AbstractStateContainer]
    operationals: list[AbstractStateContainer]
    contexts: list[AbstractStateContainer]


def _shorter_remaining_time(
    x: tuple[int, intervaltimer.IntervalTimer], y: tuple[int, intervaltimer.IntervalTimer]
) -> tuple[int, intervaltimer.IntervalTimer]:
    """Return the (period, timer) pair whose timer expires first."""
    return x if x[1].remaining_time() < y[1].remaining_time() else y


class PeriodicReportsNullHandler:
    """Handler that does not send any periodic report."""

    def __init__(self):
        """Do nothing."""

    def start(self):
        """Do nothing."""

    def stop(self):
        """Do nothing."""

    def store_metric_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Do nothing."""

    def store_alert_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Do nothing."""

    def store_component_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Do nothing."""

    def store_context_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Do nothing."""

    def store_operational_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Do nothing."""


class PeriodicReportsHandler:
    """Handler that sends periodic reports in a separate thread."""

    def __init__(self, mdib: ProviderMdib, hosted_services: HostedServices, fixed_interval: float | None = None):
        self._periodic_reports_interval = fixed_interval
        self._mdib = mdib
        self._hosted_services = hosted_services
        self._logger = get_logger_adapter('sdc.device.pReports')
        self._periodic_reports_lock = threading.Lock()
        self._periodic_reports_thread = None

        self._periodic_metric_reports = []
        self._periodic_alert_reports = []
        self._periodic_component_state_reports = []
        self._periodic_context_state_reports = []
        self._periodic_operational_state_reports = []
        self._run_periodic_reports_thread = False
        self._timer = None

    def start(self):
        """Start the thread that sends the periodic reports."""
        self._run_periodic_reports_thread = True
        if self._periodic_reports_interval:
            # This setting activates the simple periodic send loop, retrievability settings are ignored
            self._run_periodic_reports_thread = True
            self._periodic_reports_thread = threading.Thread(
                target=self._simple_periodic_reports_send_loop, name='DevPeriodicSendLoop'
            )
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()
        elif self._mdib.retrievability_periodic:
            # Periodic retrievability is set at least once, start handler loop
            self._run_periodic_reports_thread = True
            self._periodic_reports_thread = threading.Thread(
                target=self._periodic_reports_send_loop, name='DevPeriodicSendLoop'
            )
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()

    def stop(self):
        """Stop the thread that sends the periodic reports."""
        self._run_periodic_reports_thread = False

    def store_metric_states(self, mdib_version: int, state_updates: list[AbstractStateContainer]):
        """Add metric states to the next periodic metric report."""
        self._logger.debug('store %d metric states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_metric_reports)

    def store_alert_states(self, mdib_version: int, state_updates: Sequence[AbstractStateContainer]):
        """Add alert states to the next periodic alert report."""
        self._logger.debug('store %d alert states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_alert_reports)

    def store_component_states(self, mdib_version: int, state_updates: Sequence[AbstractStateContainer]):
        """Add component states to the next periodic component state report."""
        self._logger.debug('store %d component states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_component_state_reports)

    def store_context_states(self, mdib_version: int, state_updates: Sequence[AbstractStateContainer]):
        """Add context states to the next periodic context report."""
        self._logger.debug('store %d context states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_context_state_reports)

    def store_operational_states(self, mdib_version: int, state_updates: Sequence[AbstractStateContainer]):
        """Add operational states to the next periodic operational state report."""
        self._logger.debug('store %d operational states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_operational_state_reports)

    def _store_for_periodic_report(
        self, mdib_version: int, state_updates: Iterable[AbstractStateContainer], destination_list: list[PeriodicStates]
    ):
        copied_updates = [s.mk_copy() for s in state_updates]
        with self._periodic_reports_lock:
            destination_list.append(PeriodicStates(mdib_version, copied_updates))

    def _simple_periodic_reports_send_loop(self):
        """Send periodic reports in a very basic way; only a fixed interval is supported.

        It does not care about retrievability settings in the mdib.
        """
        self._logger.debug('_simple_periodic_reports_send_loop start')
        time.sleep(0.1)  # start delayed
        timer = intervaltimer.IntervalTimer(period_in_seconds=self._periodic_reports_interval)
        while self._run_periodic_reports_thread:
            timer.wait_next_interval_begin()
            self._logger.debug('_simple_periodic_reports_send_loop')
            ses = self._hosted_services.state_event_service
            cs = self._hosted_services.context_service
            for reports_list, send_func, msg in [
                (self._periodic_metric_reports, ses.send_periodic_metric_report, 'metric'),
                (self._periodic_alert_reports, ses.send_periodic_alert_report, 'alert'),
                (self._periodic_component_state_reports, ses.send_periodic_component_state_report, 'component'),
                (self._periodic_context_state_reports, cs.send_periodic_context_report, 'context'),
                (self._periodic_operational_state_reports, ses.send_periodic_operational_state_report, 'operational'),
            ]:
                tmp = None
                with self._periodic_reports_lock:
                    if reports_list:
                        tmp = reports_list[:]
                        del reports_list[:]
                if tmp:
                    self._logger.debug('send periodic %s report', msg)
                    send_func(tmp, self._mdib.mdib_version_group)

    def _periodic_reports_send_loop(self):
        """Send periodic reports, considering the retrievability settings in the mdib."""
        self._logger.debug('_periodic_reports_send_loop start')
        time.sleep(0.1)  # start delayed
        # create an interval timer for each period
        timers = {
            period_ms: intervaltimer.IntervalTimer(period_in_seconds=period_ms / 1000)
            for period_ms in self._mdib.retrievability_periodic
        }
        while self._run_periodic_reports_thread:
            # find timer with the shortest remaining time
            period_ms, timer = reduce(_shorter_remaining_time, timers.items())
            timer.wait_next_interval_begin()
            self._logger.debug('_periodic_reports_send_loop {} msec timer', period_ms)  # noqa: PLE1205
            all_handles = self._mdib.retrievability_periodic.get(period_ms, [])
            states = self._collect_states(self._separate_handles_by_type(all_handles))
            self._log_collected_states(states)
            self._send_periodic_reports(states)

    def _separate_handles_by_type(self, all_handles: Iterable[str]) -> _HandlesByType:
        """Group descriptor handles by the type of periodic report they belong to."""
        handles = _HandlesByType([], [], [], [], [])
        for handle in all_handles:
            descr = self._mdib.descriptions.handle.get_one(handle)
            if descr.is_metric_descriptor and not descr.is_realtime_sample_array_metric_descriptor:
                handles.metrics.append(handle)
            elif descr.is_system_context_descriptor or descr.is_component_descriptor:
                handles.components.append(handle)
            elif descr.is_alert_descriptor:
                handles.alerts.append(handle)
            elif descr.is_operational_descriptor:
                handles.operationals.append(handle)
            elif descr.is_context_descriptor:
                handles.contexts.append(handle)
        return handles

    def _collect_states(self, handles: _HandlesByType) -> _StatesByType:
        """Create copies of all states that belong to the provided handles."""
        with self._mdib.mdib_lock:
            context_states = []
            for context in handles.contexts:
                states_of_context = self._mdib.context_states.descriptor_handle.get(context, [])
                context_states.extend([st.mk_copy() for st in states_of_context])
            return _StatesByType(
                self._mdib.mdib_version,
                [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in handles.metrics],
                [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in handles.components],
                [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in handles.alerts],
                [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in handles.operationals],
                context_states,
            )

    def _log_collected_states(self, states: _StatesByType):
        """Log the number of collected states per type of periodic report."""
        for name, state_list in (
            ('metric_states', states.metrics),
            ('component_states', states.components),
            ('alert_states', states.alerts),
            ('operational_states', states.operationals),
            ('context_states', states.contexts),
        ):
            self._logger.debug('   _periodic_reports_send_loop {} {}', len(state_list), name)  # noqa: PLE1205

    def _send_periodic_reports(self, states: _StatesByType):
        """Send one periodic report per type of periodic report that has states."""
        ses = self._hosted_services.state_event_service
        send_functions: tuple[
            tuple[list[AbstractStateContainer], Callable[[list[PeriodicStates], MdibVersionGroup], None]], ...
        ] = (
            (states.metrics, ses.send_periodic_metric_report),
            (states.components, ses.send_periodic_component_state_report),
            (states.alerts, ses.send_periodic_alert_report),
            (states.operationals, ses.send_periodic_operational_state_report),
            (states.contexts, self._hosted_services.context_service.send_periodic_context_report),
        )
        for state_list, send_func in send_functions:
            if state_list:
                send_func([PeriodicStates(states.mdib_version, state_list)], self._mdib.mdib_version_group)
