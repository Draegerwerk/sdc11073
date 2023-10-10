import threading
import time
from collections import namedtuple
from functools import reduce

from sdc11073 import intervaltimer
from sdc11073.loghelper import get_logger_adapter

PeriodicStates = namedtuple('PeriodicStates', 'mdib_version states')


class PeriodicReportsNullHandler:
    def __init__(self):
        """Do nothing"""

    def start(self):
        """Do nothing"""

    def stop(self):
        """Do nothing"""

    def store_metric_states(self, mdib_version, state_updates):
        """Do nothing"""

    def store_alert_states(self, mdib_version, state_updates):
        """Do nothing"""

    def store_component_states(self, mdib_version, state_updates):
        """Do nothing"""

    def store_context_states(self, mdib_version, state_updates):
        """Do nothing"""

    def store_operational_states(self, mdib_version, state_updates):
        """Do nothing"""


class PeriodicReportsHandler:
    def __init__(self, mdib, hosted_services, fixed_interval=None):
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
        self._run_periodic_reports_thread = True
        if self._periodic_reports_interval:
            # This setting activates the simple periodic send loop, retrievability settings are ignored
            self._run_periodic_reports_thread = True
            self._periodic_reports_thread = threading.Thread(target=self._simple_periodic_reports_send_loop,
                                                             name='DevPeriodicSendLoop')
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()
        elif self._mdib.retrievability_periodic:
            # Periodic retrievability is set at least once, start handler loop
            self._run_periodic_reports_thread = True
            self._periodic_reports_thread = threading.Thread(target=self._periodic_reports_send_loop,
                                                             name='DevPeriodicSendLoop')
            self._periodic_reports_thread.daemon = True
            self._periodic_reports_thread.start()

    def stop(self):
        self._run_periodic_reports_thread = False

    def store_metric_states(self, mdib_version, state_updates):
        self._logger.debug('store %d metric states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_metric_reports)

    def store_alert_states(self, mdib_version, state_updates):
        self._logger.debug('store %d alert states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_alert_reports)

    def store_component_states(self, mdib_version, state_updates):
        self._logger.debug('store %d component states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_component_state_reports)

    def store_context_states(self, mdib_version, state_updates):
        self._logger.debug('store %d context states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_context_state_reports)

    def store_operational_states(self, mdib_version, state_updates):
        self._logger.debug('store %d operational states', len(state_updates))
        self._store_for_periodic_report(mdib_version, state_updates, self._periodic_operational_state_reports)

    def _store_for_periodic_report(self, mdib_version, state_updates, destination_list):
        copied_updates = [s.mk_copy() for s in state_updates]
        with self._periodic_reports_lock:
            destination_list.append(PeriodicStates(mdib_version, copied_updates))

    def _simple_periodic_reports_send_loop(self):
        """This is a very basic implementation of periodic reports, it only supports fixed interval.
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
            for reports_list, send_func, msg in \
                    [(self._periodic_metric_reports, ses.send_periodic_metric_report, 'metric'),
                     (self._periodic_alert_reports, ses.send_periodic_alert_report, 'alert'),
                     (self._periodic_component_state_reports, ses.send_periodic_component_state_report, 'component'),
                     (self._periodic_context_state_reports, cs.send_periodic_context_report, 'context'),
                     (self._periodic_operational_state_reports, ses.send_periodic_operational_state_report,
                      'operational'),
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
        """This implementation of periodic reports send loop considers retrievability settings in the mdib.
        """

        # helper for reduce
        def _next(x, y):  # pylint: disable=invalid-name
            return x if x[1].remaining_time() < y[1].remaining_time() else y

        self._logger.debug('_periodic_reports_send_loop start')
        time.sleep(0.1)  # start delayed
        # create an interval timer for each period
        timers = {}
        for period_ms in self._mdib.retrievability_periodic:
            timers[period_ms] = intervaltimer.IntervalTimer(period_in_seconds=period_ms / 1000)
        while self._run_periodic_reports_thread:
            # find timer with the shortest remaining time
            period_ms, timer = reduce(lambda x, y: _next(x, y), timers.items())  # pylint: disable=invalid-name
            timer.wait_next_interval_begin()
            self._logger.debug('_periodic_reports_send_loop {} msec timer', period_ms)
            all_handles = self._mdib.retrievability_periodic.get(period_ms, [])
            # separate them by notification types
            metrics = []
            components = []
            alerts = []
            operationals = []
            contexts = []
            for handle in all_handles:
                descr = self._mdib.descriptions.handle.get_one(handle)
                if descr.is_metric_descriptor and not descr.is_realtime_sample_array_metric_descriptor:
                    metrics.append(handle)
                elif descr.is_system_context_descriptor or descr.is_component_descriptor:
                    components.append(handle)
                elif descr.is_alert_descriptor:
                    alerts.append(handle)
                elif descr.is_operational_descriptor:
                    operationals.append(handle)
                elif descr.is_context_descriptor:
                    contexts.append(handle)

            with self._mdib.mdib_lock:
                mdib_version = self._mdib.mdib_version
                metric_states = [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in metrics]
                component_states = [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in components]
                alert_states = [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in alerts]
                operational_states = [self._mdib.states.descriptor_handle.get_one(h).mk_copy() for h in operationals]
                context_states = []
                for context in contexts:
                    print(
                        f'context.Handle {context} = {len(self._mdib.context_states.descriptor_handle.get(context, []))} states')
                    context_states.extend(
                        [st.mk_copy() for st in self._mdib.context_states.descriptor_handle.get(context, [])])
            self._logger.debug('   _periodic_reports_send_loop {} metric_states', len(metric_states))
            self._logger.debug('   _periodic_reports_send_loop {} component_states', len(component_states))
            self._logger.debug('   _periodic_reports_send_loop {} alert_states', len(alert_states))
            self._logger.debug('   _periodic_reports_send_loop {} alert_states', len(alert_states))
            self._logger.debug('   _periodic_reports_send_loop {} context_states', len(context_states))
            srv = self._hosted_services.state_event_service
            if metric_states:
                periodic_states = PeriodicStates(mdib_version, metric_states)
                srv.send_periodic_metric_report(
                    [periodic_states], self._mdib.mdib_version_group)
            if component_states:
                periodic_states = PeriodicStates(mdib_version, component_states)
                srv.send_periodic_component_state_report(
                    [periodic_states], self._mdib.mdib_version_group)
            if alert_states:
                periodic_states = PeriodicStates(mdib_version, alert_states)
                srv.send_periodic_alert_report(
                    [periodic_states], self._mdib.mdib_version_group)
            if operational_states:
                periodic_states = PeriodicStates(mdib_version, operational_states)
                srv.send_periodic_operational_state_report(
                    [periodic_states], self._mdib.mdib_version_group)
            if context_states:
                ctx_srv = self._hosted_services.context_service
                periodic_states = PeriodicStates(mdib_version, context_states)
                ctx_srv.send_periodic_context_report(
                    [periodic_states], self._mdib.mdib_version_group)
